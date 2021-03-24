use core::panic;
use std::{
    cell::RefCell,
    fmt::{self, Debug, Display, Formatter},
    iter,
    rc::{Rc, Weak},
};

use crate::{config::Time, rw_buf::RWS, utils::result_to_val, verification};
use bincode::Options;
use itertools::Itertools;
use log::debug;
use verification::Hash;

use super::{
    basic_log::LogState,
    log_error::{LogError, Result},
    op::{min_entry, EntryInfo, EntryInfoData, OpEntryInfo, OpState},
    sp::{Sp, SpState},
    LogIdx,
};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct LogEntry {
    pub log_index: u64,
    pub file_index: u64,
    pub entry: PrevEntry,
    // the pointers are serialzed manually so we do set serde(skip)
    #[serde(skip)]
    pub log_pointers: LogPointers,
    #[serde(skip)]
    pub to_pointers: TotalOrderPointers,
    #[serde(skip)]
    // size of the object serialized, we put it in an option to be sure it is written before being used
    pub ser_size: Option<u64>,
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "LogEntry => log_index: {}, file idx: {}, {}, {}, entry: {:?}",
            self.log_index, self.file_index, self.log_pointers, self.to_pointers, self.entry
        )
    }
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Drop for LogEntry {
    fn drop(&mut self) {}
}

impl LogEntry {
    #[inline(always)]
    pub fn get_time(&self) -> Time {
        self.entry.get_entry_info().basic.time
    }

    pub fn get_op_entry_info(&self) -> OpEntryInfo {
        let op = self.entry.as_op();
        OpEntryInfo {
            op: op.op.op.clone(),
            hash: op.op.hash,
            log_index: self.log_index,
        }
    }

    fn from_file<F: RWS>(idx: u64, state: &LogState<F>) -> Result<LogEntry> {
        let mut f = state.f.borrow_mut();
        f.seek_to(idx)?;
        // first read the serialized bytes
        let (entry_info, mut entry): (_, LogEntry) = f.read_log()?;
        // sanity check
        if entry_info.start_location != entry.file_index {
            panic!("entry has an invlid file index")
        }
        // write the serialized size
        entry.ser_size = Some(entry_info.bytes_consumed);
        // check the hash of the internal operation
        entry.entry.check_hash(f.serialize_option())?;
        // read the location of the previous log
        let (prev_log, _) = f.read_u64()?;
        // read the location of the previous total order
        let (prev_to, _) = f.read_u64()?;
        // read the location of the next total order
        let (next_to, _) = f.read_u64()?;

        // see if there is a next log entry
        let next_log = {
            let info = f.check_index();
            if info.at_end {
                // there is no next log
                0
            } else {
                info.start_location // the next log is at this index
            }
        };
        let to_pointers = TotalOrderPointers::from_file_indicies(&entry, prev_to, next_to);
        entry.to_pointers = to_pointers;
        let log_pointers = LogPointers::from_file_indicies(&entry, prev_log, next_log);
        entry.log_pointers = log_pointers;
        entry.check_valid();
        debug!("loaded from disk {}", entry.log_index);
        Ok(entry)
    }

    fn check_valid(&self) {
        match self.log_index {
            1 => {
                // we must have no previous pointers as we are the first entry
                if self.log_pointers.prev_entry.is_some() || self.to_pointers.prev_to.is_some() {
                    panic!("inital log entry should have no prev");
                }
            }
            _ => {
                // we must have a prevoius log pointer as we are not the first entry
                if self.log_pointers.prev_entry.is_none() {
                    panic!("log entry {} should have prev", self.log_index);
                }
            }
        }
    }

    // write pointers when the entry is first received, the next pointer in the total order should be unknown
    pub fn write_pointers_initial<F: RWS>(
        &mut self,
        log_pointers: &StrongPointers,
        to_pointers: &StrongPointers,
        state: &LogState<F>,
    ) -> Result<()> {
        // First update the previous' next total order pointer, which is at the end of the log where we are now
        let mut f = state.f.borrow_mut();
        debug!(
            "write pointers at {}, end {}",
            self.file_index,
            f.get_end_index()
        );

        if cfg!(debug_assertions) {
            // when debugging check the pointers and values in the log are what was expected
            let (nxt_to_file_idx_disk, _prev_to_file_idx) = match to_pointers.prev.as_ref() {
                Some(prev) => {
                    let prev_ref = prev.ptr.borrow_mut();
                    debug_assert_eq!(prev_ref.file_index, prev.file_idx);
                    // seek to the previous total order entry
                    f.seek_to(prev_ref.file_index + prev_ref.ser_size.unwrap() + 16)?;
                    let val = f.read_u64()?.0;
                    f.seek_to_end();
                    (val, prev_ref.file_index)
                }
                None => (0, 0),
            };
            let (prev_to_file_idx_disk, _nxt_to_file_idx) = match to_pointers.next.as_ref() {
                Some(nxt) => {
                    let nxt_ref = nxt.ptr.borrow_mut();
                    debug_assert_eq!(nxt_ref.file_index, nxt.file_idx);
                    // seek to the previous total order entry
                    f.seek_to(nxt_ref.file_index + nxt_ref.ser_size.unwrap() + 8)?;
                    let val = f.read_u64()?.0;
                    f.seek_to_end();
                    (val, nxt_ref.file_index)
                }
                None => (0, 0),
            };
            if to_pointers.next.is_some() {
                debug_assert_eq!(
                    self.to_pointers.prev_to.as_ref().map_or(0, |p| p.file_idx),
                    prev_to_file_idx_disk
                );
            }
            if to_pointers.prev.is_some() {
                debug_assert_eq!(
                    self.to_pointers.next_to.as_ref().map_or(0, |n| n.file_idx),
                    nxt_to_file_idx_disk
                );
            }
        }

        if self.log_index <= 1 {
            // log entry 1 does not have to do this since there is no previous entry to update
        } else if let Some(prev) = to_pointers.prev.as_ref() {
            // the inital index does not track its next TO entry since we keep that in memory
            let prev_ref = prev.ptr.borrow_mut();
            debug_assert_eq!(prev_ref.file_index, prev.file_idx);
            // seek to the previous total order entry
            f.seek_to(prev_ref.file_index + prev_ref.ser_size.unwrap() + 16)?;

            // now update the previous entries next log order pointer
            f.write_u64(self.file_index)?;
        }
        // we append our entry at the end of the log
        f.seek_to_end();
        // sanity check
        if cfg!(debug_assertions) {
            let check = f.check_index();
            if !check.at_end {
                panic!("should be at the end of the file when writing new entries");
            }
            if self.file_index != check.start_location {
                panic!("entry has invalid file index");
            }
        }
        // write my serialized bytes
        let info = f.append_log(self)?;
        self.ser_size = Some(info.bytes_consumed); // keep the size of this entry in bytes
                                                   // write the location of the previous log entry
        let prv_location = match log_pointers.prev.as_ref() {
            Some(prv) => prv.file_idx,
            None => 0,
        };

        // write the location of the previous log order entry
        f.write_u64(prv_location)?;

        // write the location of the previous total order entry
        let prv_location = to_pointers.prev.as_ref().map_or(0, |prev| prev.file_idx);

        f.write_u64(prv_location)?;

        // now we write the location of the next total order pointer
        match to_pointers.next.as_ref() {
            Some(nxt) => {
                // we have a nxt pointer so we write its location
                let nxt_file_idx = nxt.file_idx;
                f.write_u64(nxt_file_idx)?;
                // now since we have an entry after us in the total order, we must update that entries' previous pointer
                // the previous total order pointer is after the serialized entry and the previous log entry pointer
                let nxt_prev_file = nxt_file_idx + nxt.ptr.borrow().ser_size.unwrap() + 8;
                f.seek_to(nxt_prev_file)?;
                f.write_u64(self.file_index)?;
            }
            None => {
                // we have no nxt pointer, so we just write 0
                f.write_u64(0)?;
            }
        }

        Ok(())
    }

    pub fn get_next(&self) -> Option<LogEntryStrong> {
        self.log_pointers.get_next_strong()
    }

    pub fn set_next(&mut self, next: Option<LogEntryWeak>) {
        self.log_pointers.next_entry = next;
    }

    pub fn get_prev(&self) -> Option<LogEntryStrong> {
        self.log_pointers.get_prev_strong()
    }

    pub fn set_prev(&mut self, prv: Option<LogEntryWeak>) {
        self.log_pointers.prev_entry = prv;
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub enum PrevEntry {
    Sp(OuterSp),
    Op(OuterOp),
}

impl Debug for PrevEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Display for PrevEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrevEntry::Sp(sp) => write!(f, "{}", sp),
            PrevEntry::Op(op) => write!(f, "{}", op),
        }
    }
}

impl PrevEntry {
    #[inline(always)]
    pub fn get_hash(&self) -> Hash {
        match self {
            PrevEntry::Sp(sp) => sp.sp.hash,
            PrevEntry::Op(op) => op.op.hash,
        }
    }

    pub fn check_hash<O: Options>(&self, o: O) -> Result<Vec<u8>> {
        match self {
            PrevEntry::Sp(sp) => sp.sp.check_hash(o),
            PrevEntry::Op(op) => op.op.check_hash(o),
        }
    }

    #[inline(always)]
    pub fn get_entry_info(&self) -> EntryInfo {
        match self {
            PrevEntry::Sp(sp) => sp.sp.get_entry_info(),
            PrevEntry::Op(op) => (&op.op).into(),
        }
    }

    #[inline(always)]
    pub fn get_time(&self) -> Time {
        self.get_entry_info().basic.time
    }

    #[inline(always)]
    pub fn mut_as_op(&mut self) -> &mut OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op,
        }
    }

    #[inline(always)]
    pub fn is_op(&self) -> bool {
        matches!(self, PrevEntry::Op(_))
    }
    #[inline(always)]

    pub fn is_sp(&self) -> bool {
        matches!(self, PrevEntry::Sp(_))
    }

    #[inline(always)]
    pub fn as_op(&self) -> &OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op,
        }
    }

    #[inline(always)]
    pub fn mut_as_sp(&mut self) -> &mut OuterSp {
        match self {
            PrevEntry::Sp(sp) => sp,
            PrevEntry::Op(_) => panic!("expected sp"),
        }
    }

    #[inline(always)]
    pub fn as_sp(&self) -> &OuterSp {
        match self {
            PrevEntry::Sp(sp) => sp,
            PrevEntry::Op(_) => panic!("expected sp"),
        }
    }
}

impl TotalOrderPointers {
    fn from_file_indicies(entry: &LogEntry, prev: u64, next: u64) -> TotalOrderPointers {
        let next_to = match next {
            0 => None,
            _ => Some(LogEntryWeak {
                file_idx: next,
                ptr: Weak::default(),
            }),
        };
        let prev_to = match prev {
            0 => {
                // if previous has 0 index, then there may be no previous
                match entry.log_index {
                    1 => None, // no previous since we are the first log entry
                    _ => {
                        // we are not the first log entry
                        match entry.entry {
                            PrevEntry::Op(_) => None, // since an SP is always at index 0, then there is no previous
                            PrevEntry::Sp(_) => {
                                Some(LogEntryWeak {
                                    file_idx: prev,
                                    ptr: Weak::default(),
                                }) // the previous is the intitial SP
                            }
                        }
                    }
                }
            }
            _ => Some(LogEntryWeak {
                file_idx: prev,
                ptr: Weak::default(),
            }),
        };
        TotalOrderPointers { next_to, prev_to }
    }

    pub fn check_ptrs<F: RWS>(&self, state: &LogState<F>) {
        if let Some(prv) = self.get_prev_to(state) {
            debug_assert_eq!(
                self.prev_to.as_ref().unwrap().file_idx,
                prv.borrow().file_index
            );
        }
        if let Some(nxt) = self.get_next_to(state) {
            debug_assert_eq!(
                self.next_to.as_ref().unwrap().file_idx,
                nxt.borrow().file_index
            );
        }
    }

    #[inline(always)]
    pub fn get_prev_to<F: RWS>(&self, state: &LogState<F>) -> Option<StrongPtr> {
        self.prev_to.as_ref().map(|entry| entry.get_ptr(state))
    }

    #[inline(always)]
    pub fn get_next_to<F: RWS>(&self, state: &LogState<F>) -> Option<StrongPtr> {
        self.next_to.as_ref().map(|entry| entry.get_ptr(state))
    }

    #[inline(always)]
    pub fn get_prev_to_strong(&self) -> Option<LogEntryStrong> {
        self.prev_to.as_ref().map(|entry| entry.into())
    }

    #[inline(always)]
    pub fn get_next_to_strong(&self) -> Option<LogEntryStrong> {
        self.next_to.as_ref().map(|entry| entry.into())
    }
}

pub struct StrongPointers {
    pub prev: Option<StrongPtrIdx>,
    pub next: Option<StrongPtrIdx>,
}

impl Default for StrongPointers {
    fn default() -> Self {
        StrongPointers {
            prev: None,
            next: None,
        }
    }
}

pub fn set_to_pointers(to_pointers: &StrongPointers, new_item: &StrongPtrIdx) {
    if let Some(prev) = to_pointers.prev.as_ref() {
        prev.ptr.borrow_mut().to_pointers.next_to = Some(new_item.into());
    }
    new_item.ptr.borrow_mut().to_pointers.prev_to = to_pointers.prev.as_ref().map(|p| p.into());

    if let Some(next) = to_pointers.next.as_ref() {
        next.ptr.borrow_mut().to_pointers.prev_to = Some(new_item.into());
    }
    new_item.ptr.borrow_mut().to_pointers.next_to = to_pointers.next.as_ref().map(|n| n.into());
}

/// Updates the total order pointers and appends the new item at the end of the log.
pub fn set_to_pointers_append_to_log<F: RWS>(
    to_pointers: &StrongPointers,
    log_pointers: &StrongPointers,
    new_item: &StrongPtrIdx,
    state: &LogState<F>,
    // prev: &StrongPtrIdx,
    // new_next: &StrongPtrIdx,
    // state: &LogState<F>,
) {
    set_to_pointers(to_pointers, new_item);

    new_item
        .ptr
        .borrow_mut()
        .write_pointers_initial(&log_pointers, &to_pointers, state)
        .unwrap();
}

pub type StrongPtr = Rc<RefCell<LogEntry>>;
pub type WeakPtr = Weak<RefCell<LogEntry>>;

pub struct StrongPtrIdx {
    pub file_idx: u64,
    pub ptr: StrongPtr,
}

impl Debug for StrongPtrIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl From<&LogEntryKeep> for StrongPtrIdx {
    fn from(le: &LogEntryKeep) -> Self {
        StrongPtrIdx {
            file_idx: le.file_idx,
            ptr: le.ptr.as_ref().unwrap().clone(),
        }
    }
}

impl From<&StrongPtr> for StrongPtrIdx {
    fn from(pt: &StrongPtr) -> Self {
        StrongPtrIdx {
            file_idx: pt.borrow().file_index,
            ptr: pt.clone(),
        }
    }
}

impl Clone for StrongPtrIdx {
    fn clone(&self) -> Self {
        StrongPtrIdx {
            file_idx: self.file_idx,
            ptr: Rc::clone(&self.ptr),
        }
    }
}

impl Display for StrongPtrIdx {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "strong ptr idx: {}", self.file_idx)
    }
}

impl StrongPtrIdx {
    pub fn new(file_idx: u64, ptr: StrongPtr) -> StrongPtrIdx {
        StrongPtrIdx { file_idx, ptr }
    }
}

// Like LogEntryStrong, but ptr must always have a value
#[derive(Serialize, Deserialize)]
pub struct LogEntryKeep {
    pub file_idx: u64,
    #[serde(skip)]
    ptr: Option<StrongPtr>, // must never be none, except initally
}

impl LogEntryKeep {
    pub fn get_ptr(&self) -> &StrongPtr {
        self.ptr.as_ref().expect("should not be none")
    }

    fn load_pointer<F: RWS>(&mut self, state: &LogState<F>) {
        self.ptr = Some(get_ptr(self.file_idx, state))
    }

    pub fn new(file_idx: u64, ptr: StrongPtr) -> LogEntryKeep {
        LogEntryKeep {
            file_idx,
            ptr: Some(ptr),
        }
    }
}

impl From<&StrongPtrIdx> for LogEntryKeep {
    fn from(le: &StrongPtrIdx) -> Self {
        LogEntryKeep {
            file_idx: le.file_idx,
            ptr: Some(Rc::clone(&le.ptr)),
        }
    }
}

impl From<StrongPtrIdx> for LogEntryKeep {
    fn from(le: StrongPtrIdx) -> Self {
        LogEntryKeep {
            file_idx: le.file_idx,
            ptr: Some(le.ptr),
        }
    }
}

impl Clone for LogEntryKeep {
    fn clone(&self) -> Self {
        LogEntryKeep {
            file_idx: self.file_idx,
            ptr: self.ptr.as_ref().map(|entry| Rc::clone(entry)),
        }
    }
}

impl Eq for LogEntryKeep {}

impl PartialEq for LogEntryKeep {
    fn eq(&self, other: &Self) -> bool {
        self.file_idx == other.file_idx
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LogEntryStrong {
    pub file_idx: u64,
    #[serde(skip)]
    ptr: Option<StrongPtr>,
}

impl From<&StrongPtrIdx> for LogEntryStrong {
    fn from(le: &StrongPtrIdx) -> Self {
        LogEntryStrong {
            file_idx: le.file_idx,
            ptr: Some(Rc::clone(&le.ptr)),
        }
    }
}

impl From<&LogEntryWeak> for LogEntryStrong {
    fn from(le: &LogEntryWeak) -> Self {
        LogEntryStrong {
            file_idx: le.file_idx,
            ptr: le.ptr.upgrade(),
        }
    }
}

impl From<StrongPtrIdx> for LogEntryStrong {
    fn from(le: StrongPtrIdx) -> Self {
        LogEntryStrong {
            file_idx: le.file_idx,
            ptr: Some(le.ptr),
        }
    }
}

impl From<LogEntryKeep> for LogEntryStrong {
    fn from(le: LogEntryKeep) -> Self {
        LogEntryStrong {
            file_idx: le.file_idx,
            ptr: le.ptr,
        }
    }
}

impl Eq for LogEntryStrong {}

impl PartialEq for LogEntryStrong {
    fn eq(&self, other: &Self) -> bool {
        self.file_idx == other.file_idx
    }
}

pub fn get_ptr<F: RWS>(file_idx: u64, state: &LogState<F>) -> StrongPtr {
    // check the map
    let entry = state.m.borrow().get(file_idx);
    match entry {
        Some(entry) => entry,
        None => {
            // the entry must be loaded from the file
            debug!("load from disk at idx {}", file_idx);
            let entry = Rc::new(RefCell::new(
                LogEntry::from_file(file_idx, state).expect("unable to load entry from file"),
            ));
            let ptr = Rc::clone(&entry);
            if state
                .m
                .borrow_mut()
                .store_from_disk(file_idx, entry)
                .is_some()
            {
                panic!("found unexpected entry at file index");
            }
            ptr
        }
    }
}

impl LogEntryStrong {
    #[inline(always)]
    pub fn new(ptr: StrongPtr, file_idx: u64) -> LogEntryStrong {
        LogEntryStrong {
            file_idx,
            ptr: Some(ptr),
        }
    }

    #[inline(always)]
    pub fn from_file_idx(file_idx: u64) -> LogEntryStrong {
        LogEntryStrong {
            file_idx,
            ptr: None,
        }
    }

    #[inline(always)]
    pub fn get_ptr_in_memory(&self) -> Option<&StrongPtr> {
        self.ptr.as_ref()
    }

    #[inline(always)]
    fn drop_ptr(&mut self) -> Result<StrongPtr> {
        if let Some(ptr) = self.ptr.take() {
            return Ok(ptr);
        }
        Err(LogError::OpAlreadyDropped)
    }

    #[inline(always)]
    pub fn log_entry_keep<F: RWS>(&mut self, state: &LogState<F>) -> LogEntryKeep {
        LogEntryKeep {
            file_idx: self.file_idx,
            ptr: Some(self.get_ptr(state)),
        }
    }

    #[inline(always)]
    pub fn strong_ptr_idx<F: RWS>(&mut self, state: &LogState<F>) -> StrongPtrIdx {
        StrongPtrIdx {
            file_idx: self.file_idx,
            ptr: self.get_ptr(state),
        }
    }

    #[inline(always)]
    pub fn get_ptr<F: RWS>(&mut self, state: &LogState<F>) -> StrongPtr {
        if self.ptr.is_none() {
            self.ptr = Some(get_ptr(self.file_idx, state));
        }
        Rc::clone(self.ptr.as_ref().unwrap())
    }

    // panics if the entry is not in memory
    #[inline(always)]
    pub fn clone_strong_expect_exists(&self) -> StrongPtrIdx {
        StrongPtrIdx {
            file_idx: self.file_idx,
            ptr: Rc::clone(self.ptr.as_ref().expect("expected to have found entry")),
        }
    }
}

impl Display for LogEntryStrong {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self.ptr.as_ref() {
            None => write!(f, "file idx (strong): {}, None", self.file_idx),
            Some(entry) => write!(
                f,
                "file idx (strong): {}, {}",
                self.file_idx,
                entry.borrow().entry
            ),
        }
    }
}

impl Debug for LogEntryStrong {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Default for LogEntryStrong {
    #[inline(always)]
    fn default() -> Self {
        LogEntryStrong {
            file_idx: 0,
            ptr: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct LogEntryWeak {
    file_idx: u64,
    #[serde(skip)]
    ptr: WeakPtr,
}

impl From<LogEntryStrong> for LogEntryWeak {
    fn from(le: LogEntryStrong) -> Self {
        LogEntryWeak {
            file_idx: le.file_idx,
            ptr: match le.ptr.as_ref() {
                None => Weak::default(),
                Some(entry) => Rc::downgrade(entry),
            },
        }
    }
}

impl From<&StrongPtrIdx> for LogEntryWeak {
    fn from(le: &StrongPtrIdx) -> Self {
        LogEntryWeak {
            file_idx: le.file_idx,
            ptr: Rc::downgrade(&le.ptr),
        }
    }
}

impl From<&LogEntryKeep> for LogEntryWeak {
    fn from(le: &LogEntryKeep) -> Self {
        LogEntryWeak {
            file_idx: le.file_idx,
            ptr: Rc::downgrade(le.get_ptr()),
        }
    }
}

impl Display for LogEntryWeak {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "file idx (weak): {}", self.file_idx)
    }
}

impl Eq for LogEntryWeak {}

impl PartialEq for LogEntryWeak {
    fn eq(&self, other: &Self) -> bool {
        self.file_idx == other.file_idx
    }
}

impl Default for LogEntryWeak {
    fn default() -> Self {
        LogEntryWeak {
            file_idx: 0,
            ptr: Weak::default(),
        }
    }
}

impl LogEntryWeak {
    pub fn from_file_idx(idx: u64) -> Self {
        LogEntryWeak {
            file_idx: idx,
            ptr: Weak::default(),
        }
    }

    pub fn get_file_index(&self) -> u64 {
        self.file_idx
    }

    fn as_log_entry_strong<F: RWS>(&self, state: &LogState<F>) -> LogEntryStrong {
        LogEntryStrong {
            file_idx: self.file_idx,
            ptr: Some(self.get_ptr(state)),
        }
    }

    pub fn to_strong_ptr_idx<F: RWS>(&self, state: &LogState<F>) -> StrongPtrIdx {
        StrongPtrIdx {
            file_idx: self.file_idx,
            ptr: self.get_ptr(state),
        }
    }

    pub fn get_ptr<F: RWS>(&self, state: &LogState<F>) -> StrongPtr {
        self.ptr
            .upgrade()
            .unwrap_or_else(|| get_ptr(self.file_idx, state))
    }

    // Returns the object at the next pointer only if it is already in memory
    fn get_prt_if_in_memory(&self) -> Option<StrongPtr> {
        self.ptr.upgrade()
    }
}

#[derive(PartialEq, Eq)]
pub struct TotalOrderPointers {
    pub next_to: Option<LogEntryWeak>,
    pub prev_to: Option<LogEntryWeak>,
}

impl Display for TotalOrderPointers {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let prv = match &self.prev_to {
            None => "None".to_string(),
            Some(p) => format!("{}", p),
        };
        let nxt = match &self.next_to {
            None => "None".to_string(),
            Some(n) => format!("{}", n),
        };
        write!(f, "prev_to: {}, next_to: {}", prv, nxt)
    }
}

impl Default for TotalOrderPointers {
    fn default() -> Self {
        TotalOrderPointers {
            next_to: None,
            prev_to: None,
        }
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct LogPointers {
    pub next_entry: Option<LogEntryWeak>,
    pub prev_entry: Option<LogEntryWeak>,
}

impl Default for LogPointers {
    fn default() -> Self {
        LogPointers {
            next_entry: None,
            prev_entry: None,
        }
    }
}

impl Display for LogPointers {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let prv = match &self.prev_entry {
            None => "None".to_string(),
            Some(p) => format!("{}", p),
        };
        let nxt = match &self.next_entry {
            None => "None".to_string(),
            Some(n) => format!("{}", n),
        };
        write!(f, "prev_entry: {}, next_entry: {}", prv, nxt)
    }
}

impl LogPointers {
    fn from_file_indicies(entry: &LogEntry, prev: u64, next: u64) -> LogPointers {
        let next_log = match next {
            0 => None,
            _ => Some(LogEntryWeak {
                file_idx: next,
                ptr: Weak::default(),
            }),
        };
        let prev_log = if (prev == 0 && entry.log_index != 1) || prev > 0 {
            // if the prev file index is greater than 0, or if we are not the first log entry, then there is a previous
            Some(LogEntryWeak {
                file_idx: prev,
                ptr: Weak::new(),
            })
        } else {
            // otherwise there is no previous log entry
            None
        };
        LogPointers {
            prev_entry: prev_log,
            next_entry: next_log,
        }
    }

    pub fn get_prev<F: RWS>(&mut self, state: &LogState<F>) -> Option<StrongPtr> {
        self.prev_entry.as_mut().map(|entry| entry.get_ptr(state))
    }

    pub fn get_prev_strong(&self) -> Option<LogEntryStrong> {
        self.prev_entry.as_ref().map(|entry| entry.into())
    }

    pub fn get_next<F: RWS>(&self, state: &LogState<F>) -> Option<StrongPtr> {
        self.next_entry.as_ref().map(|entry| entry.get_ptr(state))
    }

    pub fn get_next_strong(&self) -> Option<LogEntryStrong> {
        self.next_entry.as_ref().map(|entry| entry.into())
    }

    // Returns the object at the next pointer only if it is already in memory
    fn get_next_if_in_memory(&self) -> Option<StrongPtr> {
        self.next_entry
            .as_ref()
            .and_then(|entry| entry.get_prt_if_in_memory())
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq, Debug, Clone)]
pub struct OuterOp {
    pub log_index: u64,
    pub op: OpState,
    pub include_in_hash: bool, // if false op was soo late that it will not be included in the SP
    pub arrived_late: bool, // if true, op was late, but not late enough to not be included, so it will be specially marked as included in the Sp
                            // hash: Hash,
                            // verification: Verify,
}

impl OuterOp {}

impl From<&OuterOp> for OpEntryInfo {
    fn from(o_op: &OuterOp) -> Self {
        OpEntryInfo {
            op: o_op.op.op.clone(),
            hash: o_op.op.hash,
            log_index: o_op.log_index,
        }
    }
}

impl From<&OuterOp> for EntryInfo {
    fn from(o_op: &OuterOp) -> Self {
        EntryInfo {
            basic: o_op.op.op.info,
            hash: o_op.op.hash,
        }
    }
}

impl Display for OuterOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

pub fn leq_by_entry(l: &StrongPtrIdx, r: &StrongPtrIdx) -> bool {
    l.ptr.borrow().entry.get_entry_info() <= r.ptr.borrow().entry.get_entry_info()
}

struct WeakToStrongIterRef<'a, F: RWS, J: Iterator<Item = &'a LogEntryWeak>> {
    iter: J,
    state: &'a LogState<F>,
}

impl<'a, F: RWS, J: Iterator<Item = &'a LogEntryWeak>> Iterator for WeakToStrongIterRef<'a, F, J> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|l| l.to_strong_ptr_idx(self.state))
    }
}

struct WeakToStrongIter<'a, F: RWS, J: Iterator<Item = LogEntryWeak>> {
    iter: J,
    state: &'a LogState<F>,
}

impl<'a, F: RWS, J: Iterator<Item = LogEntryWeak>> Iterator for WeakToStrongIter<'a, F, J> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next().map(|l| l.to_strong_ptr_idx(self.state))
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct OuterSp {
    pub sp: SpState,
    pub supported_sp_log_index: Option<LogIdx>, // the log index of the sp that this entry supports
    pub last_op: Option<LogEntryWeak>,          // the last op in the total order this sp supported
    // the ops before last_op in the log that are not included in the sp
    // (by definintion each is op.include_in_hash = true, but op.arrived_late may be true or false)
    // they include those not included from the previous Sps if they are still not included //TODO prevent this from getting too big
    pub not_included_ops: Vec<LogEntryWeak>,
    // the ops were late, but were still included (by definition each is op.arrived_late = true)
    pub late_included: Vec<LogEntryWeak>,
    // pub prev_sp: Option<LogEntryWeak>, // the previous sp that this entry supports
}

pub struct SpResult {
    included: Vec<StrongPtrIdx>,      // all ops that were included
    late_included: Vec<LogEntryWeak>, // ops that were late, but were still included
    not_included: Vec<LogEntryWeak>,  // ops that were on time, but were not included
    last_op: Option<LogEntryWeak>,    // op with largest time
}

impl OuterSp {
    /// Returns an iterator that contains all operations that have not been included in this SP in the log.
    /// Used when creating a new local Sp.
    /// Starts from the last op of this iterator, and returns all ops after it in the log, or ops that were part of
    /// this Sp's not_included items.
    /// first_op is the first operation in the log in total order (this is used only if this is the first SP)
    pub fn get_ops_after_iter<'a, F: RWS>(
        &'a self,
        late_include: Option<Vec<OuterOp>>,
        first_op: Option<LogEntryWeak>,
        state: &'a LogState<F>,
    ) -> Result<Box<dyn Iterator<Item = OuterOp> + 'a>> {
        let merge_fn = |x: &OuterOp, y: &OuterOp| x.op <= y.op;
        // not_included_iter are the items before self.last_op that were not included
        // log_iter are the items that have larger time and are later in the log than self.last_op
        // late_include are the items after self.last_op in the log that arrived late
        let (not_included_iter, log_iter) = self.get_iters(first_op, state)?;
        let not_included_iter = not_included_iter.map(|nxt| nxt.ptr.borrow().entry.as_op().clone());
        let log_iter = log_iter.map(|nxt| nxt.ptr.borrow().entry.as_op().clone());
        match late_include {
            Some(late) => {
                let iter = not_included_iter
                    .merge_by(log_iter, merge_fn)
                    .merge_by(late, merge_fn);
                Ok(Box::new(iter))
            }
            None => {
                let iter = not_included_iter.merge_by(log_iter, merge_fn);
                Ok(Box::new(iter))
            }
        }
    }

    /// This starts from last_op, going backwards in a total order, collecting the ops that match
    /// that match those in exact or extra_info.
    /// It returns an iterator of those ops in increasing sorted order.
    fn get_early_ops<'a, I, J, F: RWS>(
        &'a self,
        last_op: StrongPtrIdx,
        exact: I,
        extra_info: J,
        state: &'a LogState<F>,
    ) -> impl Iterator<Item = StrongPtrIdx> + 'a
    where
        I: Iterator<Item = EntryInfo> + 'a,
        J: Iterator<Item = EntryInfoData> + 'a,
    {
        let filter_fn = |(supported, op)| match supported {
            Supported::Supported => Some(op),
            Supported::SupportedData(_) => Some(op),
            _ => None,
        };
        let mut exact = exact.peekable();
        let mut extra_info = extra_info.peekable();
        let late_min = min_entry(exact.peek(), extra_info.peek());
        match late_min {
            // there is nothing to iterate over, so just return an empty iterator
            None => TotalOrderExactIterator::empty(exact, extra_info).filter_map(filter_fn),
            Some(late_min) => {
                // go backwards until late_min is reached collecting the items, sorting them
                // and filtering only the ones that are part of exact or extra_info
                let mut entries: Vec<(LogEntryWeak, EntryInfo)> =
                    total_order_iterator(&last_op.into(), false, state)
                        .take_while(|nxt| nxt.ptr.borrow().entry.get_entry_info() >= late_min)
                        .map(|nxt| ((&nxt).into(), nxt.ptr.borrow().entry.get_entry_info()))
                        .collect();
                entries.sort_by(|(_, l), (_, r)| l.cmp(r)); // TODO use different sorting, since should be in reserse for timsort?
                let entries_iter = WeakToStrongIter {
                    iter: entries.into_iter().map(|(nxt, _)| nxt),
                    state,
                };

                total_order_prev_iterator(entries_iter, exact, extra_info).filter_map(filter_fn)
            }
        }
    }

    /// first_op is the first operation in the log in total order (this is used only if this is the first SP)
    pub fn check_sp_exact<F: RWS>(
        &self,
        my_log_idx: LogIdx,
        new_sp: SpState,
        exact: &[EntryInfo],
        first_op: Option<LogEntryWeak>,
        state: &LogState<F>,
    ) -> Result<(OuterSp, Vec<StrongPtrIdx>)> {
        // where to start the iterator from
        // create a total order iterator over the operations including the unused items from the previous
        // SP that takes the exact set from exact, and returns the rest as unused, until last_op
        let extra_info = new_sp.sp.additional_ops.iter().cloned();
        let last_op = self.get_last_op(first_op.clone(), state)?;
        let early_ops = self.get_early_ops(last_op, exact.iter().cloned(), extra_info, state);

        let (not_included_iter, log_iter) = self.get_iters(first_op, state)?;
        let early_ops = early_ops.merge_by(not_included_iter, leq_by_entry);
        let extra_info = new_sp.sp.additional_ops.iter().cloned();
        let op_iter =
            total_order_exact_iterator(early_ops, log_iter, exact.iter().cloned(), extra_info);
        let sp_result = self.perform_check_sp(&new_sp.sp, op_iter)?;
        Ok((
            OuterSp {
                sp: new_sp,
                last_op: sp_result.last_op,
                not_included_ops: sp_result.not_included,
                late_included: sp_result.late_included,
                supported_sp_log_index: Some(my_log_idx),
            },
            sp_result.included,
        ))
    }

    /// Checks for the operations in the log from the given iterators and new SP.
    /// late_included and not_included are hints as to what operations to include and not include given by the
    /// node that sent the Sp. They may not be used given that the real definition of the set of the Ops
    /// must be given by the support hash of the new Sp.
    /// If this calculation fails, then this node can ask an external node for the exact set of ops needed for
    /// the new Sp, and check_sp_exact can be called with this set.
    /// first_op is the first operation in the log in total order (this is used only if this is the first SP)
    pub fn check_sp_log_order<
        F: RWS,
        // J: Iterator<Item = EntryInfo>,
        K: Iterator<Item = EntryInfo>,
    >(
        &self,
        my_log_idx: LogIdx,
        new_sp: SpState,
        late_included: &[EntryInfo],
        not_included: K,
        first_op: Option<LogEntryWeak>,
        state: &LogState<F>,
    ) -> Result<(OuterSp, Vec<StrongPtrIdx>)> {
        // where to start the iterator from
        // create a total order iterator over the operations of the log including the operations not included in thre previous sp
        // it uses included and not included to decide what operations to include
        // furthermore it includes items that have include_in_hash = true and are after the last operation of the previous
        // SP in the log
        let last_op = self.get_last_op(first_op.clone(), state)?;
        let extra_info = new_sp.sp.additional_ops.iter().cloned();
        // early ops are those smaller than last_op, but are included in either late_included or extra_info
        let early_ops =
            self.get_early_ops(last_op, late_included.iter().cloned(), extra_info, state);

        let (not_included_iter, log_iter) = self.get_iters(first_op, state)?;
        let early_ops = early_ops.merge_by(not_included_iter, leq_by_entry);
        let op_iter = total_order_check_iterator(
            early_ops,
            log_iter,
            late_included.iter().cloned(),
            not_included,
            new_sp.sp.additional_ops.iter().cloned(),
        );
        let sp_result = self.perform_check_sp(&new_sp.sp, op_iter)?;
        Ok((
            OuterSp {
                sp: new_sp,
                last_op: sp_result.last_op,
                not_included_ops: sp_result.not_included,
                late_included: sp_result.late_included,
                supported_sp_log_index: Some(my_log_idx),
            },
            sp_result.included,
        ))
    }

    /// Returns the last operation that was included in this Sp.
    /// first_op is the first operation in the log in total order (this is used only if this is the first SP)
    fn get_last_op<F: RWS>(
        &self,
        first_op: Option<LogEntryWeak>,
        state: &LogState<F>,
    ) -> Result<StrongPtrIdx> {
        let last_op = self.last_op.as_ref().or_else(|| first_op.as_ref());
        Ok(last_op
            .ok_or(LogError::PrevSpHasNoLastOp)?
            .to_strong_ptr_idx(state))
    }

    /// Returns the iterators for this Sp.
    /// The first iterates items from self.not_included_ops.
    /// The second iterates all items later in the log and any ops with time less than late_min
    /// (that are before or after this Sp in the log) and have include_in_hash = false
    /// (see total_order_after_late_iter).
    /// first_op is the first operation in the log in total order (this is used only if this is the first SP)
    fn get_iters<'a, F: RWS>(
        &'a self,
        // late_min: Option<EntryInfo>,
        first_op: Option<LogEntryWeak>,
        state: &'a LogState<F>,
    ) -> Result<(
        impl Iterator<Item = StrongPtrIdx> + 'a,
        TotalOrderAfterIter<'a, F>,
    )> {
        // we make an iterator that goes through the log in total order
        // the iterator starts from the log entry of the last op included in the previous SP (self) and traverses the log
        // from there in total order, returning the entries that occur later in the log, plus the ops that are in the
        // not_included list of the previous SP.
        let not_included_iter = WeakToStrongIterRef {
            iter: self.not_included_ops.iter(),
            state,
        };
        // items from previous sp that are earlier in the log, we need to check if these are included
        // we only want operations after the last op of the previous sp in the log
        let last_op = self.get_last_op(first_op, state)?;
        let log_iter = if self.sp.sp.is_init() {
            // for the inital SP, we always start from the first op, and include all ops
            total_order_after_all_iter(&last_op, state)
        } else {
            // for later Sps we need to start from the op after last_op
            // as well as any late arrivals, which will be only included if they are referenced as being included directly
            total_order_after_late_iter(&last_op, state)
            // log_iter.next();
            // log_iter
        };
        Ok((not_included_iter, log_iter))
    }

    fn perform_check_sp(
        &self,
        new_sp: &Sp,
        mut op_iter: impl Iterator<Item = (Supported, StrongPtrIdx)>,
    ) -> Result<SpResult> {
        let mut hasher = verification::new_hasher();
        hasher.update(self.sp.hash.as_bytes());
        let mut not_included = vec![];
        let mut last_op = None;
        let mut included = vec![];
        let mut skipped = vec![];
        let mut late_included = vec![];
        let count = result_to_val(op_iter.try_fold(0, |mut count, (supported, nxt_op)| {
            if count >= new_sp.new_ops_supported {
                // see if we already computed enough ops
                return Err(count);
            }
            // let op_ptr = nxt_op.get_ptr(m, f);
            let op_ref = nxt_op.ptr.borrow();
            let op = &op_ref.entry.as_op().op;
            if op.op.info.time > new_sp.info.time {
                // see if we have passed all ops with smaller times
                return Err(count);
            }
            // see if we should in include the op
            match supported {
                Supported::Supported => (),        // normal op
                Supported::SupportedData(_) => (), // TODO should do something with this data?
                Supported::Skipped(s) => {
                    skipped.push(s);
                    return Ok(count);
                }
                Supported::NotSupported => {
                    not_included.push((&nxt_op).into()); // Rc::downgrade(&nxt_op));
                    return Ok(count);
                }
            }
            let nxt_op_ptr = nxt_op.ptr.borrow();
            let nxt_op_op = nxt_op_ptr.entry.as_op();
            included.push(nxt_op.clone());
            if nxt_op_op.arrived_late {
                late_included.push((&nxt_op).into());
            }
            debug!("added op {:?} to Sp during log order check", nxt_op_op.op);
            // add the hash of the op
            hasher.update(nxt_op_op.op.hash.as_bytes());
            count += 1;
            // update the last op pointer
            last_op = Some((&nxt_op).into()); // Rc::downgrade(&nxt_op));
            Ok(count)
        }));
        if !skipped.is_empty() {
            Err(LogError::SpSkippedOps(skipped))
        } else if count != new_sp.new_ops_supported {
            Err(LogError::NotEnoughOpsForSP)
        } else if new_sp.support_hash == hasher.finalize() {
            Ok(SpResult {
                included,
                late_included,
                not_included,
                last_op,
            })
        } else {
            Err(LogError::SpHashNotComputed)
        }
    }
}

impl Display for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SP, {:?}", self.sp)
    }
}

impl Debug for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub fn total_order_iterator<'a, F: RWS>(
    start: &LogEntryStrong,
    forward: bool,
    state: &'a LogState<F>,
) -> TotalOrderIterator<'a, F> {
    TotalOrderIterator {
        prev_entry: Some(start.clone()),
        forward,
        state,
    }
}

pub struct TotalOrderIterator<'a, F: RWS> {
    pub prev_entry: Option<LogEntryStrong>,
    pub forward: bool,
    pub state: &'a LogState<F>,
}

impl<'a, F: RWS> Iterator for TotalOrderIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = match self.prev_entry.as_mut() {
            None => None,
            Some(prv) => Some(StrongPtrIdx::new(prv.file_idx, prv.get_ptr(self.state))),
        };
        self.prev_entry = match self.prev_entry.take() {
            None => None,
            Some(mut prv) => {
                if self.forward {
                    prv.get_ptr(self.state)
                        .borrow()
                        .to_pointers
                        .get_next_to_strong()
                } else {
                    prv.get_ptr(self.state)
                        .borrow()
                        .to_pointers
                        .get_prev_to_strong()
                }
            }
        };
        ret
    }
}

// sp.not_included are all ops that are not late, and are not included in the SP
// - this will never change at the local log because any ops that arrives later will be late

/// Total order iterator that only includes log entries with a larger (or equal) log index than the input.
pub struct TotalOrderAfterIter<'a, F: RWS> {
    iter: TotalOrderIterator<'a, F>,
    min_index: LogIdx,
    min_entry: EntryInfo,
    include_late: bool,
}

// instead collect the exact set of early ones in a vec before so we only include those (instead of going backwards)?
/// TODO do we need to check all these things (min_index, min_entry) or is just one sufficient since we are going
/// in a total order.
impl<'a, F: RWS> Iterator for TotalOrderAfterIter<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(nxt) = self.iter.next() {
            if nxt.ptr.borrow().log_index >= self.min_index
                || (self.include_late && nxt.ptr.borrow().entry.as_op().arrived_late)
                || nxt.ptr.borrow().entry.get_entry_info() > self.min_entry
            {
                return Some(nxt);
            }
        }
        None
    }
}

/// A total order op iterator that returns entries that are either after start in the log, or
/// have time after late_min and also have include_in_hash = false.
/// If late_min is None, the time of the start input is used instead for values wiht include_in_hash = false.
/// This is used when traversing after the last op in the previous SP, where it we only want to see new ops
/// (later in the log), or ops that were late and not included by default in the SP
/// (these would be the ops in "sp.additional_ops", or when using calculate SP exact).
/// TODO do this more efficiently
pub fn total_order_after_late_iter<'a, F: RWS>(
    start: &StrongPtrIdx,
    // late_min: Option<EntryInfo>,
    state: &'a LogState<F>,
) -> TotalOrderAfterIter<'a, F> {
    // if prev sp is not the inital SP, then we need to move forward 1 op since last op was already included in prev sp
    let min_index = start.ptr.borrow().log_index + 1;
    let min_entry = start.ptr.borrow().entry.get_entry_info();
    TotalOrderAfterIter {
        iter: total_order_iterator(&start.into(), true, state),
        min_index,
        min_entry,
        include_late: true,
    }
}

/// Returns an total order iterator that returns items larger than start.
/// This is the same as a total order iterator from start.
pub fn total_order_after_all_iter<'a, F: RWS>(
    start: &StrongPtrIdx,
    state: &'a LogState<F>,
) -> TotalOrderAfterIter<'a, F> {
    TotalOrderAfterIter {
        iter: total_order_iterator(&start.into(), true, state),
        min_index: 0,
        min_entry: start.ptr.borrow().entry.get_entry_info(),
        include_late: true,
    }
}

/// Returns an total order iterator that returns start and items larger than start AND are after start in the log.
pub fn total_order_after_iter<'a, F: RWS>(
    start: &StrongPtrIdx,
    state: &'a LogState<F>,
) -> TotalOrderAfterIter<'a, F> {
    TotalOrderAfterIter {
        iter: total_order_iterator(&start.into(), true, state),
        min_index: start.ptr.borrow().log_index,
        min_entry: start.ptr.borrow().entry.get_entry_info(),
        include_late: false,
    }
}
#[derive(Debug)]
pub enum Supported {
    Supported,
    SupportedData(EntryInfoData),
    NotSupported,
    Skipped(EntryInfo),
}

/// Creates an iterator that goes through prev_items (assuming it is sorted) checking for the items
/// in exact and extra info.
/// It is used to check for the exact operations that were before the last operation of the
/// previous Sp, when checking a new Sp.
pub fn total_order_prev_iterator<'a, J, K, I>(
    prev_items: I,
    exact: J,
    extra_info: K,
) -> TotalOrderExactIterator<'a, J, K>
where
    I: Iterator<Item = StrongPtrIdx> + 'a,
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    let iter = Box::new(prev_items);
    TotalOrderExactIterator {
        iter,
        extra_info_check: ExactCheck {
            last_included: None,
            included: extra_info,
        },
        exact_check: ExactCheck {
            last_included: None,
            included: exact,
        },
    }
}

/// Creates an iterator that goes through prev_not_included and log_iter (merged into a single sorted interator
/// assuming they are sorted when input), checking for the items in exact and extra info.
/// It is used as the iterator for checking an Sp with an exact set of ops.
pub fn total_order_exact_iterator<'a, J, K, I, F: RWS>(
    prev_not_included: I,
    log_iter: TotalOrderAfterIter<'a, F>,
    exact: J,
    extra_info: K,
) -> TotalOrderExactIterator<'a, J, K>
where
    I: Iterator<Item = StrongPtrIdx> + 'a,
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    // now we merge the prev_not_included and the log_iter into a single iterator them so we traverse the two of them in total order
    let iter = Box::new(
        log_iter
            .merge_by(prev_not_included, |x, y| {
                x.ptr.borrow().entry.get_entry_info() <= y.ptr.borrow().entry.get_entry_info()
            })
            .dedup_by(|l, r| l.file_idx == r.file_idx),
    );
    TotalOrderExactIterator {
        iter,
        extra_info_check: ExactCheck {
            last_included: None,
            included: extra_info,
        },
        exact_check: ExactCheck {
            last_included: None,
            included: exact,
        },
    }
}

impl<'a, J, K> TotalOrderExactIterator<'a, J, K>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    fn empty(j: J, k: K) -> Self {
        TotalOrderExactIterator {
            iter: Box::new(iter::empty()),
            extra_info_check: ExactCheck {
                last_included: None,
                included: k,
            },
            exact_check: ExactCheck {
                last_included: None,
                included: j,
            },
        }
    }
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input,
// or were from the not included ops of the previous sp, and are part included in the exact iterator input.
pub struct TotalOrderExactIterator<'a, J, K>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    iter: Box<dyn Iterator<Item = StrongPtrIdx> + 'a>,
    extra_info_check: ExactCheck<K, EntryInfoData>,
    exact_check: ExactCheck<J, EntryInfo>,
}

impl<'a, J, K> Iterator for TotalOrderExactIterator<'a, J, K>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    type Item = (Supported, StrongPtrIdx);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(nxt) = self.iter.next() {
            let info = nxt.ptr.borrow().entry.get_entry_info();
            // first check if the op is supported by an extra info field
            match self.extra_info_check.check_exact(info) {
                ExactCheckResult::NotFound => (), // nothing to do
                ExactCheckResult::Found(extra_info) => {
                    return Some((Supported::SupportedData(extra_info), nxt))
                }
                ExactCheckResult::Skipped(skipped) => {
                    return Some((Supported::Skipped(skipped.into()), nxt));
                }
            }
            // next check if the op is supported as part of the exact list
            match self.exact_check.check_exact(info) {
                ExactCheckResult::NotFound => return Some((Supported::NotSupported, nxt)),
                ExactCheckResult::Found(_) => return Some((Supported::Supported, nxt)),
                ExactCheckResult::Skipped(skipped) => {
                    return Some((Supported::Skipped(skipped), nxt));
                }
            }
        }
        None
    }
}

struct IncludedCheck<J, K>
where
    J: Iterator<Item = K>,
    K: Clone + Sized + Ord,
{
    last_included: Option<K>,
    included: J,
}

impl<J, K> IncludedCheck<J, K>
where
    J: Iterator<Item = K>,
    K: Clone + Sized + Ord,
{
    fn check_supported(&mut self, entry: K) -> bool {
        let mut found = false;
        if let Some(last_supported) = self.last_included.as_ref() {
            if last_supported >= &entry {
                found = true;
            }
        }
        if !found {
            self.last_included = self
                .included
                .find(|last_supported| last_supported >= &entry)
        }
        if let Some(last_support) = self.last_included.as_ref() {
            return last_support == &entry;
        }
        false
    }
}

/// Used when iterating through the log in total order, checking if for entries in self.included.
/// While also letting the caller know if any entires in self.included have been skipped,
/// self.included must be in increasing order.
struct ExactCheck<J, K>
where
    J: Iterator<Item = K>,
{
    last_included: Option<K>,
    included: J,
    // phantom: PhantomData<L>,
}

struct Tmp<J: Iterator<Item = EntryInfo>> {
    a: ExactCheck<J, EntryInfo>,
}

impl<J: Iterator<Item = EntryInfo>> Tmp<J> {
    fn tmp(&mut self, e: EntryInfo) {
        self.a.check_exact(e);
    }
}

enum ExactCheckResult<K> {
    NotFound,
    Found(K),
    Skipped(K),
}

/// Checks for the entry while iterating though self.included.
/// If the input entry is larger than the next entry in self.included, then that that value is returned
/// in ExactCheckResult::Skipped.
/// Expects to be called with entires in increasing order.
impl<J, K> ExactCheck<J, K>
where
    J: Iterator<Item = K>,
    K: Clone + Debug,
{
    fn check_exact<L: Ord + From<K> + Debug>(&mut self, entry: L) -> ExactCheckResult<K> {
        if self.last_included.is_none() {
            self.last_included = self.included.next();
        }
        match self.last_included.as_ref() {
            Some(last_supported) => {
                let conv: L = last_supported.clone().into();
                match conv.cmp(&entry) {
                    std::cmp::Ordering::Greater => ExactCheckResult::NotFound,
                    std::cmp::Ordering::Equal => {
                        let ret = ExactCheckResult::Found(last_supported.clone());
                        self.last_included = None;
                        ret
                    }
                    std::cmp::Ordering::Less => {
                        let ret = ExactCheckResult::Skipped(last_supported.clone());
                        self.last_included = None;
                        ret
                    }
                }
            }
            None => ExactCheckResult::NotFound,
        }
    }
}

struct ExtraInfoCheck<K>
where
    K: Iterator<Item = EntryInfoData>,
{
    last_extra_info: Option<EntryInfoData>,
    extra_info: K,
}

impl<K> ExtraInfoCheck<K>
where
    K: Iterator<Item = EntryInfoData>,
{
    fn check_extra_info(&mut self, entry: EntryInfo) -> Option<EntryInfoData> {
        let mut found = false;
        if let Some(last_extra_info) = self.last_extra_info.as_ref() {
            if last_extra_info.info >= entry {
                found = true;
            }
        }
        if !found {
            self.last_extra_info = self
                .extra_info
                .find(|last_extra_info| last_extra_info.info >= entry)
        }
        if let Some(last_extra_info) = self.last_extra_info.as_ref() {
            if last_extra_info.info == entry {
                // TODO should return ref instead of clone?
                return Some(last_extra_info.clone());
            }
        }
        None
    }
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input,
// except for those that are supported/unsupported
pub struct TotalOrderCheckIterator<'a, J, K, L>
where
    J: Iterator<Item = EntryInfo>,
    L: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    iter: Box<dyn Iterator<Item = StrongPtrIdx> + 'a>,
    extra_info_check: ExactCheck<K, EntryInfoData>,
    included_check: IncludedCheck<J, EntryInfo>,
    not_included_check: IncludedCheck<L, EntryInfo>,
}

//fn entry_less_than(x: &LogEntryStrong, y: &LogEntryStrong) -> bool {
//  x.get_ptr().borrow().entry.get_entry_info() <= y.get_ptr().borrow().entry.get_entry_info()
//}

pub fn total_order_check_iterator<'a, J, K, L, I, F: RWS>(
    prev_not_included: I,
    log_iter: TotalOrderAfterIter<'a, F>,
    included: J,
    not_included: L,
    extra_info: K,
) -> TotalOrderCheckIterator<'a, J, K, L>
where
    I: Iterator<Item = StrongPtrIdx> + 'a,
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
    L: Iterator<Item = EntryInfo>,
{
    // now we merge the prev_not_included and the log_iter into a single iterator them so we traverse the two of them in total order
    let iter = Box::new(
        log_iter
            .merge_by(prev_not_included, |x, y| {
                x.ptr.borrow().entry.get_entry_info() <= y.ptr.borrow().entry.get_entry_info()
            })
            .dedup_by(|l, r| l.file_idx == r.file_idx),
    );
    TotalOrderCheckIterator {
        iter,
        extra_info_check: ExactCheck {
            last_included: None,
            included: extra_info,
        },
        included_check: IncludedCheck {
            last_included: None,
            included,
        },
        not_included_check: IncludedCheck {
            last_included: None,
            included: not_included,
        },
    }
}

impl<'a, J, K, L> Iterator for TotalOrderCheckIterator<'a, J, K, L>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
    L: Iterator<Item = EntryInfo>,
{
    type Item = (Supported, StrongPtrIdx);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(nxt) = self.iter.next() {
            let info = nxt.ptr.borrow().entry.get_entry_info();
            // first check if the op is supported by an extra info field
            match self.extra_info_check.check_exact(info) {
                ExactCheckResult::NotFound => (), // nothing to do
                ExactCheckResult::Found(extra_info) => {
                    return Some((Supported::SupportedData(extra_info), nxt))
                }
                ExactCheckResult::Skipped(skipped) => {
                    return Some((Supported::Skipped(skipped.into()), nxt));
                }
            }
            // next check if the op is supported as part of the supported list
            if self.included_check.check_supported(info) {
                return Some((Supported::Supported, nxt));
            }
            // next check if it is not supported as part of the not supported list
            if self.not_included_check.check_supported(info) {
                return Some((Supported::NotSupported, nxt));
            }
            // finally we only add operations that arrived on time
            if nxt.ptr.borrow().entry.as_op().include_in_hash {
                return Some((Supported::Supported, nxt));
            } else {
                // otherwise we add it to the not included list
                return Some((Supported::NotSupported, nxt));
            }
        }
        None
    }
}

/// Used to iterate the Ops in the log in the order entries were added to the log.
pub struct LogOpIterator<'a, F: RWS>(LogIterator<'a, F>);

impl<'a, F: RWS> LogOpIterator<'a, F> {
    /// Creates a new op iterator from a LogIterator.
    pub fn new(i: LogIterator<'a, F>) -> Self {
        LogOpIterator(i)
    }
}

impl<'a, F: RWS> Iterator for LogOpIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(nxt) = self.0.next() {
            if nxt.ptr.borrow().entry.is_op() {
                return Some(nxt);
            }
        }
        None
    }
}

/// Used to iterate the log in the order entries were added to the log.
pub struct LogIterator<'a, F: RWS> {
    prv_entry: Option<StrongPtrIdx>,
    state: &'a LogState<F>,
    forward: bool,
}

impl<'a, F: RWS> LogIterator<'a, F> {
    /// Create a new log iterator starting from (and including) prev_entry.
    /// Moves either forward or backward in the log in the order entries were added.
    pub(crate) fn new(
        prv_entry: Option<StrongPtrIdx>,
        state: &'a LogState<F>,
        forward: bool,
    ) -> LogIterator<'a, F> {
        LogIterator {
            prv_entry,
            state,
            forward,
        }
    }
}

impl<'a, F: RWS> Iterator for LogIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prv_entry.as_ref().cloned();
        self.prv_entry = match self.prv_entry.take() {
            None => None,
            Some(prv) => {
                if self.forward {
                    prv.ptr
                        .borrow()
                        .get_next()
                        .map(|mut entry| entry.strong_ptr_idx(self.state))
                } else {
                    prv.ptr
                        .borrow()
                        .get_prev()
                        .map(|mut entry| entry.strong_ptr_idx(self.state))
                }
            }
        };
        ret
    }
}

#[cfg(test)]
mod tests {
    use std::{cell::RefCell, rc::Rc};

    use log::debug;

    use crate::{
        file_sr::FileSR,
        log::{
            basic_log::LogState,
            hash_items::HashItems,
            log_file::open_log_file,
            log_file::LogFile,
            op::{gen_rand_data, BasicInfo, EntryInfo, OpState},
            sp::SpState,
            LogIdx,
        },
        rw_buf::RWS,
        verification::{hash, Id, TimeInfo, TimeTest},
    };

    use super::{
        set_to_pointers, LogEntry, LogEntryKeep, LogEntryStrong, LogEntryWeak, LogPointers,
        OuterOp, OuterSp, PrevEntry, StrongPointers, StrongPtr, StrongPtrIdx, TotalOrderPointers,
    };

    fn to_log_entry_weak(ptr: Option<&StrongPtr>) -> Option<LogEntryWeak> {
        ptr.map(|nxt| LogEntryWeak {
            file_idx: nxt.borrow().file_index,
            ptr: Rc::downgrade(nxt),
        })
    }

    fn to_log_strong_ptr_idx(ptr: Option<&StrongPtr>) -> Option<StrongPtrIdx> {
        ptr.map(|nxt| StrongPtrIdx {
            file_idx: nxt.borrow().file_index,
            ptr: Rc::clone(nxt),
        })
    }

    fn to_log_entry_strong(ptr: Option<&StrongPtr>) -> Option<LogEntryStrong> {
        ptr.map(|nxt| LogEntryStrong {
            file_idx: nxt.borrow().file_index,
            ptr: Some(Rc::clone(nxt)),
        })
    }

    fn make_log_entry(
        entry: PrevEntry,
        prev_to: Option<&StrongPtr>,
        next_to: Option<&StrongPtr>,
        prev_log: Option<&StrongPtr>,
        next_log: Option<&StrongPtr>,
    ) -> (StrongPtr, StrongPointers, StrongPointers) {
        let file_index = match prev_log {
            None => 0,
            Some(prv) => prv.borrow().ser_size.unwrap() + prv.borrow().file_index + 24,
        };
        let log_index = match prev_log {
            None => 1, // 1 is the initial log entry
            Some(prv) => prv.borrow().log_index + 1,
        };
        let log_entry = Rc::new(RefCell::new(LogEntry {
            log_index,
            file_index,
            entry,
            log_pointers: LogPointers {
                next_entry: to_log_entry_weak(next_log),
                prev_entry: to_log_entry_weak(prev_log),
            },
            to_pointers: TotalOrderPointers {
                next_to: to_log_entry_weak(next_to),
                prev_to: to_log_entry_weak(prev_to),
            },
            ser_size: None,
        }));
        // update the log pointers
        if let Some(prev) = prev_log {
            prev.borrow_mut().log_pointers.next_entry = to_log_entry_weak(Some(&log_entry));
        }
        let to_pointers = StrongPointers {
            prev: prev_to.map(|p| p.into()),
            next: next_to.map(|n| n.into()),
        };
        let log_pointers = StrongPointers {
            prev: prev_log.map(|p| p.into()),
            next: next_log.map(|n| n.into()),
        };
        // update the TO pionters
        set_to_pointers(&to_pointers, &(&log_entry).into());
        // return
        (log_entry, to_pointers, log_pointers)
    }

    fn make_outer_op<F: RWS>(
        id: Id,
        log_index: LogIdx,
        ti: &mut TimeTest,
        f: &LogFile<F>,
    ) -> PrevEntry {
        let op = OpState::new(id, gen_rand_data(), ti, f.serialize_option()).unwrap();
        let outer_op = OuterOp {
            log_index,
            op,
            include_in_hash: true,
            arrived_late: true,
        };
        let entry = PrevEntry::Op(outer_op);
        entry.check_hash(f.serialize_option()).unwrap();
        entry
    }

    fn make_outer_sp<F: RWS>(
        id: Id,
        ti: &mut TimeTest,
        state: &LogState<F>,
        prev_sp: Option<&LogEntryKeep>,
        last_op: Option<&LogEntryKeep>,
        not_included_ops: Vec<LogEntryKeep>,
    ) -> LogEntryKeep {
        let (prev_sp_info, prev_sp_log_index) = match prev_sp.as_ref() {
            None => (
                EntryInfo {
                    basic: BasicInfo {
                        time: ti.now_monotonic(),
                        id,
                    },
                    hash: hash(b"some msg"),
                },
                None,
            ),
            Some(sp) => (
                sp.get_ptr().borrow().entry.get_entry_info(),
                Some(sp.get_ptr().borrow().log_index),
            ),
        };
        let sp_state = SpState::new(
            id,
            ti.now_monotonic(),
            not_included_ops
                .iter()
                .map(|op| op.get_ptr().borrow().entry.get_entry_info().hash),
            vec![],
            prev_sp_info,
            state.f.borrow().serialize_option(),
        )
        .unwrap();
        let outer_sp = OuterSp {
            sp: sp_state,
            last_op: last_op.map(|op| op.into()),
            not_included_ops: not_included_ops.iter().map(|op| op.into()).collect(),
            late_included: vec![],
            // prev_sp: None, // prev_sp.as_ref().map(|op| op.to_log_entry_weak()),
            supported_sp_log_index: prev_sp_log_index,
        };
        let entry = PrevEntry::Sp(outer_sp);
        entry
            .check_hash(state.f.borrow().serialize_option())
            .unwrap();
        let (log_entry, to_pointers, log_pointers) = make_log_entry(
            entry,
            prev_sp.map(|sp| sp.get_ptr()),
            None,
            last_op.map(|op| op.get_ptr()),
            None,
        );
        log_entry
            .borrow_mut()
            .write_pointers_initial(&log_pointers, &to_pointers, state)
            .unwrap();
        state
            .f
            .borrow_mut()
            .seek_to(log_entry.borrow().file_index)
            .unwrap();
        let deser_log_entry = LogEntry::from_file(log_entry.borrow().file_index, state).unwrap();
        assert_eq!(*log_entry.borrow(), deser_log_entry);
        // be sure we can read the not included ops
        for (op1, op2) in log_entry
            .borrow()
            .entry
            .as_sp()
            .not_included_ops
            .iter()
            .zip(deser_log_entry.entry.as_sp().not_included_ops.iter())
        {
            assert_eq!(*op1.get_ptr(state).borrow(), *op2.get_ptr(state).borrow());
        }

        state.m.borrow_mut().clear();
        let file_idx = log_entry.borrow().file_index;
        LogEntryKeep {
            file_idx,
            ptr: Some(log_entry),
        }
    }

    fn make_op<F: RWS>(
        id: Id,
        ti: &mut TimeTest,
        state: &LogState<F>,
        log_entry: StrongPtr,
        prev_to: Option<StrongPtr>,
    ) -> LogEntryKeep {
        let op = make_outer_op(id, log_entry.borrow().log_index + 1, ti, &state.f.borrow());
        let next_to = prev_to
            .as_ref()
            .and_then(|p| p.borrow().to_pointers.get_next_to(state));
        let (log_entry, to_pointers, log_pointers) = make_log_entry(
            op,
            prev_to.as_ref(),
            next_to.as_ref(),
            Some(&log_entry),
            None,
        );
        debug!(
            "new file index {}, log index {}",
            log_entry.borrow().file_index,
            log_entry.borrow().log_index
        );
        state
            .f
            .borrow_mut()
            .seek_to(log_entry.borrow().file_index)
            .unwrap();
        debug_assert!(state.f.borrow().check_index().at_end);
        log_entry
            .borrow_mut()
            .write_pointers_initial(&log_pointers, &to_pointers, state)
            .unwrap();
        debug!("entry size {}", log_entry.borrow().ser_size.unwrap());
        state
            .f
            .borrow_mut()
            .seek_to(log_entry.borrow().file_index)
            .unwrap();
        let deser_log_entry = LogEntry::from_file(log_entry.borrow().file_index, state).unwrap();
        assert_eq!(*log_entry.borrow(), deser_log_entry);
        // check that the prev to next to pointer is correct
        if let Some(prv) = prev_to {
            let deser_prev_to = LogEntry::from_file(prv.borrow().file_index, state).unwrap();
            assert_eq!(*prv.borrow(), deser_prev_to);
            debug!("prv after: {}, deser prv: {}", prv.borrow(), deser_prev_to);
        }
        let file_idx = log_entry.borrow().file_index;
        state.m.borrow_mut().clear();
        LogEntryKeep {
            file_idx,
            ptr: Some(log_entry),
        }
    }

    #[test]
    fn serialize_entry() {
        let f = RefCell::new(open_log_file("log_files/entry_log0.log", true, FileSR::new).unwrap());
        let mut ti = TimeTest::new();
        let id = 0;
        let m = RefCell::new(HashItems::default());
        let state = LogState { m, f };

        // insert the inital sp
        let mut log_entry = make_outer_sp(id, &mut ti, &state, None, None, vec![]);
        let last_sp = log_entry.clone();
        let mut prev_to = None;
        let mut op_vec = vec![];
        // insert 10 ops
        for _ in 0..10 {
            log_entry = make_op(id, &mut ti, &state, Rc::clone(log_entry.get_ptr()), prev_to);
            prev_to = Some(Rc::clone(log_entry.get_ptr()));
            op_vec.push(log_entry.clone());
            state.m.borrow_mut().clear();
        }
        // insert an op that uses a different total order
        prev_to = Some(Rc::clone(op_vec[0].get_ptr()));
        log_entry = make_op(id, &mut ti, &state, Rc::clone(log_entry.get_ptr()), prev_to);
        state.m.borrow_mut().clear();

        // check sp deserialization that includes the ops
        make_outer_sp(
            id,
            &mut ti,
            &state,
            Some(&last_sp),
            Some(&log_entry),
            op_vec,
        );
    }
}
