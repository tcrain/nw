use core::panic;
use std::{
    cell::RefCell,
    fmt::{self, Debug, Display, Formatter},
    rc::{Rc, Weak},
};

use crate::{
    errors::{Error, LogError},
    rw_buf::RWS,
    utils::result_to_val,
    verification,
};
use bincode::Options;
use itertools::Itertools;
use log::debug;
use verification::Hash;

use super::{
    hash_items::HashItems,
    log_file::LogFile,
    op::{EntryInfo, EntryInfoData, OpState},
    sp::{Sp, SpState},
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
            "log_index: {}, {}, {}",
            self.log_index, self.log_pointers, self.to_pointers
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
    fn finish_deserialize<F: RWS>(&mut self, m: &mut HashItems<LogEntry>, f: &mut LogFile<F>) {
        match &mut self.entry {
            PrevEntry::Sp(sp) => sp.finish_deserialize(m, f),
            PrevEntry::Op(_) => (),
        }
    }

    fn from_file<F: RWS>(
        idx: u64,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Result<LogEntry, Error> {
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
        let (prev_to, prev_to_info) = f.read_u64()?;
        // read the location of the next total order
        let next_to = if !prev_to_info.at_end {
            f.read_u64()?.0
        } else {
            0
        };
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
        entry.finish_deserialize(m, f);
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
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Result<(), Error> {
        // the entry is always added at the end of the log
        f.seek_to_end();
        // First update the previous' next total order pointer, which is at the end of the log where we are now
        if self.log_index > 1 {
            // log entry 1 does not havt to do this since there is no previous entry to update
            match self.to_pointers.prev_to.as_mut() {
                Some(prev) => {
                    let prev_ptr = prev.get_ptr(m, f);
                    let prev_ref = prev_ptr.borrow_mut();
                    if prev_ref.log_index == self.log_index - 1 {
                        // if this is the previous entry in the log then we just write the next to pointer
                        f.write_u64(self.file_index)?;
                    } else {
                        // the previous log entry is not the previous total order entry, so we seek to it
                        f.seek_to(prev_ref.file_index + prev_ref.ser_size.unwrap() + 16)?;
                        // now update the previous entries next to pointer
                        f.write_u64(self.file_index)?;
                        // go back to my location - 8, which is where the previous log entries' total order next pointer is
                        f.seek_to(self.file_index - 8)?;
                        // we write 0 since the previous log entry does not have a next total order value yet
                        f.write_u64(0)?;
                    }
                }
                None => {
                    // there is no previous total order entry, so just write 0
                    f.write_u64(0)?;
                }
            }
        }
        // sanity check
        {
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
        let prv_location = match self.log_pointers.get_prev(m, f) {
            Some(prv) => prv.borrow().file_index,
            None => 0,
        };
        f.write_u64(prv_location)?;
        // write the location of the previous total order
        let prv_location = match self.to_pointers.get_prev_to(m, f) {
            Some(prv) => prv.borrow().file_index,
            None => 0,
        };
        f.write_u64(prv_location)?;
        // now if we have another entry after us in the total order, we must update that entries' previous pointer
        if let Some(mut nxt) = self.to_pointers.get_next_to_strong() {
            let ntx_ptr = nxt.get_ptr(m, f);
            let nxt_ref = ntx_ptr.borrow();
            // the previous total order pointer is after the serialized entry and the previous log entry pointer
            let nxt_prev_file = nxt_ref.file_index + nxt_ref.ser_size.unwrap() + 8;
            f.seek_to(nxt_prev_file)?;
            f.write_u64(self.file_index)?;
            // f.seek_to_end();
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

    pub fn set_prev(&mut self, prv: Option<LogEntryStrong>) {
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

pub fn prev_entry_strong_to_weak(entry: Option<LogEntryStrong>) -> Option<LogEntryWeak> {
    entry.map(|et| et.clone_weak())
}

impl PrevEntry {
    pub fn get_hash(&self) -> Hash {
        match self {
            PrevEntry::Sp(sp) => sp.sp.hash,
            PrevEntry::Op(op) => op.op.hash,
        }
    }

    pub fn check_hash<O: Options>(&self, o: O) -> Result<Vec<u8>, Error> {
        match self {
            PrevEntry::Sp(sp) => sp.sp.check_hash(o),
            PrevEntry::Op(op) => op.op.check_hash(o),
        }
    }

    pub fn get_entry_info(&self) -> EntryInfo {
        match self {
            PrevEntry::Sp(sp) => sp.sp.get_entry_info(),
            PrevEntry::Op(op) => op.op.get_entry_info(),
        }
    }

    pub fn mut_as_op(&mut self) -> &mut OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op,
        }
    }

    pub fn as_op(&self) -> &OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op,
        }
    }

    pub fn mut_as_sp(&mut self) -> &mut OuterSp {
        match self {
            PrevEntry::Sp(sp) => sp,
            PrevEntry::Op(_) => panic!("expected sp"),
        }
    }

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
                            PrevEntry::Sp(_) => Some(LogEntryWeak {
                                file_idx: prev,
                                ptr: Weak::default(),
                            }), // the previous is the intitial SP
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

    pub fn get_prev_to<F: RWS>(
        &self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Option<StrongPtr> {
        self.prev_to.as_ref().map(|entry| entry.get_ptr(m, f))
    }

    pub fn get_next_to<F: RWS>(
        &self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Option<StrongPtr> {
        self.next_to.as_ref().map(|entry| entry.get_ptr(m, f))
    }

    pub fn get_prev_to_strong(&self) -> Option<LogEntryStrong> {
        self.prev_to.as_ref().map(|entry| entry.clone_strong())
    }

    pub fn get_next_to_strong(&self) -> Option<LogEntryStrong> {
        self.next_to.as_ref().map(|entry| entry.clone_strong())
    }
}

pub fn set_next_total_order<F: RWS>(
    prev: &StrongPtrIdx,
    new_next: &StrongPtrIdx,
    m: &mut HashItems<LogEntry>,
    f: &mut LogFile<F>,
) {
    match prev.ptr.borrow().to_pointers.next_to.as_ref() {
        None => (),
        Some(nxt) => {
            new_next.ptr.borrow_mut().to_pointers.next_to = Some(nxt.clone_weak());
            nxt.get_ptr(m, f).borrow_mut().to_pointers.prev_to = Some(new_next.to_log_entry_weak());
        }
    };
    new_next.ptr.borrow_mut().to_pointers.prev_to = Some(prev.to_log_entry_weak());
    prev.ptr.borrow_mut().to_pointers.next_to = Some(new_next.to_log_entry_weak());
}

// Removes an item from the total order list
/*pub fn drop_item_total_order(item: &LogEntryStrong) {
    let prev_to = item.borrow().to_pointers.get_prev_to();
    let next_to = item.borrow().to_pointers.get_next_to();
    match next_to.as_ref() {
        Some(nxt) => {
            match prev_to.as_ref() {
                None => nxt.borrow_mut().to_pointers.prev_to = None,
                Some(prv) => {
                    nxt.borrow_mut().to_pointers.prev_to = Some(Rc::downgrade(prv));
                    prv.borrow_mut().to_pointers.next_to = Some(Rc::downgrade(nxt))
                }
            }
        },
        None => { // next is none
            if let Some(prv) = prev_to.as_ref() {
                prv.borrow_mut().to_pointers.next_to = None
            }
        }
    }
}*/

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

    fn to_log_entry_keep(&self) -> LogEntryKeep {
        LogEntryKeep {
            file_idx: self.file_idx,
            ptr: Some(Rc::clone(&self.ptr)),
        }
    }

    pub fn to_log_entry_weak(&self) -> LogEntryWeak {
        LogEntryWeak {
            file_idx: self.file_idx,
            ptr: Rc::downgrade(&self.ptr),
        }
    }

    pub fn to_log_entry_strong(&self) -> LogEntryStrong {
        LogEntryStrong {
            file_idx: self.file_idx,
            ptr: Some(Rc::clone(&self.ptr)),
        }
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

    fn load_pointer<F: RWS>(&mut self, m: &mut HashItems<LogEntry>, f: &mut LogFile<F>) {
        self.ptr = Some(get_ptr(self.file_idx, m, f))
    }

    fn to_log_entry_strong(&self) -> LogEntryStrong {
        LogEntryStrong {
            file_idx: self.file_idx,
            ptr: self.ptr.as_ref().map(|ptr| Rc::clone(ptr)),
        }
    }

    fn to_log_entry_weak(&self) -> LogEntryWeak {
        LogEntryWeak {
            file_idx: self.file_idx,
            ptr: Rc::downgrade(&self.get_ptr()),
        }
    }

    pub fn new(file_idx: u64, ptr: StrongPtr) -> LogEntryKeep {
        LogEntryKeep {
            file_idx,
            ptr: Some(ptr),
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

#[derive(Serialize, Deserialize)]
pub struct LogEntryStrong {
    pub file_idx: u64,
    #[serde(skip)]
    ptr: Option<StrongPtr>,
}

impl Eq for LogEntryStrong {}

impl PartialEq for LogEntryStrong {
    fn eq(&self, other: &Self) -> bool {
        self.file_idx == other.file_idx
    }
}

pub fn get_ptr<F: RWS>(
    file_idx: u64,
    m: &mut HashItems<LogEntry>,
    f: &mut LogFile<F>,
) -> StrongPtr {
    // check the map
    match m.get(file_idx) {
        Some(entry) => Rc::clone(entry),
        None => {
            // the entry must be loaded from the file
            debug!("load from disk at idx {}", file_idx);
            let entry = Rc::new(RefCell::new(
                LogEntry::from_file(file_idx, m, f).expect("unable to load entry from file"),
            ));
            let ptr = Rc::clone(&entry);
            if m.store_from_disk(file_idx, entry).is_some() {
                panic!("found unexpected entry at file index");
            }
            ptr
        }
    }
}

impl LogEntryStrong {
    pub fn new(ptr: StrongPtr, file_idx: u64) -> LogEntryStrong {
        LogEntryStrong {
            file_idx,
            ptr: Some(ptr),
        }
    }

    pub fn get_ptr_in_memory(&self) -> Option<&StrongPtr> {
        self.ptr.as_ref()
    }

    fn drop_ptr(&mut self) -> Result<StrongPtr, Error> {
        if let Some(ptr) = self.ptr.take() {
            return Ok(ptr);
        }
        Err(Error::LogError(LogError::OpAlreadyDropped))
    }

    pub fn log_entry_keep<F: RWS>(
        &mut self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> LogEntryKeep {
        LogEntryKeep {
            file_idx: self.file_idx,
            ptr: Some(self.get_ptr(m, f)),
        }
    }

    pub fn strong_ptr_idx<F: RWS>(
        &mut self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> StrongPtrIdx {
        StrongPtrIdx {
            file_idx: self.file_idx,
            ptr: self.get_ptr(m, f),
        }
    }

    pub fn get_ptr<F: RWS>(
        &mut self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> StrongPtr {
        if self.ptr.is_none() {
            self.ptr = Some(get_ptr(self.file_idx, m, f));
        }
        Rc::clone(self.ptr.as_ref().unwrap())
    }

    pub fn clone_weak(&self) -> LogEntryWeak {
        LogEntryWeak {
            file_idx: self.file_idx,
            ptr: match self.ptr.as_ref() {
                None => Weak::default(),
                Some(entry) => Rc::downgrade(entry),
            },
        }
    }

    pub fn clone_strong(&self) -> LogEntryStrong {
        LogEntryStrong {
            file_idx: self.file_idx,
            ptr: self.ptr.as_ref().map(|entry| Rc::clone(entry)),
        }
    }

    // panics if the entry is not in memory
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
    fn default() -> Self {
        LogEntryStrong {
            file_idx: 0,
            ptr: None,
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct LogEntryWeak {
    file_idx: u64,
    #[serde(skip)]
    ptr: WeakPtr,
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

    fn as_log_entry_strong<F: RWS>(
        &self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> LogEntryStrong {
        LogEntryStrong {
            file_idx: self.file_idx,
            ptr: Some(self.get_ptr(m, f)),
        }
    }

    pub fn to_strong_ptr_idx<F: RWS>(
        &self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> StrongPtrIdx {
        StrongPtrIdx {
            file_idx: self.file_idx,
            ptr: self.get_ptr(m, f),
        }
    }

    pub fn get_ptr<F: RWS>(&self, m: &mut HashItems<LogEntry>, f: &mut LogFile<F>) -> StrongPtr {
        self.ptr
            .upgrade()
            .unwrap_or_else(|| get_ptr(self.file_idx, m, f))
    }

    // Returns the object at the next pointer only if it is already in memory
    fn get_prt_if_in_memory(&self) -> Option<StrongPtr> {
        self.ptr.upgrade()
    }

    pub fn clone_weak(&self) -> LogEntryWeak {
        LogEntryWeak {
            file_idx: self.file_idx,
            ptr: Weak::clone(&self.ptr),
        }
    }

    pub fn clone_strong(&self) -> LogEntryStrong {
        LogEntryStrong {
            file_idx: self.file_idx,
            ptr: self.ptr.upgrade(),
        }
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
    pub prev_entry: Option<LogEntryStrong>,
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
            Some(LogEntryStrong {
                file_idx: prev,
                ptr: None,
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

    pub fn get_prev<F: RWS>(
        &mut self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Option<StrongPtr> {
        self.prev_entry.as_mut().map(|entry| entry.get_ptr(m, f))
    }

    pub fn get_prev_strong(&self) -> Option<LogEntryStrong> {
        self.prev_entry.as_ref().map(|entry| entry.clone_strong())
    }

    pub fn get_next<F: RWS>(
        &self,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Option<StrongPtr> {
        self.next_entry.as_ref().map(|entry| entry.get_ptr(m, f))
    }

    pub fn get_next_strong(&self) -> Option<LogEntryStrong> {
        self.next_entry.as_ref().map(|entry| entry.clone_strong())
    }

    // Returns the object at the next pointer only if it is already in memory
    fn get_next_if_in_memory(&self) -> Option<StrongPtr> {
        self.next_entry
            .as_ref()
            .and_then(|entry| entry.get_prt_if_in_memory())
    }

    // Drop the strong reference to the previous entry if there is one.
    fn drop_prev(&mut self) -> Result<StrongPtr, Error> {
        if let Some(prev) = self.prev_entry.as_mut() {
            return prev.drop_ptr();
        }
        Err(Error::LogError(LogError::OpAlreadyDropped))
    }
}

// drops the reference from the input entry, so it will be gc'd even if there is no next
pub fn drop_self(entry: &mut LogEntryStrong) -> Result<StrongPtr, Error> {
    entry.drop_ptr()
}

// drops the strong reference to this entry from the next entry, so it will be GC'd.
pub fn drop_entry(entry: &StrongPtrIdx) -> Result<StrongPtr, Error> {
    // unlink from the next in the log
    if let Some(nxt) = entry.ptr.borrow().log_pointers.get_next_if_in_memory() {
        return nxt.borrow_mut().log_pointers.drop_prev();
    }
    Err(Error::LogError(LogError::OpAlreadyDropped))
    /*
    // unlink the adjacent nodes
    drop_item_total_order(entry);
    let (prev, next) = (entry.borrow().get_prev(), entry.borrow().get_next());
    match next.as_ref() {
        Some(nxt) => {
            match prev.as_ref() {
                None => nxt.borrow_mut().set_prev(None), // no prev, but a next
                Some(prv) => { // both a prev and next
                    nxt.borrow_mut().set_prev(Some(Rc::clone(prv)));
                    prv.borrow_mut().set_next(Some(Rc::downgrade(nxt)));
                }
            }
        },
        None => { // no next, but a prev
            if let Some(prv) = prev.as_ref() {
                prv.borrow_mut().set_next(None)
            }
        }
    }*/
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct OuterOp {
    pub log_index: u64,
    pub op: OpState,
    pub include_in_hash: bool,
    pub arrived_late: bool,
    // hash: Hash,
    // verification: Verify,
}

impl OuterOp {}

/*
impl PartialOrd for OuterOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OuterOp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.log_index.cmp(&other.log_index) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.op.cmp(&other.op)
        }
    }
}


impl Eq for OuterOp {}

impl PartialEq for OuterOp {
    fn eq(&self, other: &Self) -> bool {
        self.op == other.op && self.log_index == other.log_index
    }
}
*/

impl Display for OuterOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Op(log_index: {}, op: {})", self.log_index, self.op.op)
    }
}

#[derive(Serialize, Deserialize, PartialEq, Eq)]
pub struct OuterSp {
    pub sp: SpState,
    pub last_op: Option<LogEntryWeak>, // the last op in the total order this sp supported
    pub not_included_ops: Vec<LogEntryKeep>, // the ops before last_op in the log that are not included in the sp
    pub prev_sp: Option<LogEntryWeak>,       // the previous sp in the total order in the log
}

// sets the next Sp in the total ordering of the log.
pub fn set_next_sp(
    prev_sp: &StrongPtrIdx,
    old_next_sp: Option<&StrongPtrIdx>,
    new_next: &StrongPtrIdx,
) {
    new_next.ptr.borrow_mut().entry.mut_as_sp().prev_sp = Some(prev_sp.to_log_entry_weak()); //Some(Rc::downgrade(prev_sp));
    match old_next_sp {
        None => (),
        Some(old_next) => {
            old_next.ptr.borrow_mut().entry.mut_as_sp().prev_sp =
                Some(new_next.to_log_entry_weak()); // Some(Rc::downgrade(new_next))
        }
    }
}

impl OuterSp {
    pub fn finish_deserialize<F: RWS>(&mut self, m: &mut HashItems<LogEntry>, f: &mut LogFile<F>) {
        for nxt in self.not_included_ops.iter_mut() {
            nxt.load_pointer(m, f);
        }
    }

    // returns all operations that have not been included in this SP in the log
    pub fn get_ops_after_iter<'a, F: RWS>(
        &'a self,
        m: &'a mut HashItems<LogEntry>,
        f: &'a mut LogFile<F>,
    ) -> Result<Box<dyn Iterator<Item = StrongPtrIdx> + 'a>, Error> {
        let (not_included_iter, log_iter) = self.get_iters(m, f)?;
        let iter = not_included_iter.merge_by(log_iter, |x, y| {
            x.ptr.borrow().entry.get_entry_info() <= y.ptr.borrow().entry.get_entry_info()
        });
        Ok(Box::new(iter))
    }

    pub fn check_sp_exact<F: RWS>(
        &self,
        new_sp: Sp,
        exact: &[EntryInfo],
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Result<OuterSp, Error> {
        // create a total order iterator over the operations including the unused items from the previous
        // SP that takes the exact set from exact, and returns the rest as unused, until last_op
        let (not_included_iter, log_iter) = self.get_iters(m, f)?;
        let op_iter = total_order_exact_iterator(
            not_included_iter,
            log_iter,
            exact.iter().cloned(),
            new_sp.additional_ops.iter().cloned(),
        );
        let (not_included_ops, last_op) = self.perform_check_sp(&new_sp, op_iter)?;
        let sp_state = SpState::from_sp(new_sp, f.serialize_option())?;
        Ok(OuterSp {
            sp: sp_state,
            last_op,
            not_included_ops,
            prev_sp: None,
        })
    }

    pub fn check_sp_log_order<F: RWS>(
        &self,
        new_sp: Sp,
        included: &[EntryInfo],
        not_included: &[EntryInfo],
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
    ) -> Result<OuterSp, Error> {
        // create a total order iterator over the operations of the log including the operations not included in thre previous sp
        // it uses included and not included to decide what operations to include
        // furthermore it includes items that have include_in_hash = true and are after the last operation of the previous
        // SP in the log
        let (not_included_iter, log_iter) = self.get_iters(m, f)?;
        let op_iter = total_order_check_iterator(
            not_included_iter,
            log_iter,
            included.iter().cloned(),
            not_included.iter().cloned(),
            new_sp.additional_ops.iter().cloned(),
        );
        let (not_included_ops, last_op) = self.perform_check_sp(&new_sp, op_iter)?;
        let sp_state = SpState::from_sp(new_sp, f.serialize_option())?;
        Ok(OuterSp {
            sp: sp_state,
            last_op,
            not_included_ops,
            prev_sp: None,
        })
    }

    fn get_iters<'a, F: RWS>(
        &'a self,
        m: &'a mut HashItems<LogEntry>,
        f: &'a mut LogFile<F>,
    ) -> Result<
        (
            impl Iterator<Item = StrongPtrIdx> + 'a,
            TotalOrderAfterIterator<F>,
        ),
        Error,
    > {
        // we make an iterator that goes through the log in total order
        // the iterator starts from the log entry of the last op included in the previous SP (self) and traverses the log
        // from there in total order, returning the entries that occur later in the log, plus the ops that are in the
        // not_included list of the previous SP.
        let not_included_iter = self
            .not_included_ops
            .iter()
            .map(|nxt| StrongPtrIdx::new(nxt.file_idx, Rc::clone(nxt.ptr.as_ref().unwrap()))); // items from previous sp that are earlier in the log, we need to check if these are included
        let last_op = self
            .last_op
            .as_ref()
            .ok_or(Error::LogError(LogError::PrevSpHasNoLastOp))?
            .to_strong_ptr_idx(m, f);
        // we only want operations after the last op of the previous sp in the log
        let log_iter = if self.sp.sp.is_init() {
            // for the inital SP, we always start from the first op, and include all ops
            total_order_after_all_iterator(Some(&last_op), m, f)
        } else {
            // if prev sp is not the inital SP, then we need to move forward 1 op since last op was already included in prev sp
            let mut log_iter = total_order_after_iterator(Some(&last_op), m, f);
            log_iter.next();
            log_iter
        };
        Ok((not_included_iter, log_iter))
    }

    fn perform_check_sp(
        &self,
        new_sp: &Sp,
        mut op_iter: impl Iterator<Item = (Supported, StrongPtrIdx)>,
    ) -> Result<(Vec<LogEntryKeep>, Option<LogEntryWeak>), Error> {
        let mut hasher = verification::new_hasher();
        hasher.update(self.sp.hash.as_bytes());
        let mut not_included_ops = vec![];
        let mut last_op = None;
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
                Supported::NotSupported => {
                    not_included_ops.push(nxt_op.to_log_entry_keep()); // Rc::downgrade(&nxt_op));
                    return Ok(count);
                }
            }
            // add the hash of the op
            hasher.update(nxt_op.ptr.borrow().entry.as_op().op.hash.as_bytes());
            count += 1;
            // update the last op pointer
            last_op = Some(nxt_op.to_log_entry_weak()); // Rc::downgrade(&nxt_op));
            Ok(count)
        }));
        if count != new_sp.new_ops_supported {
            Err(Error::LogError(LogError::NotEnoughOpsForSP))
        } else if new_sp.support_hash == hasher.finalize() {
            Ok((not_included_ops, last_op))
        } else {
            Err(Error::LogError(LogError::SpHashNotComputed))
        }
    }

    pub fn get_prev_sp(&self) -> Option<LogEntryStrong> {
        self.prev_sp.as_ref().map(|prv| prv.clone_strong())
    }
}

impl Display for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SP")
    }
}

impl Debug for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub fn total_order_iterator<'a, F>(
    start: Option<&LogEntryStrong>,
    forward: bool,
    m: &'a mut HashItems<LogEntry>,
    f: &'a mut LogFile<F>,
) -> TotalOrderIterator<'a, F> {
    TotalOrderIterator {
        prev_entry: start.map(|op| op.clone_strong()),
        forward,
        m,
        f,
    }
}

pub struct TotalOrderIterator<'a, F> {
    pub prev_entry: Option<LogEntryStrong>,
    pub forward: bool,
    pub m: &'a mut HashItems<LogEntry>,
    pub f: &'a mut LogFile<F>,
}

impl<'a, F: RWS> Iterator for TotalOrderIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = match self.prev_entry.as_mut() {
            None => None,
            Some(prv) => Some(StrongPtrIdx::new(prv.file_idx, prv.get_ptr(self.m, self.f))),
        };
        self.prev_entry = match self.prev_entry.take() {
            None => None,
            Some(mut prv) => {
                if self.forward {
                    prv.get_ptr(self.m, self.f)
                        .borrow()
                        .to_pointers
                        .get_next_to_strong()
                } else {
                    prv.get_ptr(self.m, self.f)
                        .borrow()
                        .to_pointers
                        .get_prev_to_strong()
                }
            }
        };
        ret
    }
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input
pub struct TotalOrderAfterIterator<'a, F> {
    iter: TotalOrderIterator<'a, F>,
    min_index: u64,
}

impl<'a, F: RWS> Iterator for TotalOrderAfterIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(nxt) = self.iter.next() {
            if nxt.ptr.borrow().log_index >= self.min_index {
                return Some(nxt);
            }
        }
        None
    }
}

pub fn total_order_after_all_iterator<'a, F>(
    start: Option<&StrongPtrIdx>,
    m: &'a mut HashItems<LogEntry>,
    f: &'a mut LogFile<F>,
) -> TotalOrderAfterIterator<'a, F> {
    TotalOrderAfterIterator {
        iter: total_order_iterator(
            start.map(|entry| entry.to_log_entry_strong()).as_ref(),
            true,
            m,
            f,
        ),
        min_index: 0,
    }
}

pub fn total_order_after_iterator<'a, F>(
    start: Option<&StrongPtrIdx>,
    m: &'a mut HashItems<LogEntry>,
    f: &'a mut LogFile<F>,
) -> TotalOrderAfterIterator<'a, F> {
    TotalOrderAfterIterator {
        iter: total_order_iterator(
            start.map(|entry| entry.to_log_entry_strong()).as_ref(),
            true,
            m,
            f,
        ),
        min_index: start.map_or(0, |entry| entry.ptr.borrow().log_index),
    }
}
#[derive(Debug)]
pub enum Supported {
    Supported,
    SupportedData(EntryInfoData),
    NotSupported,
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input,
// or were from the not included ops of the previous sp, and are part included in the exact iterator input.
pub struct TotalOrderExactIterator<'a, J, K>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    iter: Box<dyn Iterator<Item = StrongPtrIdx> + 'a>,
    extra_info_check: ExtraInfoCheck<K>,
    exact_check: IncludedCheck<J>,
}

pub fn total_order_exact_iterator<'a, J, K, I, F: RWS>(
    prev_not_included: I,
    log_iter: TotalOrderAfterIterator<'a, F>,
    exact: J,
    extra_info: K,
) -> TotalOrderExactIterator<'a, J, K>
where
    I: Iterator<Item = StrongPtrIdx> + 'a,
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    // now we merge the prev_not_included and the log_iter into a single iterator them so we traverse the two of them in total order
    let iter = Box::new(log_iter.merge_by(prev_not_included, |x, y| {
        x.ptr.borrow().entry.get_entry_info() <= y.ptr.borrow().entry.get_entry_info()
    }));
    TotalOrderExactIterator {
        iter,
        extra_info_check: ExtraInfoCheck {
            last_extra_info: None,
            extra_info,
        },
        exact_check: IncludedCheck {
            last_included: None,
            included: exact,
        },
    }
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
            if let Some(extra_info) = self.extra_info_check.check_extra_info(info) {
                return Some((Supported::SupportedData(extra_info), nxt));
            }
            // next check if the op is supported as part of the exact list
            if self.exact_check.check_supported(info) {
                return Some((Supported::Supported, nxt));
            }
            // oterwise it is not included
            return Some((Supported::NotSupported, nxt));
        };
        None
    }
}

struct IncludedCheck<J>
where
    J: Iterator<Item = EntryInfo>,
{
    last_included: Option<EntryInfo>,
    included: J,
}

impl<J> IncludedCheck<J>
where
    J: Iterator<Item = EntryInfo>,
{
    fn check_supported(&mut self, entry: EntryInfo) -> bool {
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
                return Some(*last_extra_info);
            }
        }
        None
    }
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input,
// except for those that are supported/unsupported
pub struct TotalOrderCheckIterator<'a, J, K>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    iter: Box<dyn Iterator<Item = StrongPtrIdx> + 'a>,
    extra_info_check: ExtraInfoCheck<K>,
    included_check: IncludedCheck<J>,
    not_included_check: IncludedCheck<J>,
}

//fn entry_less_than(x: &LogEntryStrong, y: &LogEntryStrong) -> bool {
//  x.get_ptr().borrow().entry.get_entry_info() <= y.get_ptr().borrow().entry.get_entry_info()
//}

pub fn total_order_check_iterator<'a, J, K, I, F: RWS>(
    prev_not_included: I,
    log_iter: TotalOrderAfterIterator<'a, F>,
    included: J,
    not_included: J,
    extra_info: K,
) -> TotalOrderCheckIterator<'a, J, K>
where
    I: Iterator<Item = StrongPtrIdx> + 'a,
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    // now we merge the prev_not_included and the log_iter into a single iterator them so we traverse the two of them in total order
    let iter = Box::new(log_iter.merge_by(prev_not_included, |x, y| {
        x.ptr.borrow().entry.get_entry_info() <= y.ptr.borrow().entry.get_entry_info()
    }));
    TotalOrderCheckIterator {
        iter,
        extra_info_check: ExtraInfoCheck {
            last_extra_info: None,
            extra_info,
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

impl<'a, J, K> Iterator for TotalOrderCheckIterator<'a, J, K>
where
    J: Iterator<Item = EntryInfo>,
    K: Iterator<Item = EntryInfoData>,
{
    type Item = (Supported, StrongPtrIdx);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(nxt) = self.iter.next() {
            let info = nxt.ptr.borrow().entry.get_entry_info();
            // first check if the op is supported by an extra info field
            if let Some(extra_info) = self.extra_info_check.check_extra_info(info) {
                return Some((Supported::SupportedData(extra_info), nxt));
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

pub struct LogIterator<'a, F> {
    pub prv_entry: Option<StrongPtrIdx>,
    pub m: &'a mut HashItems<LogEntry>,
    pub f: &'a mut LogFile<F>,
}

impl<'a, F: RWS> Iterator for LogIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prv_entry.as_ref().cloned();
        self.prv_entry = match self.prv_entry.take() {
            None => None,
            Some(prv) => prv
                .ptr
                .borrow()
                .get_prev()
                .map(|mut entry| entry.strong_ptr_idx(self.m, self.f)),
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
            hash_items::HashItems,
            log_file::open_log_file,
            log_file::LogFile,
            op::{BasicInfo, EntryInfo, OpState},
            sp::SpState,
        },
        rw_buf::RWS,
        verification::{hash, Id, TimeInfo, TimeTest},
    };

    use super::{
        LogEntry, LogEntryKeep, LogEntryStrong, LogEntryWeak, LogPointers, OuterOp, OuterSp,
        PrevEntry, StrongPtr, TotalOrderPointers,
    };

    fn to_log_entry_weak(ptr: Option<&StrongPtr>) -> Option<LogEntryWeak> {
        ptr.map(|nxt| LogEntryWeak {
            file_idx: nxt.borrow().file_index,
            ptr: Rc::downgrade(nxt),
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
    ) -> StrongPtr {
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
                prev_entry: to_log_entry_strong(prev_log),
            },
            to_pointers: TotalOrderPointers {
                next_to: to_log_entry_weak(next_to),
                prev_to: to_log_entry_weak(prev_to),
            },
            ser_size: None,
        }));
        if let Some(prev) = prev_log {
            prev.borrow_mut().log_pointers.next_entry = to_log_entry_weak(Some(&log_entry));
        }
        if let Some(prev) = prev_to {
            prev.borrow_mut().to_pointers.next_to = to_log_entry_weak(Some(&log_entry));
        }
        log_entry
    }

    fn make_outer_op<F: RWS>(
        id: Id,
        log_index: u64,
        ti: &mut TimeTest,
        f: &LogFile<F>,
    ) -> PrevEntry {
        let op = OpState::new(id, ti, f.serialize_option()).unwrap();
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
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
        prev_sp: Option<&LogEntryKeep>,
        last_op: Option<&LogEntryKeep>,
        not_included_ops: Vec<LogEntryKeep>,
    ) -> LogEntryKeep {
        let prev_sp_info = match prev_sp.as_ref() {
            None => EntryInfo {
                basic: BasicInfo {
                    time: ti.now_monotonic(),
                    id,
                },
                hash: hash(b"some msg"),
            },
            Some(sp) => sp.get_ptr().borrow().entry.get_entry_info(),
        };
        let sp_state = SpState::new(
            id,
            ti.now_monotonic(),
            not_included_ops
                .iter()
                .map(|op| op.get_ptr().borrow().entry.get_entry_info().hash),
            vec![],
            prev_sp_info,
            f.serialize_option(),
        )
        .unwrap();
        let outer_sp = OuterSp {
            sp: sp_state,
            last_op: last_op.map(|op| op.to_log_entry_weak()),
            not_included_ops,
            prev_sp: prev_sp.as_ref().map(|op| op.to_log_entry_weak()),
        };
        let entry = PrevEntry::Sp(outer_sp);
        entry.check_hash(f.serialize_option()).unwrap();
        let log_entry = make_log_entry(
            entry,
            prev_sp.map(|sp| sp.get_ptr()),
            None,
            last_op.map(|op| op.get_ptr()),
            None,
        );
        log_entry.borrow_mut().write_pointers_initial(m, f).unwrap();
        f.seek_to(log_entry.borrow().file_index).unwrap();
        let deser_log_entry = LogEntry::from_file(log_entry.borrow().file_index, m, f).unwrap();
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
            assert_eq!(*op1.get_ptr().borrow(), *op2.get_ptr().borrow());
        }

        m.clear();
        let file_idx = log_entry.borrow().file_index;
        LogEntryKeep {
            file_idx,
            ptr: Some(log_entry),
        }
    }

    fn make_op<F: RWS>(
        id: Id,
        ti: &mut TimeTest,
        m: &mut HashItems<LogEntry>,
        f: &mut LogFile<F>,
        log_entry: StrongPtr,
        prev_to: Option<StrongPtr>,
    ) -> LogEntryKeep {
        let op = make_outer_op(id, log_entry.borrow().log_index + 1, ti, f);
        let log_entry = make_log_entry(op, prev_to.as_ref(), None, Some(&log_entry), None);
        debug!(
            "new file index {}, log index {}",
            log_entry.borrow().file_index,
            log_entry.borrow().log_index
        );
        f.seek_to(log_entry.borrow().file_index - 8).unwrap();
        assert!(f.check_index().at_end);
        log_entry.borrow_mut().write_pointers_initial(m, f).unwrap();
        debug!("entry size {}", log_entry.borrow().ser_size.unwrap());
        f.seek_to(log_entry.borrow().file_index).unwrap();
        let deser_log_entry = LogEntry::from_file(log_entry.borrow().file_index, m, f).unwrap();
        assert_eq!(*log_entry.borrow(), deser_log_entry);
        // check that the prev to next to pointer is correct
        if let Some(prv) = prev_to {
            let deser_prev_to = LogEntry::from_file(prv.borrow().file_index, m, f).unwrap();
            assert_eq!(*prv.borrow(), deser_prev_to);
            debug!("prv after: {}, deser prv: {}", prv.borrow(), deser_prev_to)
        }
        let file_idx = log_entry.borrow().file_index;
        m.clear();
        LogEntryKeep {
            file_idx,
            ptr: Some(log_entry),
        }
    }

    #[test]
    fn serialize_entry() {
        let mut f = open_log_file("log_files/entry_log0.log", true, FileSR::new).unwrap();
        let mut ti = TimeTest::new();
        let id = 0;
        let mut m = HashItems::default();

        // insert the inital sp
        let mut log_entry = make_outer_sp(id, &mut ti, &mut m, &mut f, None, None, vec![]);
        let last_sp = log_entry.clone();
        let mut prev_to = None;
        let mut op_vec = vec![];
        // insert 10 ops
        for _ in 0..10 {
            log_entry = make_op(
                id,
                &mut ti,
                &mut m,
                &mut f,
                Rc::clone(log_entry.get_ptr()),
                prev_to,
            );
            prev_to = Some(Rc::clone(log_entry.get_ptr()));
            op_vec.push(log_entry.clone());
            m.clear();
        }
        // insert an op that uses a different total order
        prev_to = Some(Rc::clone(op_vec[0].get_ptr()));
        log_entry = make_op(
            id,
            &mut ti,
            &mut m,
            &mut f,
            Rc::clone(log_entry.get_ptr()),
            prev_to,
        );
        m.clear();

        // check sp deserialization that includes the ops
        make_outer_sp(
            id,
            &mut ti,
            &mut m,
            &mut f,
            Some(&last_sp),
            Some(&log_entry),
            op_vec,
        );
    }
}
