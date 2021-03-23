use bincode::DefaultOptions;
use log::debug;
use std::cell::{Ref, RefCell};
use std::ops::Deref;
use std::{cmp::Ordering, rc::Rc};
use verification::{hash, TimeCheck, TimeInfo};

use crate::{
    rw_buf::RWS,
    verification::{self, Id},
};

use super::{
    entry::{
        set_to_pointers_append_to_log, LogEntry, LogEntryStrong, LogEntryWeak, LogIterator,
        LogOpIterator, LogPointers, OuterOp, OuterSp, PrevEntry, StrongPointers, StrongPtrIdx,
        TotalOrderIterator, TotalOrderPointers,
    },
    log_error::{LogError, Result},
    log_file::LogFile,
    op::{BasicInfo, EntryInfo, Op, OpState},
    sp::SpState,
};
use super::{hash_items::HashItems, sp::Sp};

struct PrevEntryHolder<'a> {
    prv_entry: Ref<'a, LogEntry>,
}

impl<'a> Deref for PrevEntryHolder<'a> {
    type Target = PrevEntry;

    fn deref(&self) -> &PrevEntry {
        &self.prv_entry.entry
    }
}

struct SpLocalTotalOrderIterator<'a, F: RWS> {
    id: Id,
    iter: TotalOrderIterator<'a, F>,
}

impl<'a, F: RWS> Iterator for SpLocalTotalOrderIterator<'a, F> {
    type Item = StrongPtrIdx;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(nxt) = self.iter.next() {
            let nxt_ret = nxt.ptr.borrow();
            let sp = &nxt_ret.entry.as_sp().sp;
            if sp.sp.info.id == self.id && !sp.sp.is_init() {
                return Some(nxt.clone());
            }
        }
        None
    }
}

enum LogItems {
    Empty,
    Single(LogSingle),
    Multiple(LogMultiple),
}

impl LogItems {
    fn log_iterator_from_end<'a, F: RWS>(&self, state: &'a LogState<F>) -> LogIterator<'a, F> {
        LogIterator::new(
            match self {
                LogItems::Empty => None,
                LogItems::Single(si) => Some(si.entry.to_strong_ptr_idx(state)), // ::clone( &si.entry)),
                LogItems::Multiple(mi) => Some(mi.last_entry.to_strong_ptr_idx(state)), //::clone( &mi.last_entry))
            },
            state,
            false,
        )
    }

    fn add_item<F: RWS>(
        &mut self,
        idx: u64,
        file_idx: u64,
        entry: PrevEntry,
        state: &LogState<F>,
    ) -> (StrongPtrIdx, StrongPointers) {
        let prv = match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(si.entry.clone()), // Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(mi.last_entry.clone()), //::clone(&mi.last_entry))
        };
        let log_entry = LogEntry {
            log_index: idx,
            file_index: file_idx,
            log_pointers: LogPointers {
                next_entry: None,
                prev_entry: prv.clone(),
            },
            to_pointers: TotalOrderPointers {
                next_to: None,
                prev_to: None,
            },
            entry,
            ser_size: None, // this will be updated once serialized
        };
        let new_entry = Rc::new(RefCell::new(log_entry));

        let new_entry_keep = StrongPtrIdx::new(file_idx, Rc::clone(&new_entry));
        let new_entry_weak: LogEntryWeak = (&new_entry_keep).into();
        let new_entry_strong = LogEntryStrong::new(Rc::clone(&new_entry), file_idx);
        match self {
            LogItems::Empty => {
                *self = LogItems::Single(LogSingle {
                    entry: new_entry_weak,
                })
            }
            LogItems::Single(si) => {
                si.entry
                    .get_ptr(state)
                    .borrow_mut()
                    .set_next(Some(new_entry_strong.into())); //::downgrade(&new_entry_ref)));
                let first = si.entry.clone(); // ::clone(&si.entry);
                *self = LogItems::Multiple(LogMultiple {
                    last_entry: new_entry_weak,
                    first_entry: first,
                })
            }
            LogItems::Multiple(lm) => {
                lm.last_entry
                    .get_ptr(state)
                    .borrow_mut()
                    .set_next(Some(new_entry_strong.into()));
                lm.last_entry = new_entry_weak;
            }
        }
        // store the item in the map in memory
        if let Some(replaced) = state.m.borrow_mut().store_recent(file_idx, new_entry) {
            // the oldest entry was removed from the map so it will be garbage collected
            debug!("dropped item at log index {}", replaced);
        }

        (
            new_entry_keep,
            StrongPointers {
                prev: prv.map(|p| p.to_strong_ptr_idx(state)),
                next: None,
            },
        )
    }

    fn get_last<F: RWS>(&self, state: &LogState<F>) -> Option<StrongPtrIdx> {
        match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(si.entry.to_strong_ptr_idx(state)),
            LogItems::Multiple(mi) => Some(mi.last_entry.to_strong_ptr_idx(state)),
        }
    }

    fn get_first<F: RWS>(&self, state: &LogState<F>) -> Option<StrongPtrIdx> {
        match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(si.entry.to_strong_ptr_idx(state)),
            LogItems::Multiple(mi) => Some(mi.first_entry.to_strong_ptr_idx(state)),
        }
    }
}

struct LogSingle {
    entry: LogEntryWeak,
}

struct LogMultiple {
    last_entry: LogEntryWeak,
    first_entry: LogEntryWeak,
}

pub struct Log<F: RWS> {
    index: u64,
    first_last: LogItems,
    last_op_total_order: Option<LogEntryWeak>,
    last_sp_total_order: Option<LogEntryWeak>,
    first_op_to: Option<(u64, EntryInfo)>, // the file index and info for the earliest op in the log
    init_hash: verification::Hash,
    init_sp: Option<LogEntryWeak>,
    init_sp_entry_info: EntryInfo,
    state: LogState<F>,
}

pub struct LogState<F: RWS> {
    pub f: RefCell<LogFile<F>>,
    pub m: RefCell<HashItems<LogEntry>>, // log entries in memory by their log file index
}

impl<F: RWS> Drop for Log<F> {
    fn drop(&mut self) {}
}

impl<F: RWS> Log<F> {
    pub fn new(log_file: LogFile<F>) -> Log<F> {
        let mut l = Log {
            index: 0,
            first_last: LogItems::Empty,
            last_op_total_order: None,
            last_sp_total_order: None,
            init_hash: hash(b"initial state"), // TODO should allow this as input?
            init_sp: None,
            first_op_to: None,
            // set_initial_sp_op: false,
            init_sp_entry_info: EntryInfo::default(),
            state: LogState {
                f: RefCell::new(log_file),
                m: RefCell::new(HashItems::default()),
            },
        };
        // insert the initial sp
        let init_sp = OuterSp {
            sp: SpState::from_sp(Sp::get_init(l.get_init_support()), l.serialize_option()).unwrap(),
            last_op: None,
            not_included_ops: vec![],
            late_included: vec![],
            supported_sp_log_index: None,
        };
        l.init_sp_entry_info = init_sp.sp.get_entry_info();
        let outer_sp = l
            .insert_outer_sp(init_sp)
            .expect("failed inserting the initial SP");
        l.init_sp = Some((&outer_sp).into());
        l
    }

    #[inline(always)]
    pub fn serialize_option(&self) -> DefaultOptions {
        self.state.f.borrow().serialize_option()
    }

    #[inline(always)]
    pub(crate) fn get_log_state(&self) -> &LogState<F> {
        &self.state
    }

    fn get_init_support(&self) -> EntryInfo {
        EntryInfo {
            basic: BasicInfo { time: 0, id: 0 },
            hash: self.init_hash,
        }
    }

    #[inline(always)]
    pub fn is_init_sp(&self, sp: &EntryInfo) -> bool {
        &self.get_initial_entry_info() == sp
    }

    #[inline(always)]
    pub fn get_initial_entry_info(&self) -> EntryInfo {
        self.init_sp_entry_info
    }

    pub fn get_last_sp_id(&mut self, id: Id) -> Result<StrongPtrIdx> {
        for nxt_sp in self.sp_total_order_iterator() {
            if nxt_sp.ptr.borrow().entry.as_sp().sp.sp.info.id == id
                && !nxt_sp.ptr.borrow().entry.as_sp().sp.sp.is_init()
            {
                return Ok(nxt_sp);
            }
        }
        Err(LogError::IdHasNoSp)
    }

    pub fn get_initial_sp(&mut self) -> Result<StrongPtrIdx> {
        self.find_sp(self.get_initial_entry_info(), None)
    }

    #[inline(always)]
    fn check_valid_init_sp(&mut self, ei: EntryInfo, nxt_sp: StrongPtrIdx) -> Result<StrongPtrIdx> {
        // if it is the inital SP, verify it is defined correctly
        Sp::is_init_entry(ei, &self.init_sp_entry_info.hash)?;
        Ok(nxt_sp)
    }

    /// Returns the first operation in the log in total order
    pub fn get_first_op(&self) -> Option<LogEntryWeak> {
        self.first_op_to
            .map(|(file_idx, _)| LogEntryWeak::from_file_idx(file_idx))
    }

    /// Searches for to_find_prev from the end of the log, returning an error if it is not found.
    /// If to_find_nxt is non-None, then to_find_prev should be the SP that to_find_nxt supports,
    /// this should be used when checking to insert a new SP.
    /// If to_find_nxt is found in the log, then LogError::SpAlreadyExists is returned.
    pub fn find_sp(
        &mut self,
        to_find_prev: EntryInfo,
        to_find_nxt: Option<EntryInfo>,
    ) -> Result<StrongPtrIdx> {
        let sp: Option<LogEntryStrong> = self.last_sp_total_order.as_ref().map(|sp| sp.into());
        match sp {
            Some(sp) => self.find_sp_from(to_find_prev, to_find_nxt, sp),
            None => Err(LogError::PrevSpNotFound),
        }
    }

    /// Same as self.find_sp(), except uses the given iterator
    pub fn find_sp_from(
        &mut self,
        to_find_prev: EntryInfo,
        mut to_find_nxt: Option<EntryInfo>,
        from: LogEntryStrong,
    ) -> Result<StrongPtrIdx> {
        for nxt_sp in self.sp_total_order_iterator_from(from) {
            let ei = nxt_sp.ptr.borrow().entry.as_sp().sp.get_entry_info();
            debug!("nxt sp to check {:?}, looking for {:?}", ei, to_find_prev);
            if let Some(find_nxt) = to_find_nxt.as_ref() {
                match ei.cmp(find_nxt) {
                    Ordering::Less => to_find_nxt = None,
                    Ordering::Equal => return Err(LogError::SpAlreadyExists),
                    Ordering::Greater => (),
                }
            }
            match ei.cmp(&to_find_prev) {
                Ordering::Less => break,
                Ordering::Equal => {
                    return self.check_valid_init_sp(ei, nxt_sp);
                }
                Ordering::Greater => (),
            }
        }
        Err(LogError::PrevSpNotFound)
    }

    fn drop_first(&mut self) -> Result<StrongPtrIdx> {
        // self.first_last.drop_first(&self.state)
        self.state
            .m
            .borrow_mut()
            .drop_entry()
            .map(|e| (&e).into())
            .ok_or(LogError::OpAlreadyDropped)
    }

    fn sp_local_total_order_iterator(&self, id: Id) -> SpLocalTotalOrderIterator<F> {
        SpLocalTotalOrderIterator {
            id,
            iter: self.sp_total_order_iterator(),
        }
    }

    fn sp_total_order_iterator_from(&self, sp: LogEntryStrong) -> TotalOrderIterator<F> {
        TotalOrderIterator {
            prev_entry: Some(sp),
            forward: false,
            state: &self.state,
        }
    }

    fn sp_total_order_iterator(&self) -> TotalOrderIterator<F> {
        TotalOrderIterator {
            prev_entry: match &self.last_sp_total_order {
                None => None,
                Some(prv) => Some(prv.into()),
            },
            forward: false,
            state: &self.state,
        }
    }

    pub fn op_total_order_iterator_from_last(&self) -> TotalOrderIterator<F> {
        TotalOrderIterator {
            prev_entry: match &self.last_op_total_order {
                None => None,
                Some(prv) => Some(prv.into()),
            },
            forward: false,
            state: &self.state,
        }
    }

    pub fn op_total_order_iterator_from_first(&mut self) -> TotalOrderIterator<F> {
        TotalOrderIterator {
            prev_entry: match &self.first_op_to {
                None => None,
                Some((idx, _)) => Some(LogEntryStrong::from_file_idx(*idx)),
            },
            forward: true,
            state: &mut self.state,
        }
    }

    pub(crate) fn op_iterator_from(
        &mut self,
        entry: LogEntryStrong,
        forward: bool,
    ) -> LogOpIterator<F> {
        let i = self.log_iterator_from(entry, forward);
        LogOpIterator::new(i)
    }

    fn log_iterator_from(&mut self, mut entry: LogEntryStrong, forward: bool) -> LogIterator<F> {
        let prv_entry = Some(entry.strong_ptr_idx(&self.state));
        LogIterator::new(prv_entry, &self.state, forward)
    }

    fn log_iterator_from_end(&self) -> LogIterator<F> {
        self.first_last.log_iterator_from_end(&self.state)
    }

    pub fn check_sp<T>(
        &mut self,
        sp: Sp,
        late_included: &[EntryInfo],
        not_included: &[EntryInfo],
        ti: &T,
    ) -> Result<(OuterSp, Vec<StrongPtrIdx>)>
    where
        T: TimeInfo,
    {
        sp.validate(&self.init_sp_entry_info.hash, ti)?;
        let sp_supported_info = sp.supported_sp_info;
        let sp_state = SpState::from_sp(sp, &mut self.serialize_option())?;
        let sp_ref = self
            .find_sp(sp_supported_info, Some(sp_state.get_entry_info()))?
            .ptr;
        let sp_ptr = sp_ref.borrow();
        let prev_sp_log_idx = sp_ptr.log_index;
        sp_ptr.entry.as_sp().check_sp_log_order(
            prev_sp_log_idx,
            sp_state,
            &late_included,
            not_included.iter().cloned(),
            self.get_first_op(),
            &self.state,
        )
    }

    pub fn get_sp_exact(&mut self, info: EntryInfo) -> Result<(OuterSp, Vec<StrongPtrIdx>)> {
        if info == self.get_initial_entry_info() {
            return Err(LogError::IsInitSP);
        }
        let sp_ref = self.find_sp(info, None)?;
        let sp_ptr = sp_ref.ptr.borrow();
        let sp = sp_ptr.entry.as_sp();
        let prev_sp_info = sp.sp.sp.supported_sp_info;
        let prev_sp = self
            .find_sp_from(prev_sp_info, None, (&sp_ref).into())
            .expect("should be able to find prev Sp for Sp already decided");
        let prev_sp_ptr = prev_sp.ptr.borrow();
        let not_included = sp
            .not_included_ops
            .iter()
            .map(|sp| sp.get_ptr(&self.state).borrow().entry.get_entry_info());
        let late_included: Vec<_> = sp
            .late_included
            .iter()
            .map(|sp| sp.get_ptr(&self.state).borrow().entry.get_entry_info())
            .collect();
        let ret = prev_sp_ptr
            .entry
            .as_sp()
            .check_sp_log_order(
                prev_sp_ptr.log_index,
                sp.sp.clone(),
                &late_included,
                not_included,
                self.get_first_op(),
                &self.state,
            )
            .expect("must be able to generate SP for entry already created");
        Ok(ret)
    }

    pub fn check_sp_exact<T>(
        &mut self,
        sp: Sp,
        exact: &[EntryInfo],
        ti: &T,
    ) -> Result<(OuterSp, Vec<StrongPtrIdx>)>
    where
        T: TimeInfo,
    {
        sp.validate(&self.init_sp_entry_info.hash, ti)?;
        let sp_supported_info = sp.supported_sp_info;
        let sp_state = SpState::from_sp(sp, &mut self.serialize_option())?;
        let sp_ref = self
            .find_sp(sp_supported_info, Some(sp_state.get_entry_info()))?
            .ptr;
        let sp_ptr = sp_ref.borrow();
        let prev_sp_log_index = sp_ptr.log_index;
        sp_ptr.entry.as_sp().check_sp_exact(
            prev_sp_log_index,
            sp_state,
            exact,
            self.get_first_op(),
            &self.state,
        )
    }

    fn add_to_log(&mut self, entry: PrevEntry) -> (StrongPtrIdx, StrongPointers) {
        self.index += 1;
        // file_index is where the item will be written in the file
        let file_index = self.state.f.borrow().get_end_index();
        debug!("inserting log index {}", self.index);
        // add the item to the log
        self.first_last
            .add_item(self.index, file_index, entry, &self.state)
    }

    /// Add the Sp to the log, assumes the Sp was already validated.
    pub fn insert_outer_sp(&mut self, sp: OuterSp) -> Result<StrongPtrIdx> {
        debug!("\nInsert new Sp {:?}\n", sp);
        let entry = PrevEntry::Sp(sp);
        // add the sp to the log
        let (new_sp_ref, log_pointers) = self.add_to_log(entry);
        // put the new sp in the total ordered list of sp
        let mut to_pointers = StrongPointers::default();
        for nxt in self.sp_total_order_iterator() {
            if nxt.ptr.borrow().entry.as_sp().sp < new_sp_ref.ptr.borrow().entry.as_sp().sp {
                to_pointers.prev = Some(nxt);
                break;
            } else {
                to_pointers.next = Some(nxt);
            }
        }
        // update the pointers and insert into the log file
        set_to_pointers_append_to_log(&to_pointers, &log_pointers, &new_sp_ref, &self.state);

        // update the last sp total order pointer if necessary
        self.last_sp_total_order = match self.last_sp_total_order.take() {
            None => Some((&new_sp_ref).into()),
            Some(last) => {
                if last.get_ptr(&self.state).borrow().entry.as_sp().sp
                    < new_sp_ref.ptr.borrow().entry.as_sp().sp
                {
                    Some((&new_sp_ref).into())
                } else {
                    Some(last)
                }
            }
        };
        Ok(new_sp_ref)
    }

    /// Add an operation to the log. The operation sould be checked for validity before this funciton is called.
    /// If the same operation has already been added to the log an error is returned.
    pub fn insert_op<T>(&mut self, op: Op, ti: &T) -> Result<StrongPtrIdx>
    where
        T: TimeInfo,
    {
        let TimeCheck {
            time_not_passed: _,
            include_in_hash,
            arrived_late,
        } = ti.arrived_time_check(op.info.time);
        let op_state = OpState::from_op(op, self.serialize_option())?;
        // compute the location of the operation in the total order of the log
        let mut to_pointers = StrongPointers::default();
        for nxt in self.op_total_order_iterator_from_last() {
            let cmp = nxt.ptr.borrow().entry.as_op().op.cmp(&op_state);
            match cmp {
                Ordering::Less => {
                    // we found where the op should be inserted
                    to_pointers.prev = Some(nxt);
                    break;
                }
                Ordering::Equal => return Err(LogError::OpAlreadyExists), // the op already found in the list
                Ordering::Greater => to_pointers.next = Some(nxt), // old_first_op = Some(nxt),            // keep looking
            }
        }
        let outer_op = PrevEntry::Op(OuterOp {
            op: op_state,
            log_index: self.index + 1,
            include_in_hash,
            arrived_late,
        });
        // add the op to the log
        let ei = outer_op.get_entry_info();
        let (new_op_ref, log_pointers) = self.add_to_log(outer_op);

        // update the total order pointers, and append to the log file
        set_to_pointers_append_to_log(&to_pointers, &log_pointers, &new_op_ref, &self.state);

        // see if need to set the op as the first op in the log
        match self.first_op_to.as_ref() {
            None => self.first_op_to = Some((new_op_ref.file_idx, ei)),
            Some((_, first_ei)) => {
                if &ei < first_ei {
                    self.first_op_to = Some((new_op_ref.file_idx, ei))
                }
            }
        }
        // update the last op pointer if necessary
        self.last_op_total_order = match self.last_op_total_order.take() {
            None => Some((&new_op_ref).into()),
            Some(last) => {
                if last.get_ptr(&self.state).borrow().entry.as_op().op
                    < new_op_ref.ptr.borrow().entry.as_op().op
                {
                    Some((&new_op_ref).into())
                } else {
                    Some(last)
                }
            }
        };

        // check the pointers are valid if debugging
        #[cfg(debug_assertions)]
        new_op_ref
            .ptr
            .borrow_mut()
            .to_pointers
            .check_ptrs(&self.state);
        Ok(new_op_ref)
    }
}

pub mod test_fns {
    use log::debug;

    use super::Log;
    use crate::log::log_error::LogError;
    use crate::rw_buf::RWS;

    pub fn print_log_from_end<F: RWS>(l: &mut Log<F>) {
        let iter = l.log_iterator_from_end();
        for nxt in iter {
            debug!("{:?}", nxt.ptr.borrow());
        }
    }

    /// Goes through each SP and verifies its supported log index corresponds correctly to the supported hash.
    pub fn check_sp_prev<F: RWS>(l: &mut Log<F>, check_last_op: bool) {
        let mut to_check = vec![];
        for sp in l.sp_total_order_iterator() {
            let sp_ref = sp.ptr.as_ref();
            let sp_ptr = sp_ref.borrow();
            match sp_ptr.entry.as_sp().supported_sp_log_index {
                Some(prev_sp_idx) => {
                    to_check.push((prev_sp_idx, sp_ptr.entry.as_sp().sp.sp.supported_sp_info))
                }
                None => assert!(sp_ptr.to_pointers.get_prev_to_strong().is_none()),
            }
        }
        for (log_idx, entry) in to_check {
            if check_last_op {
                let res = l.get_sp_exact(entry);
                if entry == l.get_initial_entry_info() {
                    assert_eq!(LogError::IsInitSP, res.unwrap_err());
                } else {
                    let (outer_sp, exact_op) = res.expect("unable to get sp exact");
                    // try to insert by exact
                    let sp_supported_info = outer_sp.sp.sp.supported_sp_info;
                    let sp_state = outer_sp.sp;
                    let exact: Vec<_> = exact_op
                        .into_iter()
                        .map(|op| op.ptr.borrow().entry.get_entry_info())
                        .collect();
                    let sp_ref = l.find_sp(sp_supported_info, None).unwrap().ptr;
                    let sp_ptr = sp_ref.borrow();
                    let prev_sp_log_index = sp_ptr.log_index;
                    sp_ptr
                        .entry
                        .as_sp()
                        .check_sp_exact(
                            prev_sp_log_index,
                            sp_state,
                            &exact,
                            l.get_first_op(),
                            &l.state,
                        )
                        .unwrap();
                }
            }
            assert_eq!(
                log_idx,
                l.find_sp(entry, None)
                    .unwrap()
                    .ptr
                    .as_ref()
                    .borrow()
                    .log_index
            )
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use bincode::Options;
    use std::{
        cmp,
        collections::{HashMap, VecDeque},
        fs::File,
    };

    use crate::{
        file_sr::FileSR,
        log::hash_items::{DEFAULT_DISK_ENTRIES, DEFAULT_MAX_ENTRIES},
        log::{entry::StrongPtr, op::tests::make_op_late},
        rw_buf::RWBuf,
        verification::TimeTest,
    };

    use crate::log::{
        entry::{total_order_after_iter, total_order_iterator},
        log_file::open_log_file,
        op::{gen_rand_data, get_empty_data, EntryInfoData},
    };

    use super::test_fns::check_sp_prev;
    use super::*;

    fn get_log_file_direct(idx: usize) -> LogFile<FileSR> {
        open_log_file(
            &format!("log_files/basic_log{}.log", idx),
            true,
            FileSR::new,
        )
        .unwrap()
    }

    fn get_log_file(idx: usize) -> LogFile<RWBuf<File>> {
        open_log_file(&format!("log_files/basic_log{}.log", idx), true, RWBuf::new).unwrap()
    }

    fn check_iter_total_order<T: Iterator<Item = StrongPtrIdx>, F: RWS>(i: T, state: &LogState<F>) {
        for op in i {
            // check we have the right pointers for the log
            if let Some(mut prv) = op.ptr.as_ref().borrow().to_pointers.get_prev_to_strong() {
                let prv_ptr = prv.get_ptr(state);
                assert_eq!(
                    prv_ptr
                        .as_ref()
                        .borrow()
                        .to_pointers
                        .get_next_to_strong()
                        .unwrap()
                        .file_idx,
                    op.file_idx
                );
                assert_eq!(
                    op.ptr
                        .as_ref()
                        .borrow()
                        .to_pointers
                        .get_prev_to_strong()
                        .unwrap()
                        .file_idx,
                    prv_ptr.as_ref().borrow().file_index
                );
            }
        }
    }

    fn check_log_indicies<F: RWS>(l: &mut Log<F>) {
        check_iter_total_order(l.op_total_order_iterator_from_last(), l.get_log_state()); // check we have the right pointers for the ops in the log
        check_iter_total_order(l.sp_total_order_iterator(), l.get_log_state()); // check we have the right pointers for the sps in log
                                                                                // check the log entry pointers
        for op in l.log_iterator_from_end() {
            if op.ptr.borrow().entry.is_op() {
                assert_eq!(
                    op.ptr.borrow().log_index,
                    op.ptr.borrow().entry.as_op().log_index
                );
            }
            if let Some(mut prv) = op.ptr.as_ref().borrow().get_prev() {
                assert_eq!(
                    prv.get_ptr(l.get_log_state())
                        .as_ref()
                        .borrow()
                        .get_next()
                        .unwrap()
                        .file_idx,
                    op.file_idx
                );
                assert_eq!(
                    op.ptr.as_ref().borrow().get_prev().unwrap().file_idx,
                    prv.get_ptr(l.get_log_state()).as_ref().borrow().file_index
                );
            }
        }
    }

    #[test]
    fn add_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(0));
        for _ in 0..5 {
            let _ = l.insert_op(Op::new(1, gen_rand_data(), &mut ti), &ti);
            check_log_indicies(&mut l);
        }
        let mut items = vec![];
        for op in l.log_iterator_from_end() {
            items.push(op);
        }
        assert_eq!(6, items.len()); // six since the first entry is the init SP
                                    // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn duplicate_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(1));
        let id = 1;
        let op1 = Op::new(id, gen_rand_data(), &mut ti);
        let op1_copy = op1.clone();
        l.insert_op(op1, &ti).unwrap();
        assert_eq!(
            LogError::OpAlreadyExists,
            l.insert_op(op1_copy, &ti).unwrap_err()
        );
        check_log_indicies(&mut l);
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn drop_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(2));
        let mut entry_infos = vec![l.get_initial_entry_info()];
        let op_count = 5;
        for _ in 0..op_count {
            let op = l
                .insert_op(Op::new(1, gen_rand_data(), &mut ti), &ti)
                .unwrap();
            check_log_indicies(&mut l);
            entry_infos.push(op.ptr.as_ref().borrow().entry.get_entry_info());
        }
        // drop initial sp and each op
        for _ in 0..=op_count {
            let entry = {
                let entry = l.drop_first().unwrap();
                Rc::downgrade(&entry.ptr)
            };
            assert!(entry.upgrade().is_none()) // ensure the entry has been dropped
        }

        // be sure we can load the items from disk
        for (i, (entry, entry_info)) in l
            .log_iterator_from_end()
            .zip(entry_infos.iter().rev())
            .enumerate()
        {
            let idx = op_count + 1 - i as u64;
            assert_eq!(idx, entry.ptr.as_ref().borrow().log_index);
            assert_eq!(
                entry_info,
                &entry.ptr.as_ref().borrow().entry.get_entry_info()
            )
        }
        // check we can still add ops
        l.insert_op(Op::new(1, gen_rand_data(), &mut ti), &ti)
            .unwrap();
        assert_eq!(
            op_count + 1,
            l.op_total_order_iterator_from_last().count() as u64
        );
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn op_iterator() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(3));
        let op1 = OpState::new(1, gen_rand_data(), &mut ti, l.serialize_option()).unwrap();
        let op2 = OpState::new(2, gen_rand_data(), &mut ti, l.serialize_option()).unwrap();
        let op3 = OpState::new(3, gen_rand_data(), &mut ti, l.serialize_option()).unwrap();
        let op1_clone = op1.clone();
        let op2_clone = op2.clone();
        let op3_clone = op3.clone();
        // insert the op in reserve order of creation
        let _ = l.insert_op(op2.op, &ti);
        check_log_indicies(&mut l);
        let _ = l.insert_op(op3.op, &ti);
        check_log_indicies(&mut l);
        let first_entry = l.insert_op(op1.op, &ti).unwrap();
        check_log_indicies(&mut l);
        {
            // when traversing in total order, we should see op2 first since it has a larger order
            let mut iter = l.op_total_order_iterator_from_last();
            assert_eq!(
                op3_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert_eq!(
                op2_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert_eq!(
                op1_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert!(iter.next().is_none());
            let mut iter = total_order_iterator(&(&first_entry).into(), true, &l.state);
            assert_eq!(
                op1_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert_eq!(
                op2_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert_eq!(
                op3_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert!(iter.next().is_none());

            // check using the total order iterator, but only after the index in the log
            let mut iter = total_order_after_iter(&first_entry, &l.state);
            assert_eq!(
                op1_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert_eq!(
                op2_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert_eq!(
                op3_clone,
                iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
            );
            assert!(iter.next().is_none());
        }
        // drop the first
        l.drop_first().unwrap(); // this is the first sp
        l.drop_first().unwrap(); // now the first op
                                 // be sure we can still traverse the remaining ops
        let mut iter = l.op_total_order_iterator_from_last();
        assert_eq!(
            op3_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
        );
        assert_eq!(
            op2_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
        );
        assert_eq!(
            op1_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_op().op
        );
        assert!(iter.next().is_none());
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn add_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(4));
        let num_sp = 5;
        let id = 1;
        // let mut prev_sp_info = l.get_initial_entry_info();
        let mut prev_sp = l.get_initial_sp().unwrap().ptr;
        for _ in 0..num_sp {
            let new_sp = l
                .insert_outer_sp(gen_sp(
                    id,
                    Rc::clone(&prev_sp),
                    &mut ti,
                    l.serialize_option(),
                ))
                .unwrap();
            check_log_indicies(&mut l);
            prev_sp = new_sp.ptr;
        }
        let mut items = vec![];
        for nxt in l.sp_total_order_iterator() {
            items.push(nxt);
        }
        assert_eq!(num_sp + 1, items.len());

        items = vec![];
        for nxt in l.sp_total_order_iterator() {
            items.push(nxt);
        }
        assert_eq!(num_sp + 1, items.len());

        items = vec![];
        for nxt in l.sp_local_total_order_iterator(id) {
            items.push(nxt);
        }
        assert_eq!(num_sp, items.len());

        assert!(l.sp_local_total_order_iterator(id + 1).next().is_none());
        // check the sp prev pointers
        check_sp_prev(&mut l, false);
    }

    #[test]
    fn drop_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(5));
        let mut entry_infos = vec![l.get_initial_entry_info()];
        let num_sp = 5;
        let id = 1;
        let prev_sp = {
            let mut prev_sp = l.get_initial_sp().unwrap().ptr;
            for _ in 0..num_sp {
                let new_sp = l
                    .insert_outer_sp(gen_sp(id, prev_sp, &mut ti, l.serialize_option()))
                    .unwrap();
                check_log_indicies(&mut l);
                prev_sp = Rc::clone(&new_sp.ptr);
                entry_infos.push(new_sp.ptr.as_ref().borrow().entry.get_entry_info());
            }
            // only keep the info for the SP so it is dropped
            let ei = prev_sp.borrow().entry.get_entry_info();
            ei
        };
        // be sure we can find all Sps
        assert_eq!(num_sp + 1, l.sp_total_order_iterator().count() as u64);

        // first drop the initial sp then the remaining sps
        for _ in 0..=num_sp {
            let entry = {
                let entry = l.drop_first().unwrap();
                Rc::downgrade(&entry.ptr)
            };
            debug!(
                "num disk entries {:?}, num log entires: {}",
                l.state.m.borrow().disk_entries_count(),
                l.state.m.borrow().recent_entries_count()
            );
            assert!(entry.upgrade().is_none()) // ensure the entry has been dropped
        }

        // be sure we can load the items from disk
        for (i, (entry, entry_info)) in l
            .log_iterator_from_end()
            .zip(entry_infos.iter().rev())
            .enumerate()
        {
            let idx = num_sp + 1 - i as u64;
            assert_eq!(idx, entry.ptr.as_ref().borrow().log_index);
            assert_eq!(
                entry_info,
                &entry.ptr.as_ref().borrow().entry.get_entry_info()
            )
        }
        // check we can still add sps
        let prev_sp = l.find_sp(prev_sp, None).unwrap();
        l.insert_outer_sp(gen_sp(id, prev_sp.ptr, &mut ti, l.serialize_option()))
            .unwrap();
        // be sure we have num_sp + 2 sps (+2 because of the inital log entry plus the additional one added)
        // assert_eq!(num_sp + 2, l.sp_iterator_from_last().count() as u64);
        assert_eq!(num_sp + 2, l.sp_total_order_iterator().count() as u64);
        // check the sp prev pointers
        check_sp_prev(&mut l, false);
    }

    #[test]
    fn sp_iterator() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(6));
        let mut last_sps: HashMap<Id, LogEntryWeak> = HashMap::new();
        let num_ids = 5;
        let num_sps = 5;

        for _ in 0..num_sps {
            for i in 0..num_ids {
                let prev_sp = get_entry_info(i, &mut l, &last_sps);
                let new_sp = l
                    .insert_outer_sp(gen_sp(i, prev_sp, &mut ti, l.serialize_option()))
                    .unwrap();
                last_sps.insert(i, (&new_sp).into());
            }
        }

        {
            // perform in scope so we drop refs
            let mut items = vec![];
            for nxt in l.sp_total_order_iterator() {
                items.push(nxt)
            }
            assert_eq!((num_ids * num_sps + 1) as usize, items.len());
        }

        for i in 0..num_ids {
            let mut items = vec![];
            for nxt in l.sp_local_total_order_iterator(i) {
                items.push(nxt)
            }
            assert_eq!(num_sps as usize, items.len())
        }

        // insert two in reverse order
        let iter_id = 0;
        let sp1 = gen_sp(
            iter_id,
            get_entry_info(iter_id, &mut l, &last_sps),
            &mut ti,
            l.serialize_option(),
        );
        let sp2 = gen_sp(
            iter_id,
            get_entry_info(iter_id, &mut l, &last_sps),
            &mut ti,
            l.serialize_option(),
        );
        let sp1_clone = sp1.sp.clone();
        let sp2_clone = sp2.sp.clone();
        l.insert_outer_sp(sp2).unwrap();
        check_log_indicies(&mut l);
        l.insert_outer_sp(sp1).unwrap();
        check_log_indicies(&mut l);

        // should be in insert order
        let mut iter = l.log_iterator_from_end();
        assert_eq!(
            sp1_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_sp().sp
        );
        assert_eq!(
            sp2_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_sp().sp
        );

        // total order should be in reverse insert order
        let mut iter = l.sp_total_order_iterator();
        assert_eq!(
            sp2_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_sp().sp
        );
        assert_eq!(
            sp1_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_sp().sp
        );

        // local total order should be in reverse insert order
        let mut iter = l.sp_local_total_order_iterator(iter_id);
        assert_eq!(
            sp2_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_sp().sp
        );
        assert_eq!(
            sp1_clone,
            iter.next().unwrap().ptr.as_ref().borrow().entry.as_sp().sp
        );
        // check the sp prev pointers
        check_sp_prev(&mut l, false);
    }

    fn gen_sp<O: Options + Copy>(id: u64, prev_sp: StrongPtr, ti: &mut TimeTest, o: O) -> OuterSp {
        let hsh = OpState::new(1, gen_rand_data(), ti, o).unwrap().hash;
        let sp = SpState::new(
            id,
            ti.now_monotonic(),
            [hsh].iter().cloned(),
            vec![],
            prev_sp.as_ref().borrow().entry.get_entry_info(),
            o,
        )
        .unwrap();
        ti.set_sp_time_valid(sp.sp.info.time);
        OuterSp {
            sp,
            not_included_ops: vec![],
            late_included: vec![],
            // prev_sp: None, /*Some(LogEntryWeak::from_file_idx(
            //            prev_sp.as_ref().borrow().file_index,
            //      )),*/
            last_op: None,
            supported_sp_log_index: Some(prev_sp.as_ref().borrow().log_index),
        }
    }

    #[test]
    fn verify_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(7));
        let num_ids = 5;
        let num_ops = 5;
        // insert some ops
        let ops: Vec<LogEntryWeak> = insert_ops(num_ops, num_ids, &mut l, &mut ti)
            .into_iter()
            .map(|op| (&op).into())
            .collect();
        let m = ops.iter().map(|op| {
            op.get_ptr(l.get_log_state())
                .as_ref()
                .borrow()
                .entry
                .as_op()
                .op
                .hash
        });
        let id = 0;
        // create an sp that supports the ops
        let sp1 = SpState::new(
            id,
            ti.now_monotonic(),
            m,
            vec![],
            l.get_initial_entry_info(),
            l.serialize_option(),
        )
        .unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        let sp1_info = sp1.get_entry_info();
        let (outer_sp1, sp1_ops) = l.check_sp(sp1.sp.clone(), &[], &[], &ti).unwrap();
        let sp1_ops: Vec<LogEntryWeak> = sp1_ops.into_iter().map(|op| (&op).into()).collect();
        l.insert_outer_sp(outer_sp1).unwrap();
        check_log_indicies(&mut l);
        for (ifo, op) in sp1_ops.iter().zip(ops.iter()) {
            assert_eq!(
                &op.get_ptr(l.get_log_state()).as_ref().borrow().entry,
                &ifo.get_ptr(l.get_log_state()).as_ref().borrow().entry
            );
        }
        // be sure we dont insert the duplicate
        assert_eq!(
            LogError::SpAlreadyExists,
            l.check_sp(sp1.sp, &[], &[], &ti).unwrap_err()
        );

        // create an Sp that supports the ops, but from a different owner
        let m = ops.iter().map(|op| {
            op.get_ptr(l.get_log_state())
                .as_ref()
                .borrow()
                .entry
                .as_op()
                .op
                .hash
        });
        let sp2 = Sp::new(
            id + 1,
            ti.now_monotonic(),
            m,
            vec![],
            l.get_initial_entry_info(),
        );
        ti.set_sp_time_valid(sp2.info.time);
        let (outer_sp2, sp2_ops) = l.check_sp(sp2, &[], &[], &ti).unwrap();
        let sp2_ops: Vec<LogEntryWeak> = sp2_ops.into_iter().map(|op| (&op).into()).collect();
        l.insert_outer_sp(outer_sp2).unwrap();
        check_log_indicies(&mut l);
        for (ifo, op) in sp2_ops.iter().zip(ops.iter()) {
            assert_eq!(
                op.get_ptr(l.get_log_state()).borrow().entry,
                ifo.get_ptr(l.get_log_state()).borrow().entry
            );
        }

        let sp3 = {
            // add some more ops
            let ops = insert_ops(num_ops, num_ids, &mut l, &mut ti);
            let m = ops
                .iter()
                .map(|op| op.ptr.as_ref().borrow().entry.as_op().op.hash);
            // create an Sp that supports just the new ops
            Sp::new(id, ti.now_monotonic(), m, vec![], sp1_info)
        };
        ti.set_sp_time_valid(sp3.info.time);
        let _outer_sp3 = l.check_sp(sp3, &[], &[], &ti).unwrap();
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn late_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(8));
        let id = 0;
        let op1 = make_op_late(
            OpState::new(id, gen_rand_data(), &mut ti, l.serialize_option()).unwrap(),
            l.serialize_option(),
        );
        let outer_op1 = l.insert_op(op1.op.clone(), &ti).unwrap();
        check_log_indicies(&mut l);
        assert!(outer_op1.ptr.as_ref().borrow().entry.as_op().arrived_late);

        // op2 will be included as normal op
        let op2 = OpState::new(id, gen_rand_data(), &mut ti, l.serialize_option()).unwrap();
        let outer_op2 = l.insert_op(op2.op, &ti).unwrap();
        check_log_indicies(&mut l);
        assert!(!outer_op2.ptr.as_ref().borrow().entry.as_op().arrived_late);

        let sp1 = Sp::new(
            id,
            ti.now_monotonic(),
            [op1.hash].iter().cloned(),
            vec![],
            l.get_initial_entry_info(),
        );
        ti.set_sp_time_valid(sp1.info.time);
        // op3 has a later time so should not be included
        let op3 = Op::new(id, gen_rand_data(), &mut ti);
        check_log_indicies(&mut l);
        l.insert_op(op3, &ti).unwrap();

        // when input op2 will be used, so it will create a hash error
        assert_eq!(
            LogError::SpHashNotComputed,
            l.check_sp(sp1, &[], &[], &ti).unwrap_err()
        );
        // use both sp1 and sp2, using included input to check_sp for sp1
        let hash_vec = vec![op1.hash, op2.hash];
        let sp1 = Sp::new(
            id,
            ti.now_monotonic(),
            hash_vec.iter().cloned(),
            vec![],
            l.get_initial_entry_info(),
        );
        ti.set_sp_time_valid(sp1.info.time);

        l.check_sp(sp1, &[(&op1).into()], &[], &ti).unwrap();

        // use both sp1 and sp2, using additional_ops input with to include sp1
        let op1_data = EntryInfoData {
            info: (&op1).into(),
            data: get_empty_data(),
        };
        let sp1 = Sp::new(
            id,
            ti.now_monotonic(),
            hash_vec.iter().cloned(),
            vec![op1_data],
            l.get_initial_entry_info(),
        );
        ti.set_sp_time_valid(sp1.info.time);
        l.check_sp(sp1, &[], &[], &ti).unwrap();
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn unsupported_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(9));
        let num_ids = 5;
        let num_ops = 6;
        let ops = insert_ops(num_ops, num_ids, &mut l, &mut ti);
        // the not included ops are the odd ones
        let not_included: Vec<_> = ops
            .iter()
            .filter_map(|op_rc| {
                let op_ref = op_rc.ptr.as_ref().borrow();
                let op = op_ref.entry.as_op();
                if op.log_index % 2 == 0 {
                    // dont include the even ones
                    return Some((&op.op).into());
                }
                None
            })
            .collect();
        // let not_included_count = not_included.len();
        // the included ops are the even ones
        let m: Vec<_> = ops
            .iter()
            .filter_map(|op_rc| {
                let op_ref = op_rc.ptr.as_ref().borrow();
                let op = op_ref.entry.as_op();
                if op.log_index % 2 != 0 {
                    // only take the odd ones
                    return Some(op.op.hash);
                }
                None
            })
            .collect();
        let m_clone: Vec<_> = m.clone();
        // drop ops so we dont have an extra reference that prevents GC
        drop(ops);

        let id = 0;
        let sp1 = SpState::new(
            id,
            ti.now_monotonic(),
            m.into_iter(),
            vec![],
            l.get_initial_entry_info(),
            l.serialize_option(),
        )
        .unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        let sp1_info = sp1.get_entry_info();
        let (outer_sp1, sp1_ops) = l.check_sp(sp1.sp, &[], &not_included, &ti).unwrap();
        let sp1_ops: Vec<LogEntryWeak> = sp1_ops.into_iter().map(|op| (&op).into()).collect();
        // be sure there are unsupported ops
        for (entry, ifo) in outer_sp1.not_included_ops.iter().zip(not_included.iter()) {
            assert_eq!(
                ifo,
                &entry.get_ptr(&l.state).borrow().entry.get_entry_info()
            );
        }
        assert_eq!(not_included.len(), outer_sp1.not_included_ops.len());
        l.insert_outer_sp(outer_sp1).unwrap();
        check_log_indicies(&mut l);
        assert_eq!(m_clone.len(), sp1_ops.len());
        for (ifo, hsh) in sp1_ops.iter().zip(m_clone) {
            assert_eq!(
                hsh,
                ifo.get_ptr(l.get_log_state()).borrow().entry.get_hash()
            );
        }

        // make an sp with the other items
        let sp2 = Sp::new(
            id,
            ti.now_monotonic(),
            not_included.iter().map(|nxt| nxt.hash),
            vec![],
            sp1_info,
        );
        ti.set_sp_time_valid(sp2.info.time);
        let (outer_sp2, sp2_ops) = l.check_sp(sp2, &[], &[], &ti).unwrap();
        let sp2_ops: Vec<LogEntryWeak> = sp2_ops.into_iter().map(|op| (&op).into()).collect();
        assert_eq!(0, outer_sp2.not_included_ops.len());
        l.insert_outer_sp(outer_sp2).unwrap();
        check_log_indicies(&mut l);
        assert_eq!(not_included.len(), sp2_ops.len());
        for (ifo, op) in sp2_ops.iter().zip(not_included.iter()) {
            assert_eq!(op, &ifo.get_ptr(&l.state).borrow().entry.get_entry_info());
        }
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn no_op_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(10));
        let id = 0;
        let op1 = OpState::new(id, gen_rand_data(), &mut ti, l.serialize_option()).unwrap();
        let sp1 = SpState::new(
            id,
            ti.now_monotonic(),
            [op1.hash].iter().cloned(),
            vec![],
            l.get_initial_entry_info(),
            l.serialize_option(),
        )
        .unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        assert_eq!(
            LogError::PrevSpHasNoLastOp,
            l.check_sp(sp1.sp, &[], &[], &ti).unwrap_err()
        );
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn exact_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(11));
        let id = 0;
        let op1 = OpState::new(id, gen_rand_data(), &mut ti, l.serialize_option()).unwrap();
        let op2 = make_op_late(
            OpState::new(id, gen_rand_data(), &mut ti, l.serialize_option()).unwrap(),
            l.serialize_option(),
        );
        l.insert_op(op2.op.clone(), &ti).unwrap();
        check_log_indicies(&mut l);
        l.insert_op(op1.op.clone(), &ti).unwrap();
        check_log_indicies(&mut l);
        let sp1 = SpState::new(
            id,
            ti.now_monotonic(),
            [op1.hash].iter().cloned(),
            vec![],
            l.get_initial_entry_info(),
            l.serialize_option(),
        )
        .unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        let sp1_info = sp1.get_entry_info();
        let (outer_sp1, sp1_ops) = l.check_sp(sp1.sp, &[], &[], &ti).unwrap();
        assert_eq!(1, outer_sp1.not_included_ops.len());
        l.insert_outer_sp(outer_sp1).unwrap();
        check_log_indicies(&mut l);
        for (ifo, op) in sp1_ops.iter().zip([op1].iter()) {
            assert_eq!(op, &ifo.ptr.borrow().entry.as_op().op);
        }

        let sp2 = Sp::new(
            id,
            ti.now_monotonic(),
            [op2.hash].iter().cloned(),
            vec![],
            sp1_info,
        );
        ti.set_sp_time_valid(sp2.info.time);
        let (outer_sp2, sp2_ops) = l.check_sp_exact(sp2, &[(&op2).into()], &ti).unwrap();
        assert_eq!(0, outer_sp2.not_included_ops.len());
        l.insert_outer_sp(outer_sp2).unwrap();
        check_log_indicies(&mut l);
        for (ifo, op) in sp2_ops.iter().zip([op2].iter()) {
            assert_eq!(&ifo.ptr.borrow().entry.as_op().op, op);
        }
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    fn insert_ops<F: RWS>(
        num_ops: usize,
        num_ids: Id,
        l: &mut Log<F>,
        ti: &mut TimeTest,
    ) -> Vec<StrongPtrIdx> {
        let mut ret = vec![];
        for _ in 0..num_ops {
            for i in 0..num_ids {
                ret.push(l.insert_op(Op::new(i, gen_rand_data(), ti), ti).unwrap());
                check_log_indicies(l);
            }
        }
        ret
    }

    fn get_entry_info<F: RWS>(
        id: Id,
        l: &mut Log<F>,
        last_sps: &HashMap<Id, LogEntryWeak>,
    ) -> StrongPtr {
        match last_sps.get(&id) {
            Some(ei) => ei.get_ptr(l.get_log_state()),
            None => l.get_initial_sp().unwrap().ptr,
        }
    }

    #[test]
    fn drop_op_max() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(12));
        let mut entry_infos = vec![l.get_initial_entry_info()];
        let mut entries = VecDeque::new();
        let op_count = DEFAULT_MAX_ENTRIES + 10;
        for i in 0..op_count {
            let op = l
                .insert_op(Op::new(1, gen_rand_data(), &mut ti), &ti)
                .unwrap();
            // we should have no more than the max entires in memory
            assert_eq!(
                cmp::min(i + 2, DEFAULT_MAX_ENTRIES),
                l.get_log_state().m.borrow_mut().recent_entries_count() as usize
            );
            entry_infos.push(op.ptr.as_ref().borrow().entry.get_entry_info());
            entries.push_back(op);
            if entries.len() > DEFAULT_MAX_ENTRIES {
                // be sure we have dropped the earlier entry
                let first = Rc::downgrade(&entries.pop_front().unwrap().ptr);
                assert!(first.upgrade().is_none());
            }
        }
        // be sure we can load the items from disk
        for (i, (entry, entry_info)) in l
            .log_iterator_from_end()
            .zip(entry_infos.iter().rev())
            .enumerate()
        {
            let idx = (op_count + 1 - i) as u64;
            assert_eq!(idx, entry.ptr.as_ref().borrow().log_index);
            assert_eq!(
                entry_info,
                &entry.ptr.as_ref().borrow().entry.get_entry_info()
            )
        }
        // check we can still add ops
        l.insert_op(Op::new(1, gen_rand_data(), &mut ti), &ti)
            .unwrap();
        assert_eq!(
            (op_count + 1) as u64,
            l.op_total_order_iterator_from_last().count() as u64
        );
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }

    #[test]
    fn load_drop_disk() {
        let mut ti = TimeTest::new();
        let mut l = Log::new(get_log_file(13));
        let mut entry_infos = vec![l.get_initial_entry_info()];
        let op_count = DEFAULT_MAX_ENTRIES * 4;
        for _ in 0..op_count {
            let op = l
                .insert_op(Op::new(1, gen_rand_data(), &mut ti), &ti)
                .unwrap();
            entry_infos.push(op.ptr.as_ref().borrow().entry.get_entry_info());
        }
        let mut items = VecDeque::new();
        // be sure we can load the items from disk
        for (i, (entry, entry_info)) in l
            .log_iterator_from_end()
            .zip(entry_infos.iter().rev())
            .enumerate()
        {
            let idx = (op_count + 1 - i) as u64;
            assert_eq!(idx, entry.ptr.as_ref().borrow().log_index);
            assert_eq!(
                entry_info,
                &entry.ptr.as_ref().borrow().entry.get_entry_info()
            );
            if i >= DEFAULT_MAX_ENTRIES {
                // after default max entires, we will start loading items from disk
                items.push_back(entry);
                if items.len() > DEFAULT_DISK_ENTRIES {
                    // be sure we old entries have been cleared from disk
                    let entry = Rc::downgrade(&items.pop_front().unwrap().ptr);
                    assert!(entry.upgrade().is_none());
                }
            }
        }
        // check the sp prev pointers
        check_sp_prev(&mut l, true);
    }
}
