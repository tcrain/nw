use std::{cmp::Ordering, rc::{Rc}};
use std::cell::{RefCell, Ref};
use std::ops::Deref;
use verification::{TimeCheck, TimeInfo, hash};

use crate::{errors::{Error, LogError}, verification::{self, Id}};

use super::{entry::{LogEntry, LogEntryStrong, LogEntryWeak, LogIterator, LogPointers, OuterOp, OuterSp, PrevEntry, TotalOrderIterator, TotalOrderPointers, drop_entry, prev_entry_strong_to_weak, set_next_sp, set_next_total_order}, op::{BasicInfo, EntryInfo, Op, OpState}, sp::SpState};
use super::sp::Sp;

// TODO: check equal when inserting, then don't insert

fn to_prev_entry_holder(prv: &LogEntryStrong) -> PrevEntryHolder {
    PrevEntryHolder{prv_entry: (&**prv).borrow()}
}

struct PrevEntryHolder<'a> {
    prv_entry: Ref<'a, LogEntry>,
}

impl<'a> Deref for PrevEntryHolder<'a> {
    type Target = PrevEntry;
    
    fn deref(&self) -> &PrevEntry {
        &self.prv_entry.entry
    }
}

struct SpIterator {
    prev_entry: Option<LogEntryStrong>,
}

impl Iterator for SpIterator {
    type Item = LogEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prev_entry.as_ref().map(|prv| Rc::clone(prv));
        self.prev_entry = match self.prev_entry.take() {
            None => None,
            Some(prv) => prv.borrow().entry.as_sp().get_prev_sp()
        };
        ret
    }
}


struct SpLocalTotalOrderIterator {
    id: Id,
    iter: TotalOrderIterator,
}

impl Iterator for SpLocalTotalOrderIterator {
    type Item = LogEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(nxt) = self.iter.next() {
            let nxt_ret = nxt.borrow();
            let sp = &nxt_ret.entry.as_sp().sp;
            if sp.sp.info.id == self.id && !sp.sp.is_init() {
                return Some(Rc::clone(&nxt))
            }
        }
        None
    }
}

enum LogItems {
    Empty,
    Single(LogSingle),
    Multiple(LogMultiple)
}

impl LogItems {
    
    // drops the first item in the log and returns the item dropped
    fn drop_first(&mut self) -> Result<LogEntryStrong, Error> {
        match self {
            LogItems::Empty => Result::Err(Error::LogError(LogError::EmptyLogError)),
            LogItems::Single(si) => {
                let prv = Rc::clone(&si.entry);
                *self = LogItems::Empty;
                Ok(prv)
            },
            LogItems::Multiple(mi) => {
                let nxt = (&*mi.first_entry).borrow().get_next().unwrap();
                let ret = Rc::clone(&mi.first_entry);
                drop_entry(&mi.first_entry);
                // &nxt.borrow_mut().drop_first();
                if (&*mi.last_entry).borrow().get_prev().is_none() {
                    *self = LogItems::Single(LogSingle{entry: nxt})
                } else {
                    mi.first_entry = nxt;
                }
                Ok(ret)
            }
        }
    }
    
    fn log_iterator(&self) -> LogIterator {
        LogIterator {
            prv_entry: match self {
                LogItems::Empty => None,
                LogItems::Single(si) => Some(Rc::clone( &si.entry)),
                LogItems::Multiple(mi) => Some(Rc::clone( &mi.last_entry))
            }
        }
    }
    
    fn add_item(&mut self, idx: u64, entry: PrevEntry) -> LogEntryStrong {
        let prv = match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(Rc::clone(&mi.last_entry))
        };
        let log_entry = LogEntry{
            log_index: idx,
            log_pointers: LogPointers {
                next_entry: None,
                prev_entry: prv,
            },
            to_pointers: TotalOrderPointers{
                next_to: None,
                prev_to: None,
            },
            entry,
        };
        let new_entry = Rc::new(RefCell::new(log_entry));
        let new_entry_ref = Rc::clone(&new_entry);
        match self {
            LogItems::Empty => {
                *self = LogItems::Single(LogSingle{
                    entry: new_entry_ref
                })
            }
            LogItems::Single(si) => {
                si.entry.borrow_mut().set_next(Some(Rc::downgrade(&new_entry_ref)));
                let first = Rc::clone(&si.entry);
                *self = LogItems::Multiple(LogMultiple{
                    last_entry: new_entry_ref,
                    first_entry: first,
                })
            }
            LogItems::Multiple(lm) => {
                lm.last_entry.borrow_mut().set_next(Some(Rc::downgrade(&new_entry_ref)));
                lm.last_entry = new_entry_ref
            }
        };
        new_entry
    }
    
    fn get_last(&self) -> Option<LogEntryStrong> {
        match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(Rc::clone(&mi.last_entry)),
        }
    }
}

struct LogSingle {
    entry: LogEntryStrong,
}

struct LogMultiple {
    last_entry: LogEntryStrong,
    first_entry: LogEntryStrong
}

pub struct Log {
    index: u64,
    first_last: LogItems,
    last_op_total_order: Option<LogEntryWeak>,
    last_sp: Option<LogEntryWeak>,
    last_sp_total_order: Option<LogEntryWeak>,
    init_hash: verification::Hash,
    init_sp: Option<LogEntryWeak>,
    set_initial_sp_op: bool,
    init_sp_entry_info: Option<EntryInfo>,
}

impl Log {
    pub fn new() -> Log {
        let mut l = Log{
            index: 0,
            first_last: LogItems::Empty,
            last_op_total_order: None,
            last_sp: None,
            last_sp_total_order: None,
            init_hash: hash(b"initial state"), // TODO should allow this as input?
            init_sp: None,
            set_initial_sp_op: false,
            init_sp_entry_info: None,
        };
        // insert the initial sp
        let init_sp = OuterSp{
            sp: SpState::from_sp(Sp::get_init(l.get_init_support())).unwrap(),
            last_op: None,
            not_included_ops: vec![],
            prev_sp: None,    
        };
        l.init_sp_entry_info = Some(init_sp.sp.get_entry_info());
        l.init_sp = Some(Rc::downgrade(&l.insert_outer_sp(init_sp)));
        l
    }

    fn get_init_support(&self) -> EntryInfo {
        EntryInfo{
            basic: BasicInfo{
                time: 0,
                id: 0,
            },
            hash: self.init_hash,
        }
    }
    
    pub fn get_initial_entry_info(&self) -> EntryInfo {
        /*EntryInfo{
            basic: BasicInfo{
                time: 0,
                id: 0,
            },
            hash: self.init_hash,
        }*/
        self.init_sp_entry_info.unwrap()
    }
    
    pub fn get_last_sp_id(&self, id: Id) -> Result<LogEntryStrong, Error> {
        for nxt_sp in self.sp_total_order_iterator() {
            if nxt_sp.borrow().entry.as_sp().sp.sp.info.id == id && !nxt_sp.borrow().entry.as_sp().sp.sp.is_init() {
                return Ok(nxt_sp)
            }
        }
        Err(Error::LogError(LogError::IdHasNoSp))
    }
    
    pub fn get_initial_sp(&self) -> Result<LogEntryStrong, Error> {
        self.find_sp(self.get_initial_entry_info())
    }
    
    fn find_sp(&self, to_find: EntryInfo) -> Result<LogEntryStrong, Error> {
        for nxt_sp in self.sp_total_order_iterator() {
            match nxt_sp.borrow().entry.as_sp().sp.get_entry_info().cmp(&to_find) {
                Ordering::Less => break,
                Ordering::Equal => return Ok(Rc::clone(&nxt_sp)),
                Ordering::Greater => (),
            }
        }
        Err(Error::LogError(LogError::PrevSpNotFound))
    }
    
    fn drop_first(&mut self) -> Result<LogEntryStrong, Error> {
        match self.first_last.drop_first() {
            Err(e) => Err(e),
            Ok(first) => {
                // if the dropped value was the last op, then the last op/sp pointer must be updated
                // (this is needed because the total order of ops/sps may be in a differnt order than the log)
                match first.borrow().entry {
                    PrevEntry::Op(_) => {
                        let last_op = self.last_op_total_order.as_ref().unwrap().upgrade().unwrap();
                        if Rc::ptr_eq(&last_op, &first) {
                            self.last_op_total_order = prev_entry_strong_to_weak(last_op.borrow().to_pointers.get_prev_to());
                        }
                    },
                    PrevEntry::Sp(_) => {
                        let last_sp = self.last_sp_total_order.as_ref().unwrap().upgrade().unwrap();
                        if Rc::ptr_eq(&last_sp, &first) {
                            self.last_sp_total_order = prev_entry_strong_to_weak(last_sp.borrow().to_pointers.get_prev_to());
                        }
                    }             
                }
                Ok(first)
            }
        }
    }
    
    fn sp_local_total_order_iterator(&self, id: Id) -> SpLocalTotalOrderIterator {
        SpLocalTotalOrderIterator{
            id,
            iter: self.sp_total_order_iterator(),
        }
    }
    
    fn sp_total_order_iterator(&self) -> TotalOrderIterator {
        TotalOrderIterator{
            prev_entry: match &self.last_sp_total_order {
                None => None,
                Some(prv) => prv.upgrade()
            },
            forward: false,
        }
    }
    
    fn sp_iterator_from_last(&self) -> SpIterator {
        SpIterator{
            prev_entry: match &self.last_sp {
                None => None,
                Some(prv) => prv.upgrade()
            }
        }
    }
    
    fn op_total_order_iterator_from_last(&self) -> TotalOrderIterator {
        TotalOrderIterator{
            prev_entry: match &self.last_op_total_order {
                None => None,
                Some(prv) => prv.upgrade()
            },
            forward: false,
        }
    }
    
    fn log_iterator_from_end(&self) -> LogIterator {
        self.first_last.log_iterator()
    }
    
    pub fn check_sp<T>(&self, sp: Sp, included: &[EntryInfo], not_included: &[EntryInfo], ti: &T) -> Result<OuterSp, Error>
    where T: TimeInfo {
        sp.validate(&self.init_sp_entry_info.unwrap().hash, ti)?;
        let prev_sp = self.find_sp(sp.prev_sp)?;
        let prev_sp_ref = prev_sp.borrow();
        prev_sp_ref.entry.as_sp().check_sp_log_order(sp, included, not_included)
    }
    
    pub fn check_sp_exact<T>(&self, sp: Sp, exact: &[EntryInfo], ti: &T) -> Result<OuterSp, Error>
    where T: TimeInfo {
        sp.validate(&self.init_sp_entry_info.unwrap().hash, ti)?;
        let prev_sp = self.find_sp(sp.prev_sp)?;
        let prev_sp_ref = prev_sp.borrow();
        prev_sp_ref.entry.as_sp().check_sp_exact(sp, exact)
    }
    
    pub fn insert_outer_sp(&mut self, sp: OuterSp) -> LogEntryStrong {
        self.index += 1;
        let entry = PrevEntry::Sp(sp);
        // add the sp to the log
        let new_sp_ref = self.first_last.add_item(self.index, entry);
        // put the new sp in the total ordered list of sp
        let mut last = None;
        for nxt in self.sp_total_order_iterator() {
            if nxt.borrow().entry.as_sp().sp < new_sp_ref.borrow().entry.as_sp().sp {
                set_next_total_order(&nxt, &new_sp_ref);
                last = None;
                break
            } else {
                last = Some(nxt);
            }
        }
        // if last != None then we are at the begginning of the list, so we must update the new sp next pointer
        if let Some(last_sp) = last {
            set_next_total_order(&new_sp_ref, &last_sp)
        }
        
        // update the last sp total order pointer if necessary
        self.last_sp_total_order = match self.last_sp_total_order.take() {
            None => Some(Rc::downgrade(&new_sp_ref)),
            Some(weak_last) => {
                weak_last.upgrade().map(| last| {
                    if last.borrow().entry.as_sp().sp < new_sp_ref.borrow().entry.as_sp().sp {
                        Rc::downgrade(&new_sp_ref)
                    } else {
                        weak_last
                    }
                })
            }
        };
        // update the sp log order pointer
        if let Some(last_sp) = self.last_sp.as_ref().and_then(|sp| sp.upgrade()) {
            set_next_sp(&last_sp, None, &new_sp_ref);
        }
        self.last_sp = Some(Rc::downgrade(&new_sp_ref));
        Rc::clone(&new_sp_ref)
    }
    
    pub fn new_op<T>(&mut self, op: Op, ti: &T) -> Result<LogEntryStrong, Error> where T: TimeInfo {
        self.index += 1;
        let TimeCheck {time_not_passed: _ , include_in_hash, arrived_late} = ti.arrived_time_check(op.info.time);
        let op_state = OpState::from_op(op)?;
        let outer_op = PrevEntry::Op(OuterOp{
            op: op_state,
            log_index: self.index,
            include_in_hash,
            arrived_late,
        });
        // compute the location of the operation in the total order of the log
        // if the operations is before the first operation in the log, first_op will not be none
        let mut first_op = None;
        // otherwise we compute the operation prior to the new one in the list
        let mut prev = None;
        for nxt in self.op_total_order_iterator_from_last() {
            match nxt.borrow().entry.as_op().op.cmp(&op_state) {
                Ordering::Less => { // we found where the op should be inserted
                    first_op = None;
                    prev = Some(Rc::clone(&nxt));
                    break
                }
                Ordering::Equal => return Err(Error::LogError(LogError::OpAlreadyExists)), // the op already found in the list
                Ordering::Greater => first_op = Some(Rc::clone(&nxt)), // keep looking
            }
        };
        if first_op.is_some() && prev.is_some() { // cannot be at the beginning of the list and have a previous entry
            panic!(format!("problem finding the operation in the total order list, first_op {}, prev {}", first_op.is_none(), prev.is_none()))
        }
        // add the new op to the end of the log
        let new_op_ref = self.first_last.add_item(self.index, outer_op);
        // add the op to the total order of the log
        if let Some(prv) = prev {
            set_next_total_order(&prv, &new_op_ref);
        }
        
        // see if need to put the op on the intial list
        if !self.set_initial_sp_op {
            self.set_initial_sp_op = true;
            if let Some(sp) = self.init_sp.as_ref().and_then(|sp| sp.upgrade()) {
                sp.borrow_mut().entry.mut_as_sp().last_op = Some(Rc::downgrade(&new_op_ref));
            }
        }
        // if last != None then we are at the begginning of the list, so we must update the new op next pointer
        if let Some(last_op) = first_op {
            set_next_total_order(&new_op_ref, &last_op)
        }
        
        // update the last op pointer if necessary
        self.last_op_total_order = match self.last_op_total_order.take() {
            None => Some(Rc::downgrade(&new_op_ref)),
            Some(weak_last) => {
                weak_last.upgrade().map(| last| {
                    if last.borrow().entry.as_op().op < new_op_ref.borrow().entry.as_op().op {
                        Rc::downgrade(&new_op_ref)
                    } else {
                        weak_last
                    }
                })
            }
        };
        Ok(Rc::clone(&new_op_ref))
    }
    
    /*     fn insert_sp<'a>(&mut self, sp: SP) -> Result<&'a OuterSP, Error> {
        self.index += 1;
        match self.prev_entry.borrow().take() {
            None => None,
            Some(prv) => Some(Rc::clone(&prv))
        };
        let outer_sp = Rc::new(Some(PrevEntry::SP(OuterSP{
            sp: SP,
            log_index: self.index,
            not_included_op: vec![],
            prev_local: RefCell::new(Weak::new()), // RefCell<Weak<OuterSP>>,
            prev_to: RefCell::new(Weak::new()), // RefCell<Weak<OuterSP>>,
            prev_entry: RefCell::new(Rc::clone(&self.prev_entry.borrow())), // RefCell<Rc<Option<PrevEntry>>>,
        })));
        *self.prev_entry.borrow_mut() = Rc::clone(&outer_sp);
        let PrevEntry::SP(ret) = Rc::clone(&outer_sp).unwrap();
        Ok(&ret)
    }*/
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::{log::{op::tests::make_op_late}, verification::TimeTest};
    
    use crate::{log::{entry::{total_order_after_iterator, total_order_iterator}, op::{EntryInfo, EntryInfoData, get_empty_data}}};
    
    use super::*;
    
    #[test]
    fn add_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        for _ in 0..5 {
            let _ = l.new_op(Op::new(1, &mut ti), &ti);
        }
        let mut items = vec![];
        for op in l.log_iterator_from_end() {
            items.push(op);
        }
        assert_eq!(6, items.len()) // six since the first entry is the init SP
    }
    
    #[test]
    fn duplicate_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let id = 1;
        let op1 = Op::new(id, &mut ti);
        let op1_copy = op1;
        l.new_op(op1, &ti).unwrap();
        assert_eq!(Error::LogError(LogError::OpAlreadyExists), l.new_op(op1_copy, &ti).unwrap_err());
    }
    
    #[test]
    fn drop_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        for _ in 0..5 {
            let _ = l.new_op(Op::new(1, &mut ti), &ti);
        }
        for _ in 0..6 {
            l.drop_first().unwrap();
        }
        assert!(l.log_iterator_from_end().next().is_none());
        let err = l.drop_first().unwrap_err();
        assert_eq!(Error::LogError(LogError::EmptyLogError), err); 
        for _ in 0..5 {
            let _ = l.new_op(Op::new(2, &mut ti), &ti);
        }
    }
    
    #[test]
    fn op_iterator() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let op1 = OpState::new(1, &mut ti).unwrap();
        let op2 = OpState::new(2, &mut ti).unwrap();
        let op3 = OpState::new(3, &mut ti).unwrap();
        let op1_clone = op1;
        let op2_clone = op2;
        let op3_clone = op3;
        // insert the op in reserve order of creation
        let _ = l.new_op(op2.op, &ti);
        let _ = l.new_op(op3.op, &ti);
        let first_entry = l.new_op(op1.op, &ti).unwrap();
        // when traversing in total order, we should see op2 first since it has a larger order
        let mut iter = l.op_total_order_iterator_from_last();
        assert_eq!(op3_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op2_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert!(iter.next().is_none());
        let mut iter = total_order_iterator(Some(&first_entry), true);
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op2_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op3_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert!(iter.next().is_none());
        
        // check using the total order iterator, but only after the index in the log
        let mut after_iter = total_order_after_iterator(Some(&first_entry));
        assert_eq!(op1_clone, after_iter.next().unwrap().borrow().entry.as_op().op);
        assert!(after_iter.next().is_none()); // no values since this is the last item in the log
        
        // drop the first
        l.drop_first().unwrap(); // this is the first sp
        l.drop_first().unwrap(); // now the first op
        // be sure we can still traverse the remaining ops
        iter = l.op_total_order_iterator_from_last();
        assert_eq!(op3_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert!(iter.next().is_none());
        
        l.drop_first().unwrap();
        // be sure the pointer to the op was updated since we have a new last op in the total order
        iter = l.op_total_order_iterator_from_last();
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert!(iter.next().is_none())
    }
    
    #[test]
    fn add_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let num_sp = 5;
        let id = 1;
        let mut prev_sp_info = l.get_initial_entry_info();
        for _ in 0..num_sp {
            let new_sp = l.insert_outer_sp(gen_sp(id, prev_sp_info, &mut ti));
            prev_sp_info = new_sp.borrow().entry.as_sp().sp.get_entry_info();
        }
        let mut items = vec![];
        for nxt in l.sp_iterator_from_last() {
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
        
        assert!(l.sp_local_total_order_iterator(id+1).next().is_none());
    }
    
    #[test]
    fn drop_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let num_sp = 5;
        let id = 1;
        let mut prev_sp_info = l.get_initial_entry_info();
        for _ in 0..num_sp {
            let new_sp = l.insert_outer_sp(gen_sp(id, prev_sp_info, &mut ti));
            prev_sp_info = new_sp.borrow().entry.as_sp().sp.get_entry_info();
        }
        // first drop the initial sp
        l.drop_first().unwrap();
        for _ in 0..num_sp {
            l.drop_first().unwrap();
        }
        assert!(l.log_iterator_from_end().next().is_none());
        let err = l.drop_first().unwrap_err();
        assert_eq!(Error::LogError(LogError::EmptyLogError), err);
        
        prev_sp_info = l.get_initial_entry_info();
        for _ in 0..num_sp {
            let new_sp = l.insert_outer_sp(gen_sp(id, prev_sp_info, &mut ti));
            prev_sp_info = new_sp.borrow().entry.as_sp().sp.get_entry_info();
        }
    }
    
    #[test]
    fn sp_iterator() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let mut last_sps: HashMap<Id, EntryInfo> = HashMap::new();
        let num_ids = 5;
        let num_sps = 5;
        
        for _ in 0..num_sps {
            for i in 0..num_ids {
                let new_sp = l.insert_outer_sp(gen_sp(i, get_entry_info(i, &l, &last_sps), &mut ti));
                last_sps.insert(i, new_sp.borrow().entry.as_sp().sp.get_entry_info());    
            }
        }
        
        let mut items = vec![];
        for nxt in l.sp_iterator_from_last() {
            items.push(nxt)
        }
        assert_eq!((num_ids * num_sps + 1) as usize, items.len());
        
        for i in 0..num_ids {
            let mut items = vec![];
            for nxt in l.sp_local_total_order_iterator(i) {
                items.push(nxt)
            }
            assert_eq!(num_sps as usize, items.len())
        }
        
        // insert two in reverse order
        let iter_id = 0;
        let sp1 = gen_sp(iter_id, get_entry_info(iter_id, &l, &last_sps), &mut ti);
        let sp2 = gen_sp(iter_id, get_entry_info(iter_id, &l, &last_sps), &mut ti);
        let sp1_clone = sp1.sp.clone();
        let sp2_clone = sp2.sp.clone();
        l.insert_outer_sp(sp2);
        l.insert_outer_sp(sp1);
        
        // should be in insert order
        let mut iter = l.sp_iterator_from_last();
        assert_eq!(sp1_clone, iter.next().unwrap().borrow().entry.as_sp().sp);
        assert_eq!(sp2_clone, iter.next().unwrap().borrow().entry.as_sp().sp);
        
        // total order should be in reverse insert order
        let mut iter = l.sp_total_order_iterator();
        assert_eq!(sp2_clone, iter.next().unwrap().borrow().entry.as_sp().sp);
        assert_eq!(sp1_clone, iter.next().unwrap().borrow().entry.as_sp().sp);
        
        
        // local total order should be in reverse insert order
        let mut iter = l.sp_local_total_order_iterator(iter_id);
        assert_eq!(sp2_clone, iter.next().unwrap().borrow().entry.as_sp().sp);
        assert_eq!(sp1_clone, iter.next().unwrap().borrow().entry.as_sp().sp);        
    }
    
    fn gen_sp(id: u64, prev_sp: EntryInfo, ti: &mut TimeTest) -> OuterSp {
        let hsh = OpState::new(1, ti).unwrap().hash;
        let sp = SpState::new(id, ti.now_monotonic(), [hsh].iter().cloned(), vec![], prev_sp).unwrap();
        ti.set_sp_time_valid(sp.sp.info.time);
        OuterSp{
            sp,
            not_included_ops: vec![],
            prev_sp: None,
            last_op: None,
        }
    }
    
    #[test]
    fn verify_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let num_ids = 5;
        let num_ops  = 5;
        let ops = insert_ops(num_ops, num_ids, &mut l, &mut ti);
        let m = ops.iter().map(|op| {
            op.borrow().entry.as_op().op.hash
        });
        let id = 0;
        let sp1 = SpState::new(id, ti.now_monotonic(),m, vec![], l.get_initial_entry_info()).unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        let sp1_info = sp1.get_entry_info();
        let outer_sp1 = l.check_sp(sp1.sp, &[], &[], &ti).unwrap();
        l.insert_outer_sp(outer_sp1);
        
        let m = ops.iter().map(|op| {
            op.borrow().entry.as_op().op.hash
        });
        let sp2 = Sp::new(id+1, ti.now_monotonic(),m, vec![], l.get_initial_entry_info());
        ti.set_sp_time_valid(sp2.info.time);
        let outer_sp2 = l.check_sp(sp2, &[], &[], &ti).unwrap();
        l.insert_outer_sp(outer_sp2);
        
        let ops = insert_ops(num_ops, num_ids, &mut l, &mut ti);
        let m = ops.iter().map(|op| {
            op.borrow().entry.as_op().op.hash
        });
        let sp3 = Sp::new(id, ti.now_monotonic(),m, vec![], sp1_info);
        ti.set_sp_time_valid(sp3.info.time);
        let _outer_sp3 = l.check_sp(sp3, &[], &[], &ti).unwrap();
    }
    
    #[test]
    fn late_op() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let id = 0;
        let op1 = make_op_late(OpState::new(id, &mut ti).unwrap());
        let outer_op1 = l.new_op(op1.op, &ti).unwrap();
        assert!(outer_op1.borrow().entry.as_op().arrived_late);
        
        // op2 will be included as normal op
        let op2 = OpState::new(id, &mut ti).unwrap();
        let outer_op2 = l.new_op(op2.op, &ti).unwrap();
        assert!(!outer_op2.borrow().entry.as_op().arrived_late);
        
        let sp1 = Sp::new(id, ti.now_monotonic(),[op1.hash].iter().cloned(), vec![], l.get_initial_entry_info());
        ti.set_sp_time_valid(sp1.info.time);
        // op3 has a later time so should not be included
        let op3 = Op::new(id, &mut ti);
        l.new_op(op3, &ti).unwrap();
        
        println!("op1 {}, op2 {}, op3 {}, sp1 {}", op1.op.info.time, op2.op.info.time, op3.info.time, sp1.info.time);

        // when input op2 will be used, so it will create a hash error
        assert_eq!(Error::LogError(LogError::SpHashNotComputed), l.check_sp(sp1, &[], &[], &ti).unwrap_err());
        // use both sp1 and sp2, using included input to check_sp for sp1
        let hash_vec = vec![op1.hash, op2.hash];
        let sp1 = Sp::new(id, ti.now_monotonic(),hash_vec.iter().cloned(), vec![], l.get_initial_entry_info());
        ti.set_sp_time_valid(sp1.info.time);
        
        l.check_sp(sp1, &[op1.get_entry_info()], &[], &ti).unwrap();
        
        // use both sp1 and sp2, using additional_ops input with to include sp1
        let op1_data = EntryInfoData{
            info: op1.get_entry_info(),
            data: get_empty_data(),
        };
        let sp1 = Sp::new(id, ti.now_monotonic(),hash_vec.iter().cloned(), vec![op1_data], l.get_initial_entry_info());
        ti.set_sp_time_valid(sp1.info.time);
        l.check_sp(sp1, &[], &[], &ti).unwrap();
    }
    
    #[test]
    fn unsupported_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let num_ids = 5;
        let num_ops  = 5;
        let ops = insert_ops(num_ops, num_ids, &mut l, &mut ti);
        // the not included ops are the odd ones
        let not_included: Vec<_> = ops.iter().filter_map(|op_rc| {
            let op_ref = op_rc.borrow();
            let op = op_ref.entry.as_op();
            if op.log_index % 2 != 0 { // dont include he non even ones
                return Some(op.op.get_entry_info())
            }
            None
        }).collect();
        let not_included_count = not_included.len();
        // the included ops are the even ones
        let m = ops.iter().filter_map(|op_rc| {
            let op_ref = op_rc.borrow();
            let op = op_ref.entry.as_op();
            if op.log_index % 2 == 0 { // only take the even ones
                return Some(op.op.hash)
            }
            None
        });
        
        let id = 0;
        let sp1 = SpState::new(id, ti.now_monotonic(),m, vec![], l.get_initial_entry_info()).unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        let sp1_info = sp1.get_entry_info();
        let outer_sp1 = l.check_sp(sp1.sp, &[], &not_included, &ti).unwrap();
        // be sure there are unsupported ops
        assert_eq!(not_included_count, outer_sp1.not_included_ops.len());
        l.insert_outer_sp(outer_sp1);
        
        // make an sp with the other items
        let sp2 = Sp::new(id, ti.now_monotonic(),not_included.iter().map(|nxt| nxt.hash), vec![], sp1_info);
        ti.set_sp_time_valid(sp2.info.time);
        let outer_sp2 = l.check_sp(sp2, &[], &[], &ti).unwrap();
        assert_eq!(0, outer_sp2.not_included_ops.len());
        l.insert_outer_sp(outer_sp2);
    }
    
    #[test]
    fn no_op_sp() {
        let mut ti = TimeTest::new();
        let l = Log::new();
        let id = 0;
        let op1 = OpState::new(id, &mut ti).unwrap();
        let sp1 = SpState::new(id, ti.now_monotonic(),[op1.hash].iter().cloned(), vec![], l.get_initial_entry_info()).unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        assert_eq!(Error::LogError(LogError::PrevSpHasNoLastOp), l.check_sp(sp1.sp, &[], &[], &ti).unwrap_err());
    }
    
    #[test]
    fn exact_sp() {
        let mut ti = TimeTest::new();
        let mut l = Log::new();
        let id = 0;
        let op1 = OpState::new(id, &mut ti).unwrap();
        let op2 = make_op_late(OpState::new(id, &mut ti).unwrap());
        l.new_op(op2.op, &ti).unwrap();
        l.new_op(op1.op, &ti).unwrap();
        let sp1 = SpState::new(id, ti.now_monotonic(),[op1.hash].iter().cloned(), vec![], l.get_initial_entry_info()).unwrap();
        ti.set_sp_time_valid(sp1.sp.info.time);
        let sp1_info = sp1.get_entry_info();
        let outer_sp1 = l.check_sp(sp1.sp, &[], &[], &ti).unwrap();
        assert_eq!(1, outer_sp1.not_included_ops.len());
        l.insert_outer_sp(outer_sp1);
        let sp2 = Sp::new(id, ti.now_monotonic(),[op2.hash].iter().cloned(), vec![], sp1_info);
        ti.set_sp_time_valid(sp2.info.time);
        let outer_sp2 = l.check_sp_exact(sp2, &[op2.get_entry_info()], &ti).unwrap();
        assert_eq!(0, outer_sp2.not_included_ops.len());
        l.insert_outer_sp(outer_sp2);
    }
    
    fn insert_ops(num_ops: usize, num_ids: Id, l: &mut Log, ti: &mut TimeTest) -> Vec<LogEntryStrong> {
        let mut ret = vec![];
        for _ in 0..num_ops {
            for i in 0..num_ids {
                ret.push(l.new_op(Op::new(i, ti), ti).unwrap());
            }
        }
        ret
    }
    
    fn get_entry_info(id: Id, l: &Log, last_sps: &HashMap<Id, EntryInfo>) -> EntryInfo {
        match last_sps.get(&id) {
            Some(ei) => *ei,
            None => l.get_initial_entry_info(),
        }
    }
    
}
