use std::{cmp::Ordering, rc::{Rc, Weak}};
use std::cell::{RefCell, Ref};
use std::ops::Deref;
use std::fmt::{Debug, Display, Formatter};
use std::{fmt};
use crate::{errors::{Error, LogError}, verification::Id};

use super::{op::Op};
use super::sp::Sp;

// TODO: check equal when inserting, then don't insert

struct LogEntry {
    log_index: u64,
    log_pointers: LogPointers,
    entry: PrevEntry,
    to_pointers: TotalOrderPointers,
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "log_index: {}, {}", self.log_index, self.log_pointers)
    }
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self, f)
    }
}

impl LogEntry {
    fn get_next(&self) -> Option<LogEntryStrong> {
        self.log_pointers.get_next()
    }
    
    fn set_next(&mut self, next: Option<LogEntryWeak>) {
        self.log_pointers.next_entry = next;
    }
    
    fn get_prev(&self) -> Option<LogEntryStrong> {
        self.log_pointers.get_prev()
    }
    
    fn set_prev(&mut self, prv: Option<LogEntryStrong>) {
        self.log_pointers.prev_entry = prv;
    }
}

enum PrevEntry {
    Sp(OuterSp),
    Op(OuterOp),
}

impl Debug for PrevEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        Debug::fmt(&self, f)
    }
}

impl Display for PrevEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            PrevEntry::Sp(sp) => write!(f, "{}", sp),
            PrevEntry::Op(op) => write!(f, "{}", op)
        }
    }
}

fn prev_entry_strong_to_weak(entry: Option<LogEntryStrong>) -> Option<LogEntryWeak> {
    entry.map(|et| Rc::downgrade(&et))
}

impl Drop for PrevEntry {
    fn drop(&mut self) {
        println!("drop {}", self)
    }
}

impl PrevEntry {
    
    fn mut_as_op(&mut self) -> &mut OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op
        }
    }
    
    fn as_op(&self) -> &OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op
        }
    }
    
    fn mut_as_sp(&mut self) -> &mut OuterSp {
        match self {
            PrevEntry::Sp(sp) => sp,
            PrevEntry::Op(_) => panic!("expected sp"),
        }
    }
    
    fn as_sp(&self) -> &OuterSp {
        match self {
            PrevEntry::Sp(sp) => sp,
            PrevEntry::Op(_) => panic!("expected sp"),
        }
    }        
}

type LogEntryStrong = Rc<RefCell<LogEntry>>;
type LogEntryWeak = Weak<RefCell<LogEntry>>;

struct TotalOrderPointers {
    next_to: Option<LogEntryWeak>,
    prev_to: Option<LogEntryWeak>,
}

impl TotalOrderPointers {
    fn get_prev_to(&self) -> Option<LogEntryStrong> {
        match &self.prev_to {
            None => None,
            Some(prv) => prv.upgrade()
        }
    }
    
    fn get_next_to(&self) -> Option<LogEntryStrong> {
        match &self.next_to {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }
}

fn set_next_total_order(prev: &LogEntryStrong, new_next: &LogEntryStrong) {
    match prev.borrow().to_pointers.get_next_to() {
        None => (),
        Some(nxt) => {
            new_next.borrow_mut().to_pointers.next_to = Some(Rc::downgrade(&nxt));
            nxt.borrow_mut().to_pointers.prev_to = Some(Rc::downgrade(new_next))
        }
    };
    new_next.borrow_mut().to_pointers.prev_to = Some(Rc::downgrade(prev));
    prev.borrow_mut().to_pointers.next_to = Some(Rc::downgrade(new_next));
}

fn drop_item_total_order(item: &LogEntryStrong) {
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
}

struct LogPointers {
    next_entry: Option<LogEntryWeak>,
    prev_entry: Option<LogEntryStrong>,
}

impl Display for LogPointers {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let prv = match &self.prev_entry {
            None => "None",
            Some(_) => "Some",
        };
        let nxt = match &self.next_entry {
            None => "None",
            Some(ne) => {
                match ne.upgrade() {
                    None => "None",
                    Some(_) => "Some"
                }
            },
        };
        write!(f, "prev_entry: {}, next_entry: {}", prv, nxt)
    }
}

impl LogPointers {
    fn get_next(&self) -> Option<LogEntryStrong> {
        match self.next_entry.as_ref() {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }
    
    fn get_prev(&self) -> Option<LogEntryStrong> {
        self.prev_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        })
    }
}

struct OuterOp {
    log_index: u64,
    op: Op,
    // hash: Hash,
    // verification: Verify,
    prev_op_total_order: Option<LogEntryWeak>,
    next_op_total_order: Option<LogEntryWeak>,
}

fn drop_entry(entry: &LogEntryStrong) {
    drop_item_total_order(entry);
    let (prev, next) = (entry.borrow().get_prev(), entry.borrow().get_next());
    match next.as_ref() {
        Some(nxt) => {
            match prev.as_ref() {
                None => nxt.borrow_mut().set_prev(None),
                Some(prv) => {
                    nxt.borrow_mut().set_prev(Some(Rc::clone(prv)));
                    prv.borrow_mut().set_next(Some(Rc::downgrade(nxt)));
                }
            }
        },
        None => {
            if let Some(prv) = prev.as_ref() {
                prv.borrow_mut().set_next(None)
            }
        }
    }
}

impl OuterOp {}

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

impl Display for OuterOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Op(log_index: {}, op: {})", self.log_index, self.op)
    }
}

struct OuterSp {
    sp: Sp,
    not_included_ops: Vec<LogEntryWeak>,
    // last_op: LogEntryWeak,
    prev_sp: Option<LogEntryWeak>,
    prev_sp_total_order: Option<LogEntryWeak>,
    next_sp_total_order: Option<LogEntryWeak>,
}

fn set_next_sp(prev_sp: &LogEntryStrong, old_next_sp: Option<&LogEntryStrong>, new_next: &LogEntryStrong) {
    new_next.borrow_mut().entry.mut_as_sp().prev_sp = Some(Rc::downgrade(prev_sp));
    match old_next_sp {
        None => (),
        Some(old_next) => {
            old_next.borrow_mut().entry.mut_as_sp().prev_sp = Some(Rc::downgrade(new_next))
        }
    }
}

impl OuterSp {
    fn get_prev_sp(&self) -> Option<LogEntryStrong> {
        match &self.prev_sp {
            None => None,
            Some(prv) => prv.upgrade()
        }
    }
}

impl Display for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SP")
    }
}

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

struct TotalOrderIterator {
    prev_entry: Option<LogEntryStrong>,
}

impl Iterator for TotalOrderIterator {
    type Item = LogEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prev_entry.as_ref().map(|prv| Rc::clone(prv));
        self.prev_entry = match self.prev_entry.take() {
            None => None,
            Some(prv) => prv.borrow().to_pointers.get_prev_to()
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
            if nxt.borrow().entry.as_sp().sp.id == self.id {
                return Some(nxt)
            }
        }
        None
    }
}

struct LogIterator {
    prv_entry: Option<LogEntryStrong>,
}

impl Iterator for LogIterator {
    type Item = LogEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prv_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        });
        self.prv_entry = match self.prv_entry.take() {
            None => None,
            Some(prv) => (& *prv).borrow().get_prev()
        };
        ret
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

struct Log {
    index: u64,
    first_last: LogItems,
    last_op_total_order: Option<LogEntryWeak>,
    last_sp: Option<LogEntryWeak>,
    last_sp_total_order: Option<LogEntryWeak>,
}

impl Log {
    fn new() -> Log {
        Log{
            index: 0,
            first_last: LogItems::Empty,
            last_op_total_order: None,
            last_sp: None,
            last_sp_total_order: None,
        }
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
            }
        }
    }
    
    fn sp_iterator(&self) -> SpIterator {
        SpIterator{
            prev_entry: match &self.last_sp {
                None => None,
                Some(prv) => prv.upgrade()
            }
        }
    }
    
    fn op_total_order_iterator(&self) -> TotalOrderIterator {
        TotalOrderIterator{
            prev_entry: match &self.last_op_total_order {
                None => None,
                Some(prv) => prv.upgrade()
            }
        }
    }
    
    fn log_iterator(&self) -> LogIterator {
        self.first_last.log_iterator()
    }
    
    fn check_sp(&self, _sp: Sp) -> Result<OuterSp, Error> {
        Err(Error::LogError(LogError::PrevSpNotFound))
    }
    
    
    fn new_sp(&mut self, sp: OuterSp) -> LogEntryStrong {
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
    
    fn new_op(&mut self, op: Op) -> LogEntryStrong {
        self.index += 1;
        let outer_op = PrevEntry::Op(OuterOp{
            prev_op_total_order: None,
            next_op_total_order: None,
            op,
            log_index: self.index,
        });
        // add the new op to the log
        let new_op_ref = self.first_last.add_item(self.index, outer_op);
        // put the new op in the total ordered list of ops
        let mut last = None;
        for nxt in self.op_total_order_iterator() {
            if nxt.borrow().entry.as_op().op < new_op_ref.borrow().entry.as_op().op {
                set_next_total_order(&nxt, &new_op_ref);
                last = None;
                break
            } else {
                last = Some(nxt);
            }
        }
        // if last != None then we are at the begginning of the list, so we must update the new op next pointer
        if let Some(last_op) = last {
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
        Rc::clone(&new_op_ref)
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
    
    use crate::verification::Hash;
    
    use super::*;
    
    #[test]
    fn add_op() {
        let mut l = Log::new();
        for _ in 0..5 {
            let _ = l.new_op(Op::new(1));
        }
        let mut items = vec![];
        for op in l.log_iterator() {
            items.push(op);
        }
        assert_eq!(5, items.len())
    }
    
    #[test]
    fn drop_op() {
        let mut l = Log::new();
        for _ in 0..5 {
            let _ = l.new_op(Op::new(1));
        }
        for _ in 0..5 {
            l.drop_first().unwrap();
        }
        assert!(l.log_iterator().next().is_none());
        let err = l.drop_first().unwrap_err();
        assert_eq!(Error::LogError(LogError::EmptyLogError), err); 
        for _ in 0..5 {
            let _ = l.new_op(Op::new(2));
        }
    }
    
    #[test]
    fn op_iterator() {
        let mut l = Log::new();
        let op1 = Op::new(1);
        let op2 = Op::new(2);
        let op3 = Op::new(3);
        let op1_clone = op1;
        let op2_clone = op2;
        let op3_clone = op3;
        // insert the op in reserve order of creation
        let _ = l.new_op(op2);
        let _ = l.new_op(op3);
        let _ = l.new_op(op1);
        // when traversing in total order, we should see op2 first since it has a larger order
        let mut iter = l.op_total_order_iterator();
        assert_eq!(op3_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op2_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        
        // drop the first
        l.drop_first().unwrap();
        // be sure we can still traverse the remaining ops
        iter = l.op_total_order_iterator();
        assert_eq!(op3_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert!(iter.next().is_none());
        
        l.drop_first().unwrap();
        // be sure the pointer to the op was updated since we have a new last op in the total order
        iter = l.op_total_order_iterator();
        assert_eq!(op1_clone, iter.next().unwrap().borrow().entry.as_op().op);
        assert!(iter.next().is_none())
    }
    
    #[test]
    fn add_sp() {
        let mut l = Log::new();
        let mut prev_sp_hash = None;
        let num_sp = 5;
        let id = 1;
        for _ in 0..num_sp {
            let new_sp = l.new_sp(gen_sp(id, prev_sp_hash));
            prev_sp_hash = Some(new_sp.borrow().entry.as_sp().sp.hash);
        }
        let mut items = vec![];
        for nxt in l.sp_iterator() {
            items.push(nxt);
        }
        assert_eq!(num_sp, items.len());
        
        items = vec![];
        for nxt in l.sp_total_order_iterator() {
            items.push(nxt);
        }
        assert_eq!(num_sp, items.len());
        
        items = vec![];
        for nxt in l.sp_local_total_order_iterator(id) {
            items.push(nxt);
        }
        assert_eq!(5, items.len());
        
        assert!(l.sp_local_total_order_iterator(id+1).next().is_none());
    }
    
    #[test]
    fn drop_sp() {
        let mut l = Log::new();
        let mut prev_sp_hash = None;
        let num_sp = 5;
        let id = 1;
        for _ in 0..num_sp {
            let new_sp = l.new_sp(gen_sp(id, prev_sp_hash));
            prev_sp_hash = Some(new_sp.borrow().entry.as_sp().sp.hash);
        }
        for _ in 0..5 {
            l.drop_first().unwrap();
        }
        assert!(l.log_iterator().next().is_none());
        let err = l.drop_first().unwrap_err();
        assert_eq!(Error::LogError(LogError::EmptyLogError), err);
        
        prev_sp_hash = None;
        for _ in 0..num_sp {
            let new_sp = l.new_sp(gen_sp(id, prev_sp_hash));
            prev_sp_hash = Some(new_sp.borrow().entry.as_sp().sp.hash);
        }
    }
    
    #[test]
    fn sp_iterator() {
        let mut l = Log::new();
        let mut last_sps: HashMap<Id, Hash> = HashMap::new();
        let num_ids = 5;
        let num_sps = 5;
        
        for _ in 0..num_sps {
            for i in 0..num_ids {
                let new_sp = l.new_sp(gen_sp(i, last_sps.remove(&i)));
                last_sps.insert(i, new_sp.borrow().entry.as_sp().sp.hash);    
            }
        }
        
        let mut items = vec![];
        for nxt in l.sp_iterator() {
            items.push(nxt)
        }
        assert_eq!((num_ids * num_sps) as usize, items.len());
        
        for i in 0..num_ids {
            let mut items = vec![];
            for nxt in l.sp_local_total_order_iterator(i) {
                items.push(nxt)
            }
            assert_eq!(num_sps as usize, items.len())
        }
        
        // insert two in reverse order
        let iter_id = 0;
        let sp1 = gen_sp(iter_id, Some(*last_sps.get(&iter_id).unwrap()));
        let sp2 = gen_sp(iter_id, Some(*last_sps.get(&iter_id).unwrap()));
        let sp1_clone = sp1.sp.clone();
        let sp2_clone = sp2.sp.clone();
        l.new_sp(sp2);
        l.new_sp(sp1);
        
        // should be in insert order
        let mut iter = l.sp_iterator();
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
    
    fn gen_sp(id: u64, prev_sp_hash: Option<Hash>) -> OuterSp {
        OuterSp{
            sp: Sp::new(id, prev_sp_hash),
            not_included_ops: vec![],
            prev_sp: None,
            prev_sp_total_order: None,
            next_sp_total_order: None,
        }
    }
}
