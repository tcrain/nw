use std::{cmp::Ordering, rc::{Rc, Weak}};
use std::cell::{RefCell, Ref};
use std::ops::Deref;
use std::fmt::{Debug, Display, Formatter};
use std::{fmt};
use crate::errors::{Error, LogError};

use super::op::Op;

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
    
    fn get_next(&self) -> Option<PrevEntryStrong> {
        match self {
            PrevEntry::Sp(sp) => sp.log_pointers.get_next(),
            PrevEntry::Op(op) => op.log_pointers.get_next(),
        }
    }
    
    fn set_next(&mut self, next: &PrevEntryStrong) {
        match self {
            PrevEntry::Sp(sp) => {
                sp.log_pointers.next_entry = Some(Rc::downgrade(next));
            },
            PrevEntry::Op(op) => {
                op.log_pointers.next_entry = Some(Rc::downgrade(next));
            },
        };
    }
    
    fn drop_previous(&mut self) {
        match self {
            PrevEntry::Sp(sp) => sp.log_pointers.prev_entry = None,
            PrevEntry::Op(op) => op.drop_previous(),
        }
    }
    
    fn get_prev(&self) -> Option<PrevEntryStrong> {
        match self {
            PrevEntry::Sp(sp) => sp.log_pointers.get_prev(),
            PrevEntry::Op(op) => op.log_pointers.get_prev(),
        }
    }
}

type PrevEntryStrong = Rc<RefCell<PrevEntry>>;
type PrevEntryWeak = Weak<RefCell<PrevEntry>>;

struct LogPointers {
    next_entry: Option<PrevEntryWeak>,
    prev_entry: Option<PrevEntryStrong>,
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
    fn get_next(&self) -> Option<PrevEntryStrong> {
        match self.next_entry.as_ref() {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }
    
    fn get_prev(&self) -> Option<PrevEntryStrong> {
        self.prev_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        })
    }
}

struct OuterOp {
    log_pointers: LogPointers,
    log_index: u64,
    op: Op,
    // hash: Hash,
    // verification: Verify,
    prev_op: Option<PrevEntryWeak>,
    next_op: Option<PrevEntryWeak>,
}

fn set_next_op(prev: &PrevEntryStrong, new_next: &PrevEntryStrong) {
    match prev.borrow().as_op().get_next_op() {
        None => (),
        Some(nxt) => {
            new_next.borrow_mut().mut_as_op().next_op = Some(Rc::downgrade(&nxt));
            nxt.borrow_mut().mut_as_op().prev_op = Some(Rc::downgrade(new_next))
        }
    };
    new_next.borrow_mut().mut_as_op().prev_op = Some(Rc::downgrade(prev));
    prev.borrow_mut().mut_as_op().next_op = Some(Rc::downgrade(new_next));
}

impl OuterOp {
    
    fn drop_previous(&mut self) {
        match self.get_next_op() {
            None => (),
            Some(nxt) => {
                nxt.borrow_mut().mut_as_op().prev_op = self.prev_op.take();
            }
        }
        self.log_pointers.prev_entry = None;
    }
    
    fn get_prev_op(&self) -> Option<PrevEntryStrong> {
        match &self.prev_op {
            None => None,
            Some(prv) => prv.upgrade()
        }
    }
    
    fn get_next_op(&self) -> Option<PrevEntryStrong> {
        match &self.next_op {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }
}

impl PartialOrd for OuterOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OuterOp {
    fn cmp(&self, other: &Self) -> Ordering {
        if self.log_index > other.log_index {
            Ordering::Greater
        } else if self.log_index < other.log_index {
            Ordering::Less
        } else {
            self.op.cmp(&other.op)
        }
    }
}

impl Eq for OuterOp {}

impl PartialEq for OuterOp {
    fn eq(&self, other: &Self) -> bool {
        if self.op == other.op && self.log_index == other.log_index {
            true
        } else {
            false
        }
    }
}

impl Display for OuterOp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Op(log_index: {}, op: {}, {})", self.log_index, self.op, self.log_pointers)
    }
}

struct OuterSp {
    log_index: u64,
    log_pointers: LogPointers,
    // sp: SP,
    // not_included_op: Vec<Rc<Outerop>>,
    // prev_local: RefCell<Weak<OuterSP>>,
    // prev_to: RefCell<Weak<OuterSP>>,
}

impl Display for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "SP(log_index: {}, {})", self.log_index, self.log_pointers)
    }
}

fn to_prev_entry_holder(prv: &PrevEntryStrong) -> PrevEntryHolder {
    PrevEntryHolder{prv_entry: (&**prv).borrow()}
}

struct PrevEntryHolder<'a> {
    prv_entry: Ref<'a, PrevEntry>,
}

impl<'a> Deref for PrevEntryHolder<'a> {
    type Target = PrevEntry;
    
    fn deref(&self) -> &PrevEntry {
        &self.prv_entry
    }
}

struct OpIterator {
    prev_entry: Option<PrevEntryStrong>,
}

impl Iterator for OpIterator {
    type Item = PrevEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prev_entry.as_ref().map(|prv| Rc::clone(prv));
        self.prev_entry = match self.prev_entry.take() {
            None => None,
            Some(prv) => {
                prv.borrow().as_op().get_prev_op()
            }
        };
        ret
    }
}

struct LogIterator {
    prv_entry: Option<PrevEntryStrong>,
}

impl Iterator for LogIterator {
    type Item = PrevEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prv_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        });
        self.prv_entry = match self.prv_entry.take() {
            None => None,
            Some(prv) => {
                (& *prv).borrow().get_prev()
            }
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
    
    fn drop_first(&mut self) -> Result<PrevEntryStrong, Error> {
        match self {
            LogItems::Empty => Result::Err(Error::LogError(LogError::EmptyLogError)),
            LogItems::Single(si) => {
                let prv = Rc::clone(&si.entry);
                *self = LogItems::Empty;
                Ok(prv)
            },
            LogItems::Multiple(mi) => {
                let nxt = (&*mi.first_entry).borrow().get_next().unwrap();
                let ret = Rc::clone(&nxt);
                &nxt.borrow_mut().drop_previous();
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
    
    fn add_item(&mut self, idx: u64, op: Op) -> PrevEntryStrong {
        let prv = match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(Rc::clone(&mi.last_entry))
        };
        let new_op = Rc::new(RefCell::new(PrevEntry::Op(OuterOp{
            log_pointers: LogPointers {
                next_entry: None,
                prev_entry: prv,
            },
            prev_op: None,
            next_op: None,
            op: op,
            log_index: idx,
        })));
        let new_op_ref = Rc::clone(&new_op);
        match self {
            LogItems::Empty => {
                *self = LogItems::Single(LogSingle{
                    entry: new_op_ref
                })
            }
            LogItems::Single(si) => {
                si.entry.borrow_mut().set_next(&new_op_ref);
                let first = Rc::clone(&si.entry);
                *self = LogItems::Multiple(LogMultiple{
                    last_entry: new_op_ref,
                    first_entry: first,
                })
            }
            LogItems::Multiple(lm) => {
                lm.last_entry.borrow_mut().set_next(&new_op_ref);
                lm.last_entry = new_op_ref
            }
        };
        new_op
    }
    
    fn get_last(&self) -> Option<PrevEntryStrong> {
        match self {
            LogItems::Empty => None,
            LogItems::Single(si) => Some(Rc::clone(&si.entry)),
            LogItems::Multiple(mi) => Some(Rc::clone(&mi.last_entry)),
        }
    }
}

struct LogSingle {
    entry: PrevEntryStrong,
}

struct LogMultiple {
    last_entry: PrevEntryStrong,
    first_entry: PrevEntryStrong
}

struct Log {
    index: u64,
    first_last: LogItems,
    last_op: Option<PrevEntryWeak>,
}

impl Log {
    fn new() -> Log {
        Log{
            index: 0,
            first_last: LogItems::Empty,
            last_op: None,
        }
    }
    
    fn op_iterator(&self) -> OpIterator {
        OpIterator{
            prev_entry: match &self.last_op {
                None => None,
                Some(prv) => prv.upgrade()
            }
        }
    }
    
    fn log_iterator(&self) -> LogIterator {
        self.first_last.log_iterator()
    }
    
    fn new_op(&mut self, op: Op) -> PrevEntryStrong {
        self.index += 1;
        // add the new op to the log
        let new_op_ref = self.first_last.add_item(self.index, op);
        // put the new op in the total ordered list of transactions
        let mut last = None;
        for nxt in self.op_iterator() {
            if nxt.borrow().as_op().op < new_op_ref.borrow().as_op().op {
                set_next_op(&nxt, &new_op_ref);
                last = None;
                break
            } else {
                last = Some(nxt);
            }
        }
        // if last != None then we are at the begginning of the list, so we must update the new op next pointer
        last.map(|last_op| set_next_op(&new_op_ref, &last_op)); 
        
        
        // update the last op pointer if necessary
        self.last_op = match self.last_op.take() {
            None => Some(Rc::downgrade(&new_op_ref)),
            Some(weak_last) => {
                weak_last.upgrade().map(| last| {
                    if last.borrow().as_op().op < new_op_ref.borrow().as_op().op {
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
            l.first_last.drop_first().unwrap();
        }
        assert!(l.log_iterator().next().is_none());
        let err = l.first_last.drop_first().unwrap_err();
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
        let op1_clone = op1.clone();
        let op2_clone = op2.clone();
        // insert the op in reserve order of creation
        let _ = l.new_op(op2);
        let _ = l.new_op(op1);
        // when traversing in total order, we should see op2 first since it has a larger order
        let mut iter = l.op_iterator();
        assert_eq!(op2_clone, iter.next().unwrap().borrow().as_op().op);
        assert_eq!(op1_clone, iter.next().unwrap().borrow().as_op().op);
        
    }
}
