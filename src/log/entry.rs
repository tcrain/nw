use core::panic;
use std::{cell::RefCell, cmp::Ordering, fmt::{self, Debug, Display, Formatter}, rc::{Rc, Weak}};

use crate::{errors::{Error, LogError}, verification, utils::result_to_val};
use itertools::Itertools;
use super::{op::{EntryInfo, EntryInfoData, OpState}, sp::{Sp, SpState}};


pub struct LogEntry {
    pub log_index: u64,
    pub log_pointers: LogPointers,
    pub entry: PrevEntry,
    pub to_pointers: TotalOrderPointers,
}

impl Display for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "log_index: {}, {}", self.log_index, self.log_pointers)
    }
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl LogEntry {
    
    pub fn get_next(&self) -> Option<LogEntryStrong> {
        self.log_pointers.get_next()
    }
    
    pub fn set_next(&mut self, next: Option<LogEntryWeak>) {
        self.log_pointers.next_entry = next;
    }
    
    pub fn get_prev(&self) -> Option<LogEntryStrong> {
        self.log_pointers.get_prev()
    }
    
    pub fn set_prev(&mut self, prv: Option<LogEntryStrong>) {
        self.log_pointers.prev_entry = prv;
    }
}

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
            PrevEntry::Op(op) => write!(f, "{}", op)
        }
    }
}

pub fn prev_entry_strong_to_weak(entry: Option<LogEntryStrong>) -> Option<LogEntryWeak> {
    entry.map(|et| Rc::downgrade(&et))
}

impl Drop for PrevEntry {
    fn drop(&mut self) {
        println!("drop {}", self)
    }
}

impl PrevEntry {
    
    pub fn get_entry_info(&self) -> EntryInfo {
        match self {
            PrevEntry::Sp(sp) => sp.sp.get_entry_info(),
            PrevEntry::Op(op) => op.op.get_entry_info(),
        }        
    }
    
    pub fn mut_as_op(&mut self) -> &mut OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op
        }
    }
    
    pub fn as_op(&self) -> &OuterOp {
        match self {
            PrevEntry::Sp(_) => panic!("expected op"),
            PrevEntry::Op(op) => op
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
    
    pub fn get_prev_to(&self) -> Option<LogEntryStrong> {
        match &self.prev_to {
            None => None,
            Some(prv) => prv.upgrade()
        }
    }
    
    pub fn get_next_to(&self) -> Option<LogEntryStrong> {
        match &self.next_to {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }
}

pub fn set_next_total_order(prev: &LogEntryStrong, new_next: &LogEntryStrong) {
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

pub fn drop_item_total_order(item: &LogEntryStrong) {
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

pub type LogEntryStrong = Rc<RefCell<LogEntry>>;
pub type LogEntryWeak = Weak<RefCell<LogEntry>>;

pub struct TotalOrderPointers {
    pub next_to: Option<LogEntryWeak>,
    pub prev_to: Option<LogEntryWeak>,
}


pub struct LogPointers {
    pub next_entry: Option<LogEntryWeak>,
    pub prev_entry: Option<LogEntryStrong>,
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
    pub fn get_next(&self) -> Option<LogEntryStrong> {
        match self.next_entry.as_ref() {
            None => None,
            Some(nxt) => nxt.upgrade()
        }
    }
    
    pub fn get_prev(&self) -> Option<LogEntryStrong> {
        self.prev_entry.as_ref().map(|prv| {
            Rc::clone(prv)
        })
    }
}


pub fn drop_entry(entry: &LogEntryStrong) {
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

pub struct OuterOp {
    pub log_index: u64,
    pub op: OpState,
    pub include_in_hash: bool,
    pub arrived_late: bool,
    // hash: Hash,
    // verification: Verify,
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
        write!(f, "Op(log_index: {}, op: {})", self.log_index, self.op.op)
    }
}

pub struct OuterSp {
    pub sp: SpState,
    pub last_op: Option<LogEntryWeak>, // the last op in the total order this sp supported
    pub not_included_ops: Vec<LogEntryWeak>, // the ops before last_op in the log that are not included in the sp
    pub prev_sp: Option<LogEntryWeak>, // the previous sp in the total order in the log
}

pub fn set_next_sp(prev_sp: &LogEntryStrong, old_next_sp: Option<&LogEntryStrong>, new_next: &LogEntryStrong) {
    new_next.borrow_mut().entry.mut_as_sp().prev_sp = Some(Rc::downgrade(prev_sp));
    match old_next_sp {
        None => (),
        Some(old_next) => {
            old_next.borrow_mut().entry.mut_as_sp().prev_sp = Some(Rc::downgrade(new_next))
        }
    }
}

impl OuterSp {
    
    // returns all operations that have not been included in this SP in the log
    pub fn get_ops_after_iter<'a>(&'a self) -> Result<Box<dyn Iterator<Item = LogEntryStrong> + 'a>, Error> {
        let (not_included_iter, log_iter) = self.get_iters()?;
        let iter = not_included_iter.merge_by(log_iter, log_entry_strong_order);
        Ok(Box::new(iter))
    }

    pub fn check_sp_exact(&self, new_sp: Sp, exact: &[EntryInfo]) -> Result<OuterSp, Error> {
        // create a total order iterator over the operations including the unused items from the previous
        // SP that takes the exact set from exact, and returns the rest as unused, until last_op
        let (not_included_iter, log_iter) = self.get_iters()?;
        let op_iter = total_order_exact_iterator(
            not_included_iter,
            log_iter,
            exact.iter().cloned(),
            new_sp.additional_ops.iter().cloned(),
        );
        let (not_included_ops, last_op) = self.perform_check_sp(&new_sp, op_iter)?;
        let sp_state = SpState::from_sp(new_sp)?;
        Ok(OuterSp{
            sp: sp_state,
            last_op,
            not_included_ops,
            prev_sp: None,
        })
    }
        
    pub fn check_sp_log_order(&self, new_sp: Sp, included: &[EntryInfo], not_included: &[EntryInfo]) -> Result<OuterSp, Error> {
        // create a total order iterator over the operations of the log including the operations not included in thre previous sp
        // it uses included and not included to decide what operations to include
        // furthermore it includes items that have include_in_hash = true and are after the last operation of the previous
        // SP in the log
        let (not_included_iter, log_iter) = self.get_iters()?;
        let op_iter = total_order_check_iterator(
            not_included_iter,
            log_iter,
            included.iter().cloned(),
            not_included.iter().cloned(),
            new_sp.additional_ops.iter().cloned()
        );
        let (not_included_ops, last_op) = self.perform_check_sp(&new_sp, op_iter)?;
        let sp_state = SpState::from_sp(new_sp)?;
        Ok(OuterSp{
            sp: sp_state,
            last_op,
            not_included_ops,
            prev_sp: None,
        })
    }

    fn get_iters<'a>(&'a self) -> Result<(impl Iterator<Item = LogEntryStrong> + 'a, TotalOrderAfterIterator), Error> {
        // we make an iterator that goes through the log in total order
        // the iterator starts from the log entry of the last op included in the previous SP (self) and traverses the log
        // from there in total order, returning the entries that occur later in the log, plus the ops that are in the
        // not_included list of the previous SP.
        let not_included_iter = self.not_included_ops.iter().map(|nxt| {
            nxt.upgrade().unwrap()
        }); // items from previous sp that are earlier in the log, we need to check if these are included
        let last_op = self.last_op.as_ref().ok_or(Error::LogError(LogError::PrevSpHasNoLastOp))?.upgrade();
        // we only want operations after the last op of the previous sp in the log
        let mut log_iter = total_order_after_iterator(last_op.as_ref());
        if !self.sp.sp.is_init() { // if prev sp is not the inital SP, then we need to move forward 1 op since last op was already included in prev sp
            log_iter.next();
        }    
        Ok((not_included_iter, log_iter))
    }

    fn perform_check_sp(&self, new_sp: &Sp, mut op_iter: impl Iterator<Item = (Supported, LogEntryStrong)>) -> Result<(Vec<LogEntryWeak>, Option<LogEntryWeak>), Error> {
        let mut hasher = verification::new_hasher();
        hasher.update(self.sp.hash.as_bytes());
        let mut not_included_ops = vec![];
        let mut last_op = None;
        let count = result_to_val(op_iter.try_fold(0, | mut count, (supported, nxt_op) | {
            if count >= new_sp.new_ops_supported { // see if we already computed enough ops
                return Err(count)
            }
            let op_ref =  nxt_op.borrow();
            let op = &op_ref.entry.as_op().op;
            println!("op index {}", op_ref.entry.as_op().log_index);
            if op.op.info.time > new_sp.info.time { // see if we have passed all ops with smaller times
                return Err(count)
            }
            println!("supported {:#?}", supported);
            // see if we should in include the op
            match supported {
                Supported::Supported => (), // normal op
                Supported::SupportedData(_) => (), // TODO should do something with this data?
                Supported::NotSupported => {
                    not_included_ops.push(Rc::downgrade(&nxt_op));
                    return Ok(count)
                }
            }
            println!("add hash {}, sp time {}", nxt_op.borrow().entry.as_op().op.op.info.time, new_sp.info.time);
            // add the hash of the op
            hasher.update(nxt_op.borrow().entry.as_op().op.hash.as_bytes());
            count += 1;
            // update the last op pointer
            last_op = Some(Rc::downgrade(&nxt_op));
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

impl Debug for OuterSp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

pub fn total_order_iterator(start: Option<&LogEntryStrong>, forward: bool) -> TotalOrderIterator {
    TotalOrderIterator{
        prev_entry: start.map(|op| Rc::clone(op)),
        forward,
    }
}


pub struct TotalOrderIterator {
    pub prev_entry: Option<LogEntryStrong>,
    pub forward: bool,
}

impl Iterator for TotalOrderIterator {
    type Item = LogEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.prev_entry.as_ref().map(|prv| Rc::clone(prv));
        self.prev_entry = match self.prev_entry.take() {
            None => None,
            Some(prv) => {
                if self.forward {
                    prv.borrow().to_pointers.get_next_to()
                } else {
                    prv.borrow().to_pointers.get_prev_to()
                }
            }
        };
        ret
    }
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input
pub struct TotalOrderAfterIterator {
    iter: TotalOrderIterator,
    min_index: u64,
}

impl Iterator for TotalOrderAfterIterator {
    type Item = LogEntryStrong;
    
    fn next(&mut self) -> Option<Self::Item> {
        while let Some(nxt) = self.iter.next() {
            if nxt.borrow().log_index >= self.min_index {
                return Some(nxt)
            }
        }
        None
    }
}

pub fn total_order_after_iterator(start: Option<&LogEntryStrong>) -> TotalOrderAfterIterator {
    TotalOrderAfterIterator{
        iter: total_order_iterator(start, true),
        min_index: start.map_or(0, |entry| entry.borrow().log_index),
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
pub struct TotalOrderExactIterator<'a, J, K> where
J: Iterator<Item = EntryInfo>,
K: Iterator<Item = EntryInfoData> {
    iter: Box<dyn Iterator<Item = LogEntryStrong> + 'a>,
    extra_info_check: ExtraInfoCheck<K>,
    exact_check: IncludedCheck<J>,
}

fn log_entry_strong_order(x: &LogEntryStrong, y: &LogEntryStrong) -> bool {
        x.borrow().entry.get_entry_info() <= y.borrow().entry.get_entry_info()
}

pub fn total_order_exact_iterator<'a, J, K, I>(prev_not_included: I, log_iter: TotalOrderAfterIterator, exact: J, extra_info: K) -> TotalOrderExactIterator<'a, J, K> where
I: Iterator<Item = LogEntryStrong> + 'a,
J: Iterator<Item = EntryInfo>,
K: Iterator<Item = EntryInfoData> {
    // now we merge the prev_not_included and the log_iter into a single iterator them so we traverse the two of them in total order
    let iter = Box::new(log_iter.merge_by(prev_not_included, log_entry_strong_order));
    TotalOrderExactIterator{
        iter,
        extra_info_check: ExtraInfoCheck{
            last_extra_info: None,
            extra_info,
        },
        exact_check: IncludedCheck{
            last_included: None,
            included: exact,
        },
    }
}

impl<'a, J, K> Iterator for TotalOrderExactIterator<'a, J, K> where
J: Iterator<Item = EntryInfo>,
K: Iterator<Item = EntryInfoData> {
    type Item = (Supported, LogEntryStrong);
    
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(nxt) = self.iter.next() {
            let info = nxt.borrow().entry.get_entry_info();
            // first check if the op is supported by an extra info field
            if let Some(extra_info) = self.extra_info_check.check_extra_info(info) {
                return Some((Supported::SupportedData(extra_info), nxt))
            }
            // next check if the op is supported as part of the exact list
            if self.exact_check.check_supported(info) {
                return Some((Supported::Supported, nxt))
            }
            // oterwise it is not included
            return Some((Supported::NotSupported, nxt))
        };
        None
    }
}

struct IncludedCheck<J> where
J: Iterator<Item = EntryInfo> {
    last_included: Option<EntryInfo>,
    included: J,
}

impl<J> IncludedCheck<J> where
J: Iterator<Item = EntryInfo> {
    
    fn check_supported(&mut self, entry: EntryInfo) -> bool {
        let mut found = false;
        if let Some(last_supported) = self.last_included.as_ref()  {
            if last_supported >= &entry {
                found = true;
            }
        }
        if !found {
            self.last_included = self.included.find(|last_supported| {
                last_supported >= &entry
            })
        }
        if let Some(last_support) = self.last_included.as_ref() {
            return last_support == &entry
        }
        false
    }
}

struct ExtraInfoCheck<K> where 
K: Iterator<Item = EntryInfoData> {
    last_extra_info: Option<EntryInfoData>,
    extra_info: K,    
}

impl<K> ExtraInfoCheck<K> where
K: Iterator<Item = EntryInfoData> {
    
    fn check_extra_info(&mut self, entry: EntryInfo) -> Option<EntryInfoData> {
        let mut found = false;
        if let Some(last_extra_info) = self.last_extra_info.as_ref()  {
            if last_extra_info.info >= entry {
                found = true;
            }
        }
        if !found {
            self.last_extra_info = self.extra_info.find(|last_extra_info| {
                last_extra_info.info >= entry
            })
        }
        if let Some(last_extra_info) = self.last_extra_info.as_ref() {
            if last_extra_info.info == entry {
                return Some(*last_extra_info)
            }
        }
        None
    }
}

// total order iterator that only includes log entries with a larger (or equal) log index than the input,
// except for those that are supported/unsupported
pub struct TotalOrderCheckIterator<'a, J, K> where
J: Iterator<Item = EntryInfo>,
K: Iterator<Item = EntryInfoData> {
    iter: Box<dyn Iterator<Item = LogEntryStrong> + 'a>,
    extra_info_check: ExtraInfoCheck<K>,
    included_check: IncludedCheck<J>,
    not_included_check: IncludedCheck<J>,
}

fn entry_less_than(x: &LogEntryStrong, y: &LogEntryStrong) -> bool {
    x.borrow().entry.get_entry_info() <= y.borrow().entry.get_entry_info()
}

pub fn total_order_check_iterator<'a, J, K, I>(prev_not_included: I, log_iter: TotalOrderAfterIterator, included: J, not_included: J, extra_info: K) -> TotalOrderCheckIterator<'a, J, K> where
I: Iterator<Item = LogEntryStrong> + 'a,
J: Iterator<Item = EntryInfo>,
K: Iterator<Item = EntryInfoData> {
    // now we merge the prev_not_included and the log_iter into a single iterator them so we traverse the two of them in total order
    let iter = Box::new(log_iter.merge_by(prev_not_included, entry_less_than));
    TotalOrderCheckIterator{
        iter,
        extra_info_check: ExtraInfoCheck{
            last_extra_info: None,
            extra_info,
        },
        included_check: IncludedCheck{
            last_included: None,
            included,
        },
        not_included_check: IncludedCheck{
            last_included: None,
            included: not_included,
        },
    }
}

impl<'a, J, K> Iterator for TotalOrderCheckIterator<'a, J, K> where
J: Iterator<Item = EntryInfo>,
K: Iterator<Item = EntryInfoData> {
    type Item = (Supported, LogEntryStrong);
    
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(nxt) = self.iter.next() {

            let info = nxt.borrow().entry.get_entry_info();
            // first check if the op is supported by an extra info field
            if let Some(extra_info) = self.extra_info_check.check_extra_info(info) {
                return Some((Supported::SupportedData(extra_info), nxt))
            }
            // next check if the op is supported as part of the supported list
            if self.included_check.check_supported(info) {
                return Some((Supported::Supported, nxt))
            }
            // next check if it is not supported as part of the not supported list
            if self.not_included_check.check_supported(info) {
                return Some((Supported::NotSupported, nxt))
            }
            // finally we only add operations that arrived on time
            if nxt.borrow().entry.as_op().include_in_hash {
                return Some((Supported::Supported, nxt))
            } else { // otherwise we add it to the not included list
                return Some((Supported::NotSupported, nxt))
            }
        }
        None
    }
}

pub struct LogIterator {
    pub prv_entry: Option<LogEntryStrong>,
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
