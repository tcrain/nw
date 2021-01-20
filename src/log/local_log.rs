use std::{cmp::Ordering};
use crate::{errors::{Error}, verification::{Id, TimeInfo}};

use super::{basic_log::Log, entry::LogEntryStrong, op::{EntryInfo, Op}, sp::{Sp, SpState}};

struct HeapOp(LogEntryStrong);

impl Eq for HeapOp {}

impl Ord for HeapOp {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.get_ptr().borrow().entry.as_op().op.cmp(&other.0.get_ptr().borrow().entry.as_op().op)
    }
}

impl PartialOrd for HeapOp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HeapOp {
    fn eq(&self, other: &Self) -> bool {
        self.0.get_ptr().borrow().entry.as_op().op == other.0.get_ptr().borrow().entry.as_op().op
    }
}


pub struct LocalLog {
    id: Id,
    log: Log,
    // new_ops: BinaryHeap<HeapOp>,
    last_sp: LogEntryStrong,
}

pub fn new_local_log(id: Id, l: Log) -> Result<LocalLog, Error> {
    let last_sp = l.get_last_sp_id(id).or_else(|_| {
        l.get_initial_sp()
    })?;
    Ok(LocalLog {
        id,
        log: l,
        last_sp,
    })
}

pub struct SpToProcess {
    pub sp: Sp,
    pub included: Vec<EntryInfo>,
    pub not_included: Vec<EntryInfo>,
}

pub struct SpExactToProcess {
    pub sp: Sp,
    pub exact: Vec<EntryInfo>,
}

impl LocalLog {
    
    pub fn received_op<T>(&mut self, op: Op, ti: &T) -> Result<LogEntryStrong, Error> where T: TimeInfo {
        let op_log = self.log.new_op(op, ti)?;
        Ok(op_log)
    }

    //pub fn received_external_sp(&mut self, sp: Sp) -> Result<LogEntryStrong, Error> {
        //self.log.check_sp_exact(sp, exact)
    //}
    
    pub fn create_local_sp<T>(&mut self, ti: &mut T) -> Result<(LogEntryStrong, Vec<EntryInfo>), Error> where T: TimeInfo {
        let mut late_included = vec![];
        let outer_sp = { // compute the outer_sp in the seperate view so we dont have a ref when we perform insert_outer_sp, which will need to borrow as mut
            let t = ti.get_largest_sp_time();
            let last_sp = self.last_sp.clone_strong();
            let last_sp_ptr = last_sp.get_ptr();
            let last_sp_ref = last_sp_ptr.borrow();
            
            let op_iter = last_sp_ref.entry.as_sp().get_ops_after_iter()?.filter_map(|op_rc| {
                let op_ptr = op_rc.get_ptr();
                let op_ref = op_ptr.borrow();
                let op = op_ref.entry.as_op();
                if op.op.op.info.time <= t {
                    if op.arrived_late {
                        late_included.push(op.op.get_entry_info());
                    }
                    return Some(op.op.hash) 
                }
                None
            });
            let sp = SpState::new(self.id, t, op_iter, vec![], last_sp_ref.entry.as_sp().sp.get_entry_info()).unwrap();
            self.log.check_sp(sp.sp, &late_included, &[], ti)?
        };
        let ret = self.log.insert_outer_sp(outer_sp)?;
        self.last_sp = ret.clone_strong(); // ::clone(&ret);
        Ok((ret, late_included))
    }
    
}

#[cfg(test)]
mod tests {
    use crate::{log::{basic_log::Log, log_file::LogFile, op::{Op, OpState, tests::{make_op_late}}}, verification::TimeTest};
    
    use super::new_local_log;
    
    fn get_log_file(idx: usize) -> LogFile {
        LogFile::open(&format!("local_log{}.log", idx), true).unwrap()
    }

    #[test]
    fn local_log_add_op() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(0));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, &mut ti);
        ll.received_op(op1, &ti).unwrap();
    }
    
    #[test]
    fn local_log_sp() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(1));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, &mut ti);
        let op_t = op1.info.time;
        ll.received_op(op1, &ti).unwrap();
        ti.sleep_op_until_late(op_t);
        let (_outer_sp, late_included) = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(0, late_included.len());
    }

    #[test]
    fn local_log_late_op() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(2));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = make_op_late(OpState::new(id, &mut ti).unwrap());
        let op_t = op1.op.info.time;
        let op1_ref = ll.received_op(op1.op, &ti).unwrap();
        assert!(!op1_ref.get_ptr().borrow().entry.as_op().include_in_hash);
        ti.sleep_op_until_late(op_t);
        let (_outer_sp, late_included) = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(1, late_included.len());
    }
}