use bincode::Options;

use crate::{errors::{Error, LogError}, verification::{Id, TimeInfo}};

use super::{basic_log::{Log}, op::{EntryInfo, Op}, sp::{Sp}};

pub struct LocalLog {
    id: Id,
    l: Log,
    last_sp: EntryInfo,
}

pub fn new_local_log(id: Id, mut l: Log) -> Result<LocalLog, Error> {
    let last_sp = l.get_last_sp_id(id).or_else(|_| {
        l.get_initial_sp()
    })?.ptr.borrow().entry.get_entry_info();
    Ok(LocalLog {
        id,
        l,
        last_sp,
    })
}

pub struct SpToProcess {
    pub sp: SpCreated,
    pub not_included: Vec<EntryInfo>, // all operations with time less that the SP time that were not included
    pub late_included: Vec<EntryInfo>, // late operations that were included
}

pub struct SpExactToProcess {
    pub sp: SpCreated,
    pub exact: Vec<EntryInfo>,
}

pub struct SpCreated(Vec<u8>);

impl SpCreated {
    fn to_sp<O: Options>(&self, options: O) -> Result<Sp, Error> {
        options.deserialize(&self.0).map_err(|_err| Error::LogError(LogError::DeserializeError))
    }
}

pub struct OpCreated(Vec<u8>);

impl OpCreated {
    fn to_op<O: Options>(&self, options: O) -> Result<Op, Error> {
        options.deserialize(&self.0).map_err(|_err| Error::LogError(LogError::DeserializeError))
    }
}

pub struct OpStatus {
    pub include_in_hash: bool,
    pub arrived_late: bool,
}

impl OpStatus {
    fn new(include_in_hash: bool, arrived_late: bool) -> OpStatus {
        OpStatus{
            include_in_hash,
            arrived_late,
        }
    }
}

impl LocalLog {
    
    /// Inputs an operation in the log.
    /// Returns the hash of the operation.
    pub fn create_local_op<T>(&mut self, op: Op, ti: &T) -> Result<(OpCreated, OpStatus), Error> where T: TimeInfo {
        let op_log = self.l.insert_op(op, ti)?;
        let op_ptr = op_log.ptr.borrow();
        let serialized = op_ptr.entry.check_hash(self.l.f.serialize_option()).expect("serialization and hash was checked in call to new_op");
        let op = op_ptr.entry.as_op();
        Ok((OpCreated(serialized), OpStatus::new(op.include_in_hash, op.arrived_late)))
    }
    
    /// Inpts an operation from an external node in the log.
    pub fn received_op<T>(&mut self, op_c: OpCreated, ti: &T) -> Result<(OpCreated, OpStatus), Error> where T: TimeInfo {
        let op = op_c.to_op(self.l.f.serialize_option())?;
        let op_log = self.l.insert_op(op, ti)?;
        let op_ptr = op_log.ptr.borrow();
        let op = op_ptr.entry.as_op();
        Ok((op_c, OpStatus::new(op.include_in_hash, op.arrived_late)))
    }
    
    pub fn received_sp<T: TimeInfo>(&mut self, ti: &mut T, sp_p: SpToProcess) -> Result<SpToProcess, Error> {
        let sp = sp_p.sp.to_sp(self.l.f.serialize_option())?;
        self.process_sp(ti, sp, sp_p.late_included, &sp_p.not_included)
    }
    
    pub fn received_sp_exact<T: TimeInfo>(&mut self, ti: &mut T, sp_p: SpExactToProcess) -> Result<SpExactToProcess, Error> {
        let sp = sp_p.sp.to_sp(self.l.f.serialize_option())?;
        self.process_sp_exact(ti, sp, sp_p.exact)
    }
    
    fn process_sp_exact<T: TimeInfo>(&mut self, ti: &mut T, sp: Sp, exact: Vec<EntryInfo>) -> Result<SpExactToProcess, Error> {
        let outer_sp = self.l.check_sp_exact(sp, &exact, ti)?;
        let sp = self.l.insert_outer_sp(outer_sp)?;
        if sp.ptr.borrow().entry.get_entry_info().basic.id == self.id { // keep our own local last sp so we can consruct our next sp
            self.last_sp = sp.ptr.borrow().entry.get_entry_info();
        }
        let sp_ptr = sp.ptr.borrow();
        let serialized = sp_ptr.entry.check_hash(self.l.f.serialize_option()).expect("serialization and hash was checked in call to new_op");
        Ok(SpExactToProcess{
            sp: SpCreated(serialized),
            exact,
        })
    }
    
    fn process_sp<T: TimeInfo>(&mut self, ti: &mut T, sp: Sp, late_included: Vec<EntryInfo>, not_included: &[EntryInfo]) -> Result<SpToProcess, Error> {
        let outer_sp = self.l.check_sp(sp, &late_included, &not_included, ti)?;
        println!("sp hash {:?}", outer_sp.sp.hash);
        let sp = self.l.insert_outer_sp(outer_sp)?;
        if sp.ptr.borrow().entry.get_entry_info().basic.id == self.id { // keep our own local last sp so we can consruct our next sp
            self.last_sp = sp.ptr.borrow().entry.get_entry_info();
        }
        let sp_ptr = sp.ptr.borrow();
        let serialized = sp_ptr.entry.check_hash(self.l.f.serialize_option()).expect("serialization and hash was checked in call to new_op");
        Ok(SpToProcess{
            sp: SpCreated(serialized),
            not_included: sp_ptr.entry.as_sp().not_included_ops.iter().map(|op| op.get_ptr().borrow().entry.get_entry_info()).collect(),
            late_included,
        })
    }
    
    /// Creates a new SP for the local node with the largest time t, including all operations with time before t not already
    /// supported by a previous SP of this node. Any ops with time < t and that have arrived late are included as well.
    pub fn create_local_sp<T>(&mut self, ti: &mut T) -> Result<SpToProcess, Error> where T: TimeInfo {
        let mut late_included = vec![];
        let sp = { // compute the outer_sp in the seperate view so we dont have a ref when we perform insert_outer_sp, which will need to borrow as mut
            let t = ti.get_largest_sp_time();
            println!("create sp at time {}", t);
            let last_sp = self.l.find_sp(self.last_sp)?;
            let last_sp_ref = last_sp.ptr.borrow();
            println!("last sp {}, {:?}", last_sp_ref.log_index, last_sp_ref.entry.get_entry_info());
            let id = self.id;
            let op_iter = last_sp_ref.entry.as_sp().get_ops_after_iter(&mut self.l.m, &mut self.l.f)?.filter_map(|op_rc| {
                let op_ref = op_rc.ptr.borrow();
                let op = op_ref.entry.as_op();
                println!("id {}, time {} add op time {} hash {:?}", id, t, op.op.op.info.time, op.op.hash);
                if op.op.op.info.time <= t {
                    if op.arrived_late {
                        late_included.push(op.op.get_entry_info());
                    }
                    return Some(op.op.hash) 
                }
                None
            });
            Sp::new(self.id, t, op_iter, vec![], last_sp_ref.entry.as_sp().sp.get_entry_info())
        };
        self.process_sp(ti, sp, late_included, &[])
    }
    
}

#[cfg(test)]
mod tests {

    use crate::{log::{basic_log::Log, log_file::LogFile, op::{Op, OpState, tests::{make_op_late}}}, verification::{Id, TimeTest}};
    
    use super::{LocalLog, OpCreated, OpStatus, SpToProcess, new_local_log};
    use rand::{SeedableRng, seq::SliceRandom};
    use rand::rngs::StdRng;
    
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
        ll.create_local_op(op1, &ti).unwrap();
    }
    
    #[test]
    fn local_log_sp() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(1));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, &mut ti);
        let op_t = op1.info.time;
        ll.create_local_op(op1, &ti).unwrap();
        ti.sleep_op_until_late(op_t);
        let sp_process = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(0, sp_process.not_included.len());
    }
    
    #[test]
    fn local_log_late_op() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(2));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = make_op_late(OpState::new(id, &mut ti, ll.l.f.serialize_option()).unwrap(), ll.l.f.serialize_option());
        let op_t = op1.op.info.time;
        let (_, op_s) = ll.create_local_op(op1.op, &ti).unwrap();
        assert!(!op_s.include_in_hash);
        ti.sleep_op_until_late(op_t);
        let sp_process = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(1, sp_process.late_included.len());
        assert_eq!(0, sp_process.not_included.len());
    }
    
    struct LogTest {
        ll: LocalLog,
        ti: TimeTest,
        id: Id,    
    }
    
    impl LogTest {
        fn new(id: Id) -> LogTest {
            let ti = TimeTest::new();
            let l = Log::new(get_log_file(3 + id as usize));
            let ll = new_local_log(id, l).unwrap();
            LogTest{
                ll,
                ti,
                id,
            }
        }
        
        fn create_op(&mut self) -> (OpCreated, OpStatus) {
            let op1 = Op::new(self.id, &mut self.ti);
            self.ll.create_local_op(op1, &self.ti).unwrap()    
        }
        
        fn create_sp(&mut self) -> SpToProcess {
            self.ti.set_current_time_valid();
            self.ll.create_local_sp(&mut self.ti).unwrap()
        }
        
        fn got_op(&mut self, op_c: OpCreated) -> (OpCreated, OpStatus) {
            self.ll.received_op(op_c, &self.ti).unwrap()
        }
        
        fn got_sp(&mut self, sp_p: SpToProcess) -> SpToProcess {
            self.ll.received_sp(&mut self.ti, sp_p).unwrap()
        }
    }
    
    #[test]
    fn transfer_op() {
        let num_logs = 5;
        let mut logs = vec![];
        for i in 0..num_logs {
            logs.push(LogTest::new(100+ i))
        }
        let num_ops = 5;
        for i in 0..num_ops {
            let l = logs.get_mut((i % num_logs) as usize).unwrap();
            let (mut op, _) = l.create_op();
            for j in i+1..i+num_logs {
                op = logs.get_mut((j % num_logs) as usize).unwrap().got_op(op).0;
            }
        }
    }
    
    #[test]
    fn transfer_op_order() {
        // this test inserts one op per participant in a random order at each log
        let mut rng = StdRng::seed_from_u64(100);
        let num_logs = 5;
        let mut logs = vec![];
        for i in 0..num_logs {
            logs.push(LogTest::new(200 + i))
        }
        { // 1 op per log
            // let num_ops = 5;
            let mut new_ops = vec![];
            for (i, l) in logs.iter_mut().enumerate() {
                let (op, _) = l.create_op();
                let mut order: Vec<usize> = (0..num_logs as usize).collect();
                order.remove(i as usize); // since this instance already has the op
                order.shuffle(&mut rng); // we want to send each op to logs in a random order
                new_ops.push((i, op, order));
            }
            while let Some((idx, mut op, mut order)) = new_ops.pop() {
                if let Some(i) = order.pop() {
                    println!("add op {} to {}", i, idx);
                    op = logs.get_mut(i).unwrap().got_op(op).0;
                    new_ops.push((idx, op, order));
                }
            }
            for l in logs.iter_mut() {
                assert_eq!(num_logs as usize, l.ll.l.op_total_order_iterator_from_last().count());
                assert_eq!(num_logs as usize, l.ll.l.op_total_order_iterator_from_first().count());
            }
        }
        { // 1 sp per log
            let mut new_sps = vec![];
            for (i, l) in logs.iter_mut().enumerate() {
                let sp = l.create_sp();
                let mut order: Vec<usize> = (0..num_logs as usize).collect();
                order.remove(i as usize); // since this instance already has the op
                order.shuffle(&mut rng); // we want to send each sp to logs in a random order
                new_sps.push((sp, order));
            }
            while let Some((mut sp, mut order)) = new_sps.pop() {
                if let Some(i) = order.pop() {
                    let l = logs.get_mut(i).unwrap();
                    sp = l.got_sp(sp);
                    new_sps.push((sp, order));
                }
            }
        }
    }
}