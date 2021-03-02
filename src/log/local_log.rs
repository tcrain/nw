use bincode::Options;
use log::debug;

use crate::{
    errors::EncodeError,
    rw_buf::RWS,
    verification::{Id, TimeInfo},
};

use super::{
    basic_log::Log,
    log_error::{LogError, Result},
    op::{EntryInfo, Op, OpEntryInfo},
    sp::{Sp, SpInfo},
};

pub struct LocalLog<F: RWS> {
    id: Id,
    l: Log<F>,
    last_sp: EntryInfo,
}

pub fn new_local_log<F: RWS>(id: Id, mut l: Log<F>) -> Result<LocalLog<F>> {
    let last_sp = l
        .get_last_sp_id(id)
        .or_else(|_| l.get_initial_sp())?
        .ptr
        .borrow()
        .entry
        .get_entry_info();
    Ok(LocalLog { id, l, last_sp })
}

#[derive(Debug, Clone)]
pub struct SpToProcess {
    pub sp: SpCreated,
    pub info: SpInfo,
    pub not_included: Vec<EntryInfo>, // all operations with time less that the SP time that were not included
    pub late_included: Vec<EntryInfo>, // late operations that were included
}

#[derive(Debug)]
pub struct SpExactToProcess {
    pub sp: SpCreated,
    pub info: SpInfo,
    pub exact: Vec<EntryInfo>,
}

#[derive(Debug, Clone)]
pub struct SpCreated(Vec<u8>);

impl SpCreated {
    fn to_sp<O: Options>(&self, options: O) -> Result<Sp> {
        options
            .deserialize(&self.0)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))
    }
}

#[derive(Debug, Clone)]
pub struct OpCreated(Vec<u8>);

impl OpCreated {
    fn to_op<O: Options>(&self, options: O) -> Result<Op> {
        options
            .deserialize(&self.0)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))
    }
}

#[derive(Debug)]
pub struct OpResult {
    pub create: OpCreated,
    pub status: OpStatus,
    pub info: OpEntryInfo,
}

#[derive(Debug)]
pub struct OpStatus {
    pub include_in_hash: bool,
    pub arrived_late: bool,
}

impl OpStatus {
    fn new(include_in_hash: bool, arrived_late: bool) -> OpStatus {
        OpStatus {
            include_in_hash,
            arrived_late,
        }
    }
}

impl<F: RWS> LocalLog<F> {
    /// Inputs an operation in the log.
    /// Returns the hash of the operation.
    pub fn create_local_op<T>(&mut self, op: Op, ti: &T) -> Result<OpResult>
    where
        T: TimeInfo,
    {
        let op_log = self.l.insert_op(op, ti)?;
        let op_ptr = op_log.ptr.borrow();
        let serialized = op_ptr
            .entry
            .check_hash(self.l.f.serialize_option())
            .expect("serialization and hash was checked in call to new_op");
        let op = op_ptr.entry.as_op();
        Ok(OpResult {
            info: op_ptr.get_op_entry_info(),
            create: OpCreated(serialized),
            status: OpStatus::new(op.include_in_hash, op.arrived_late),
        })
    }

    #[inline(always)]
    pub fn my_id(&self) -> Id {
        self.id
    }

    /// Inpts an operation from an external node in the log.
    pub fn received_op<T>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult>
    where
        T: TimeInfo,
    {
        let op = op_c.to_op(self.l.f.serialize_option())?;
        let op_log = self.l.insert_op(op, ti)?;
        let op_ptr = op_log.ptr.borrow();
        let op_info = op_ptr.get_op_entry_info();
        let op = op_ptr.entry.as_op();
        Ok(OpResult {
            create: op_c,
            status: OpStatus::new(op.include_in_hash, op.arrived_late),
            info: op_info,
        })
    }

    pub fn received_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpToProcess,
    ) -> Result<(SpToProcess, Vec<OpEntryInfo>)> {
        let sp = sp_p.sp.to_sp(self.l.f.serialize_option())?;
        self.process_sp(ti, sp, sp_p.late_included, &sp_p.not_included)
    }

    pub fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpExactToProcess,
    ) -> Result<(SpExactToProcess, Vec<OpEntryInfo>)> {
        let sp = sp_p.sp.to_sp(self.l.f.serialize_option())?;
        self.process_sp_exact(ti, sp, sp_p.exact)
    }

    fn process_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp: Sp,
        exact: Vec<EntryInfo>,
    ) -> Result<(SpExactToProcess, Vec<OpEntryInfo>)> {
        let id = sp.info.id;
        let (outer_sp, ops) = self.l.check_sp_exact(sp, &exact, ti)?;
        let sp = self.l.insert_outer_sp(outer_sp)?;
        if sp.ptr.borrow().entry.get_entry_info().basic.id == self.id {
            // keep our own local last sp so we can consruct our next sp
            self.last_sp = sp.ptr.borrow().entry.get_entry_info();
        }
        let sp_ptr = sp.ptr.borrow();
        let serialized = sp_ptr
            .entry
            .check_hash(self.l.f.serialize_option())
            .expect("serialization and hash was checked in call to new_op");
        Ok((
            SpExactToProcess {
                sp: SpCreated(serialized),
                exact,
                info: SpInfo {
                    id,
                    log_index: sp_ptr.log_index,
                    supported_sp_log_index: sp_ptr.entry.as_sp().supported_sp_log_index,
                },
            },
            ops,
        ))
    }

    fn process_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp: Sp,
        late_included: Vec<EntryInfo>,
        not_included: &[EntryInfo],
    ) -> Result<(SpToProcess, Vec<OpEntryInfo>)> {
        let id = sp.info.id;
        let (outer_sp, ops) = self.l.check_sp(sp, &late_included, &not_included, ti)?;
        // println!("sp hash {:?}", outer_sp.sp.hash);
        let sp = self.l.insert_outer_sp(outer_sp)?;
        if id == self.id {
            // keep our own local last sp so we can consruct our next sp
            self.last_sp = sp.ptr.borrow().entry.get_entry_info();
        }
        let sp_ptr = sp.ptr.borrow();
        let serialized = sp_ptr
            .entry
            .check_hash(self.l.f.serialize_option())
            .expect("serialization and hash was checked in call to new_op");
        Ok((
            SpToProcess {
                sp: SpCreated(serialized),
                not_included: sp_ptr
                    .entry
                    .as_sp()
                    .not_included_ops
                    .iter()
                    .map(|op| op.get_ptr().borrow().entry.get_entry_info())
                    .collect(),
                late_included,
                info: SpInfo {
                    id,
                    log_index: sp_ptr.log_index,
                    supported_sp_log_index: sp_ptr.entry.as_sp().supported_sp_log_index,
                },
            },
            ops,
        ))
    }

    /// Creates a new SP for the local node with the largest time t, including all operations with time before t not already
    /// supported by a previous SP of this node. Any ops with time < t and that have arrived late are included as well.
    pub fn create_local_sp<T>(&mut self, ti: &mut T) -> Result<(SpToProcess, Vec<OpEntryInfo>)>
    where
        T: TimeInfo,
    {
        let mut late_included = vec![];
        let sp = {
            // compute the outer_sp in the seperate view so we dont have a ref when we perform insert_outer_sp, which will need to borrow as mut
            let t = ti.get_largest_sp_time();
            let last_sp = self.l.find_sp(self.last_sp, None)?;
            let last_sp_ref = last_sp.ptr.borrow();
            debug!(
                "create local sp time {}, last sp {}, {:?}",
                t,
                last_sp_ref.log_index,
                last_sp_ref.entry.get_entry_info()
            );
            let op_iter = last_sp_ref
                .entry
                .as_sp()
                .get_ops_after_iter(&mut self.l.m, &mut self.l.f)?
                .filter_map(|op_rc| {
                    let op_ref = op_rc.ptr.borrow();
                    let op = op_ref.entry.as_op();
                    debug!(
                        "add op to local sp: id {}, time {} add op time {} hash {:?}",
                        op.op.op.info.id, t, op.op.op.info.time, op.op.hash
                    );
                    if op.op.op.info.time <= t {
                        if op.arrived_late {
                            late_included.push(op.op.get_entry_info());
                        }
                        return Some(op.op.hash);
                    }
                    None
                });
            Sp::new(
                self.id,
                t,
                op_iter,
                vec![],
                last_sp_ref.entry.as_sp().sp.get_entry_info(),
            )
        };
        self.process_sp(ti, sp, late_included, &[])
    }
}

pub mod test_setup {

    use std::fs::File;

    use rand::prelude::{SliceRandom, StdRng};

    use crate::log::basic_log::test_fns::check_sp_prev;
    use crate::{
        log::{
            basic_log::Log,
            log_file::open_log_file,
            log_file::LogFile,
            op::Op,
            op::{gen_rand_data, OpEntryInfo},
        },
        rw_buf::RWS,
        verification::{Id, TimeTest},
    };

    use super::{new_local_log, LocalLog, OpCreated, OpResult, SpToProcess};

    pub struct LogTest<F: RWS> {
        ll: LocalLog<F>,
        ti: TimeTest,
        id: Id,
    }

    pub fn get_log_file<F: RWS, G: Fn(File) -> F>(idx: usize, open_fn: G) -> LogFile<F> {
        open_log_file(&format!("log_files/local_log{}.log", idx), true, open_fn).unwrap()
    }

    pub fn new_log_test<F: RWS, G: Fn(File) -> F>(id: Id, open_fn: G) -> LogTest<F> {
        let ti = TimeTest::new();
        let l = Log::new(get_log_file(3 + id as usize, open_fn));
        let ll = new_local_log(id, l).unwrap();
        LogTest { ll, ti, id }
    }

    impl<F: RWS> LogTest<F> {
        pub fn create_op(&mut self) -> OpResult {
            let op1 = Op::new(self.id, gen_rand_data(), &mut self.ti);
            self.ll.create_local_op(op1, &self.ti).unwrap()
        }

        pub fn create_sp(&mut self) -> (SpToProcess, Vec<OpEntryInfo>) {
            self.ti.set_current_time_valid();
            let ret = self.ll.create_local_sp(&mut self.ti).unwrap();
            check_sp_prev(&mut self.ll.l, true);
            ret
        }

        pub fn got_op(&mut self, op_c: OpCreated) -> OpResult {
            self.ll.received_op(op_c, &self.ti).unwrap()
        }

        pub fn got_sp(&mut self, sp_p: SpToProcess) -> (SpToProcess, Vec<OpEntryInfo>) {
            let ret = self.ll.received_sp(&mut self.ti, sp_p).unwrap();
            check_sp_prev(&mut self.ll.l, true);
            ret
        }
    }

    pub fn add_ops_rand_order<F: RWS>(
        logs: &mut [LogTest<F>],
        rng: &mut StdRng,
        num_iterations: u64,
    ) {
        let num_logs = logs.len();
        let mut new_ops = vec![];
        for (i, l) in logs.iter_mut().enumerate() {
            let op_result = l.create_op();
            let mut order: Vec<usize> = (0..num_logs as usize).collect();
            order.remove(i as usize); // since this instance already has the op
            order.shuffle(rng); // we want to send each op to logs in a random order
            new_ops.push((i, op_result.create, order));
        }
        while let Some((idx, mut op, mut order)) = new_ops.pop() {
            if let Some(i) = order.pop() {
                // println!("add op {} to {}", i, idx);
                op = logs.get_mut(i).unwrap().got_op(op).create;
                new_ops.push((idx, op, order));
            }
        }
        for l in logs.iter_mut() {
            debug_assert_eq!(
                num_logs * num_iterations as usize,
                l.ll.l.op_total_order_iterator_from_last().count()
            );
            debug_assert_eq!(
                num_logs * num_iterations as usize,
                l.ll.l.op_total_order_iterator_from_first().count()
            );
        }
    }

    pub fn add_sps<F: RWS>(logs: &mut [LogTest<F>], rng: &mut StdRng) {
        let num_logs = logs.len();
        let mut new_sps = vec![];
        for (i, l) in logs.iter_mut().enumerate() {
            let (sp, _) = l.create_sp();
            let mut order: Vec<usize> = (0..num_logs as usize).collect();
            order.remove(i as usize); // since this instance already has the op
            order.shuffle(rng); // we want to send each sp to logs in a random order
            new_sps.push((sp, order));
        }
        while let Some((sp, mut order)) = new_sps.pop() {
            if let Some(i) = order.pop() {
                let l = logs.get_mut(i).unwrap();
                let (sp, _) = l.got_sp(sp);
                new_sps.push((sp, order));
            }
        }
    }
}

#[cfg(test)]
pub mod tests {

    use crate::{
        log::{
            basic_log::Log,
            op::{gen_rand_data, tests::make_op_late, Op, OpState},
        },
        rw_buf::RWBuf,
        verification::TimeTest,
    };

    use super::{
        new_local_log,
        test_setup::{add_ops_rand_order, add_sps, get_log_file, new_log_test},
    };
    use rand::rngs::StdRng;
    use rand::SeedableRng;

    #[test]
    fn local_log_add_op() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(0, RWBuf::new));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, gen_rand_data(), &mut ti);
        ll.create_local_op(op1, &ti).unwrap();
    }

    #[test]
    fn local_log_sp() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(1, RWBuf::new));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, gen_rand_data(), &mut ti);
        let op_t = op1.info.time;
        ll.create_local_op(op1, &ti).unwrap();
        ti.sleep_op_until_late(op_t);
        let (sp_process, _) = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(0, sp_process.not_included.len());
    }

    #[test]
    fn local_log_late_op() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(get_log_file(2, RWBuf::new));
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = make_op_late(
            OpState::new(id, gen_rand_data(), &mut ti, ll.l.f.serialize_option()).unwrap(),
            ll.l.f.serialize_option(),
        );
        let op_t = op1.op.info.time;
        let op_result = ll.create_local_op(op1.op, &ti).unwrap();
        assert!(!op_result.status.include_in_hash);
        ti.sleep_op_until_late(op_t);
        let (sp_process, _) = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(1, sp_process.late_included.len());
        assert_eq!(0, sp_process.not_included.len());
    }

    #[test]
    fn transfer_op() {
        let num_logs = 5;
        let mut logs = vec![];
        for i in 0..num_logs {
            logs.push(new_log_test(100 + i, RWBuf::new))
        }
        let num_ops = 5;
        for i in 0..num_ops {
            let l = logs.get_mut((i % num_logs) as usize).unwrap();
            let mut op_result = l.create_op();
            for j in i + 1..i + num_logs {
                op_result = logs
                    .get_mut((j % num_logs) as usize)
                    .unwrap()
                    .got_op(op_result.create);
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
            logs.push(new_log_test(200 + i, RWBuf::new))
        }
        for i in 1..3 {
            // 1 op per log
            add_ops_rand_order(&mut logs, &mut rng, i);
            // 1 sp per log
            add_sps(&mut logs, &mut rng);
        }
    }
}
