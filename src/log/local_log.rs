use std::fmt::Debug;

use bincode::{DefaultOptions, Options};
use log::debug;

use crate::{
    errors::EncodeError,
    rw_buf::RWS,
    verification::{hash, Id, TimeInfo},
};

use super::{
    basic_log::Log,
    entry::{LogEntry, LogEntryStrong, LogEntryWeak, OuterOp, StrongPtrIdx},
    log_error::{LogError, Result},
    op::{EntryInfo, Op, OpEntryInfo, OpState},
    sp::{Sp, SpInfo, SpState},
};

pub struct LocalLog<F: RWS, AppendF: RWS> {
    id: Id,
    pub(crate) l: Log<F, AppendF>,
    last_sp: EntryInfo,
}

pub fn new_local_log<F: RWS, AppendF: RWS>(
    id: Id,
    mut l: Log<F, AppendF>,
) -> Result<LocalLog<F, AppendF>> {
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
pub struct SpDetails {
    pub ops: Vec<OpEntryInfo>,
    pub info: SpInfo,
}

#[derive(Debug, Clone)]
pub struct SpOps {
    pub not_included: Vec<EntryInfo>, // all operations with time less that the SP time that were not included
    pub late_included: Vec<EntryInfo>, // late operations that were included
}

/// This is created to be sent to other nodes to process an Sp normally.
#[derive(Debug, Clone)]
pub struct SpToProcess {
    pub sp: Sp,
    pub ops: SpOps,
}

/// This is calculated from SpToProcess at the local node to process the received Sp.
#[derive(Debug, Clone)]
pub struct SpStateToProcess {
    pub sp: SpState,
    pub ops: SpOps,
}

impl SpStateToProcess {
    pub fn from_sp<O: Options>(sp: SpToProcess, o: O) -> Result<Self> {
        Ok(Self {
            sp: SpState::from_sp(sp.sp, o)?,
            ops: sp.ops,
        })
    }
}

/// This is created to be sent to other nodes to process an Sp by the exact set of operations it contains.
#[derive(Debug, Clone)]
pub struct SpExactToProcess {
    pub sp: Sp,
    pub exact: Vec<Op>,
}

/// This is calculated from SpExactToProcess at the local node to process the received Sp.
#[derive(Debug, Clone)]
pub struct SpStateExactToProcess {
    pub sp: SpState,
    pub exact: Vec<Op>,
}

impl SpStateExactToProcess {
    pub fn from_sp<O: Options>(sp: SpExactToProcess, o: O) -> Result<Self> {
        Ok(Self {
            sp: SpState::from_sp(sp.sp, o)?,
            exact: sp.exact,
        })
    }
}

#[derive(Clone)]
pub struct SpCreated(Vec<u8>);

impl Debug for SpCreated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.0[..4])
    }
}

impl SpCreated {
    pub fn to_sp<O: Options>(&self, options: O) -> Result<Sp> {
        options
            .deserialize(&self.0)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))
    }

    pub fn to_entry_info<O: Options>(&self, options: O) -> Result<EntryInfo> {
        let sp = self.to_sp(options)?;
        Ok(EntryInfo {
            basic: sp.info,
            hash: hash(&self.0),
        })
    }
}

impl From<SpCreated> for Vec<u8> {
    fn from(sp: SpCreated) -> Self {
        sp.0
    }
}

#[derive(Clone, PartialEq, Eq)]
pub struct OpCreated(Vec<u8>);

impl Debug for OpCreated {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", &self.0[..4])
    }
}

impl OpCreated {
    fn to_op<O: Options>(&self, options: O) -> Result<Op> {
        options
            .deserialize(&self.0)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))
    }

    fn from_op<O: Options>(op: &Op, options: O) -> Result<OpCreated> {
        let ser = options
            .serialize(op)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))?;
        Ok(OpCreated(ser))
    }

    fn to_op_state<O: Options>(&self, options: O) -> Result<OpState> {
        Ok(OpState {
            hash: hash(&self.0),
            op: self.to_op(options)?,
        })
    }

    fn to_entry_info<O: Options>(&self, options: O) -> Result<EntryInfo> {
        let op = self.to_op(options)?;
        Ok(EntryInfo {
            basic: op.info,
            hash: hash(&self.0),
        })
    }
}

#[derive(Debug, Clone)]
pub struct OpResult {
    pub create: OpCreated,
    pub status: OpStatus,
    pub info: OpEntryInfo,
}

#[derive(Debug, Clone)]
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

fn to_op_entry_info(ops: &[StrongPtrIdx]) -> Vec<OpEntryInfo> {
    let mut ser = vec![];
    for op in ops {
        ser.push(op.ptr.borrow().get_op_entry_info())
    }
    ser
}

impl<F: RWS, AppendF: RWS> LocalLog<F, AppendF> {
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
            .check_hash(self.l.serialize_option())
            .expect("serialization and hash was checked in call to new_op");
        let op = op_ptr.entry.as_op();
        Ok(OpResult {
            info: op_ptr.get_op_entry_info(),
            create: OpCreated(serialized),
            status: OpStatus::new(op.include_in_hash, op.arrived_late),
        })
    }

    #[inline(always)]
    pub fn serialize_option(&self) -> DefaultOptions {
        self.l.serialize_option()
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
        let op = op_c.to_op(self.l.serialize_option())?;
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
        sp_p: &SpStateToProcess,
    ) -> Result<(SpToProcess, SpDetails)> {
        //  let sp = sp_p.sp.to_sp(self.l.serialize_option())?;
        self.process_sp(
            ti,
            &sp_p.sp,
            &sp_p.ops.late_included,
            &sp_p.ops.not_included,
        )
    }

    pub fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: &SpStateExactToProcess,
    ) -> Result<(Vec<OpEntryInfo>, SpToProcess, SpDetails)> {
        // let sp = &sp_p.sp; //.to_sp(self.l.serialize_option())?;
        let mut new_ops = vec![];
        let mut all_ops = vec![];
        for op in sp_p.exact.iter() {
            match self.l.insert_op(op.clone(), ti) {
                Err(e) => match e {
                    LogError::OpAlreadyExists => {
                        // the op already exists in the log
                        all_ops.push(op.to_entry_info(self.serialize_option())?);
                    }
                    _ => return Err(e),
                },
                Ok(op_ptr) => {
                    let op_ptr = op_ptr.ptr.borrow();
                    let op = op_ptr.entry.as_op();
                    all_ops.push(op.into());
                    new_ops.push(op.into())
                }
            }
        }
        let (sp_p, sp_d) = self.process_sp_exact(ti, &sp_p.sp, &all_ops)?;
        Ok((new_ops, sp_p, sp_d))
    }

    pub fn get_sp_exact(&mut self, sp: Sp) -> Result<SpExactToProcess> {
        let sp_s = SpState::from_sp(sp, self.l.serialize_option())?;
        let (o_sp, ops) = self.l.get_sp_exact(sp_s.get_entry_info())?;
        let ops = ops
            .iter()
            .map(|ptr| ptr.ptr.borrow().entry.as_op().op.op.clone())
            .collect();
        Ok(SpExactToProcess {
            exact: ops,
            sp: o_sp.sp.sp,
        })
    }

    fn get_sp_to_process(&self, sp_ptr: &LogEntry) -> SpToProcess {
        // let sp_ptr = sp.ptr.borrow();
        let state = self.l.get_log_state();
        let new_sp = sp_ptr.entry.as_sp();
        let not_included = new_sp
            .not_included_ops
            .iter()
            .map(|op| op.get_ptr(state).borrow().entry.get_entry_info())
            .collect();
        let late_included = new_sp
            .late_included
            .iter()
            .map(|op| op.get_ptr(state).borrow().entry.get_entry_info())
            .collect();
        SpToProcess {
            ops: SpOps {
                not_included,
                late_included,
            },
            sp: sp_ptr.entry.as_sp().sp.sp.clone(),
        }
    }

    fn process_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp: &SpState,
        exact: &[EntryInfo],
    ) -> Result<(SpToProcess, SpDetails)> {
        let id = sp.sp.info.id;
        debug!("\nprocess exact sp: {:?}, ops: {:?}\n", sp, exact);
        let (outer_sp, ops) = self.l.check_sp_exact(sp, exact, ti)?;
        let sp = self.l.insert_outer_sp(outer_sp)?;
        if sp.ptr.borrow().entry.get_entry_info().basic.id == self.id {
            // keep our own local last sp so we can consruct our next sp
            self.last_sp = sp.ptr.borrow().entry.get_entry_info();
        }
        let sp_ptr = sp.ptr.borrow();
        Ok((
            self.get_sp_to_process(&sp_ptr),
            SpDetails {
                info: SpInfo {
                    id,
                    log_index: sp_ptr.log_index,
                    supported_sp_log_index: sp_ptr.entry.as_sp().supported_sp_log_index,
                },
                ops: to_op_entry_info(&ops),
            },
        ))
    }

    fn process_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp: &SpState,
        late_included: &[EntryInfo],
        not_included: &[EntryInfo],
    ) -> Result<(SpToProcess, SpDetails)> {
        let id = sp.sp.info.id;
        let (outer_sp, ops) = self.l.check_sp(sp, &late_included, &not_included, ti)?;
        let sp = self.l.insert_outer_sp(outer_sp)?;
        if id == self.id {
            // keep our own local last sp so we can consruct our next sp
            self.last_sp = sp.ptr.borrow().entry.get_entry_info();
        }
        let sp_ptr = sp.ptr.borrow();

        Ok((
            self.get_sp_to_process(&sp_ptr),
            SpDetails {
                info: SpInfo {
                    id,
                    log_index: sp_ptr.log_index,
                    supported_sp_log_index: sp_ptr.entry.as_sp().supported_sp_log_index,
                },
                ops: to_op_entry_info(&ops),
            },
        ))
    }

    /// Creates a new SP for the local node with the largest time t, including all operations with time before t not already
    /// supported by a previous SP of this node. Any ops with time < t and that have arrived late are included as well.
    pub fn create_local_sp<T>(&mut self, ti: &mut T) -> Result<(SpToProcess, SpDetails)>
    where
        T: TimeInfo,
    {
        let mut late_included = vec![];
        let sp = {
            // compute the outer_sp in the seperate view so we dont have a ref when we perform insert_outer_sp, which will need to borrow as mut
            let t = ti.get_largest_sp_time();

            let (last_sp, last_op, sp_entry_info): (LogEntryWeak, _, _) = {
                let last_sp = self.l.find_sp(self.last_sp, None)?;
                let last_sp_ref = last_sp.ptr.borrow();
                let last_sp_entry = last_sp_ref.entry.as_sp();
                debug!(
                    "\n\ncreate local sp time {}, last sp {}, {:?}\n",
                    t,
                    last_sp_ref.log_index,
                    last_sp_ref.entry.get_entry_info(),
                );
                (
                    (&last_sp).into(),
                    last_sp_entry.last_op.clone(),
                    last_sp_ref.entry.get_entry_info(),
                )
            };
            // collect the operations later in the log that have time earlier in than last op
            // we will add these specifically as they would not be included normally as they
            // arrived late
            let after_ops = {
                // last_sp_entry.last_op is only None if this is the initial SP
                // in this case after_ops will be None the call to get_ops_after_iter
                // will return all ops afterwards in the log
                last_op.as_ref().map(|last_op| {
                    let mut last_strong: LogEntryStrong = last_op.into();
                    let largest_op = last_strong
                        .get_ptr(self.l.get_log_state())
                        .borrow()
                        .entry
                        .get_entry_info();
                    let mut iter = self.l.op_iterator_from(last_strong, true);
                    // move forward one op so we dont include the last_op
                    iter.next();
                    // take the entries that arrive late (i.e. are after sp.last_op in the log,
                    // and have smaller total order time)
                    let mut late_items: Vec<OuterOp> = iter
                        .filter_map(move |nxt_op| {
                            let nxt_op_ptr = nxt_op.ptr.borrow();
                            if nxt_op_ptr.entry.get_entry_info() < largest_op
                                && nxt_op_ptr.entry.as_op().arrived_late
                            {
                                Some(nxt_op_ptr.entry.as_op().clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    // they must be sorted since they will be merged with the other iterators
                    late_items.sort_by(|l, r| l.op.cmp(&r.op));
                    late_items
                })
            };
            let last_sp = last_sp.get_ptr(self.l.get_log_state());
            let last_sp_ref = last_sp.borrow();
            let op_iter = last_sp_ref
                .entry
                .as_sp()
                .get_ops_after_iter(after_ops, self.l.get_first_op(), self.l.get_log_state())?
                .filter_map(|op| {
                    if op.op.op.info.time <= t {
                        if op.arrived_late {
                            late_included.push((&op.op).into());
                        }
                        return Some(op.op.hash);
                    }
                    None
                });
            SpState::from_sp(
                Sp::new(self.id, t, op_iter, vec![], sp_entry_info),
                self.serialize_option(),
            )
            .expect("should be able to serialize own sp")
        };
        self.process_sp(ti, &sp, &late_included, &[])
    }
}

pub mod test_setup {

    use std::fs::File;

    use rand::prelude::{SliceRandom, StdRng};

    #[cfg(debug_assertions)]
    use crate::log::basic_log::test_fns::check_sp_prev;

    use crate::{
        log::{
            basic_log::Log, log_file::open_log_file, log_file::LogFile, op::gen_rand_data, op::Op,
        },
        rw_buf::RWS,
        verification::{Id, TimeTest},
    };

    use super::{
        new_local_log, LocalLog, OpCreated, OpResult, SpDetails, SpStateToProcess, SpToProcess,
    };

    pub struct LogTest<F: RWS, AppendF: RWS> {
        ll: LocalLog<F, AppendF>,
        ti: TimeTest,
        id: Id,
    }

    pub fn get_log_file<F: RWS, G: Fn(File) -> F>(
        idx: usize,
        append: bool,
        open_fn: G,
    ) -> LogFile<F> {
        let file_path = match append {
            true => format!("log_files/local_append_log{}.log", idx),
            false => format!("log_files/local_log{}.log", idx),
        };
        open_log_file(&file_path, true, append, open_fn).unwrap()
    }

    pub fn new_log_test<F: RWS, AppendF: RWS, G: Fn(File) -> F, AppendG: Fn(File) -> AppendF>(
        id: Id,
        open_fn: G,
        open_append_fn: AppendG,
    ) -> LogTest<F, AppendF> {
        let ti = TimeTest::new();
        let l = Log::new(
            get_log_file(3 + id as usize, false, open_fn),
            get_log_file(3 + id as usize, true, open_append_fn),
        );
        let ll = new_local_log(id, l).unwrap();
        LogTest { ll, ti, id }
    }

    impl<F: RWS, AppendF: RWS> LogTest<F, AppendF> {
        pub fn create_op(&mut self) -> OpResult {
            let op1 = Op::new(self.id, gen_rand_data(), &mut self.ti);
            self.ll.create_local_op(op1, &self.ti).unwrap()
        }

        pub fn create_sp(&mut self) -> (SpToProcess, SpDetails) {
            self.ti.set_current_time_valid();
            let ret = self.ll.create_local_sp(&mut self.ti).unwrap();
            #[cfg(debug_assertions)]
            check_sp_prev(&mut self.ll.l, true);
            ret
        }

        pub fn got_op(&mut self, op_c: OpCreated) -> OpResult {
            self.ll.received_op(op_c, &self.ti).unwrap()
        }

        pub fn got_sp(&mut self, sp_p: SpToProcess) -> (SpToProcess, SpDetails) {
            let sp_p = SpStateToProcess::from_sp(sp_p, self.ll.l.serialize_option()).unwrap();
            let ret = self.ll.received_sp(&mut self.ti, &sp_p).unwrap();
            #[cfg(debug_assertions)]
            check_sp_prev(&mut self.ll.l, true);
            ret
        }
    }

    pub fn add_ops_rand_order<F: RWS, AppendF: RWS>(
        logs: &mut [LogTest<F, AppendF>],
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

    pub fn add_sps<F: RWS, AppendF: RWS>(logs: &mut [LogTest<F, AppendF>], rng: &mut StdRng) {
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
        let l = Log::new(
            get_log_file(0, false, RWBuf::new),
            get_log_file(0, true, RWBuf::new),
        );
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, gen_rand_data(), &mut ti);
        ll.create_local_op(op1, &ti).unwrap();
    }

    #[test]
    fn local_log_sp() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(
            get_log_file(1, false, RWBuf::new),
            get_log_file(1, true, RWBuf::new),
        );
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = Op::new(id, gen_rand_data(), &mut ti);
        let op_t = op1.info.time;
        ll.create_local_op(op1, &ti).unwrap();
        ti.sleep_op_until_late(op_t);
        let (sp_process, _) = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(0, sp_process.ops.not_included.len());
    }

    #[test]
    fn local_log_late_op() {
        let mut ti = TimeTest::new();
        let id = 0;
        let l = Log::new(
            get_log_file(2, false, RWBuf::new),
            get_log_file(2, true, RWBuf::new),
        );
        let mut ll = new_local_log(id, l).unwrap();
        let op1 = make_op_late(
            OpState::new(id, gen_rand_data(), &mut ti, ll.l.serialize_option()).unwrap(),
            ll.l.serialize_option(),
        );
        let op_t = op1.op.info.time;
        let op_result = ll.create_local_op(op1.op, &ti).unwrap();
        assert!(!op_result.status.include_in_hash);
        ti.sleep_op_until_late(op_t);
        let (sp_process, _) = ll.create_local_sp(&mut ti).unwrap();
        assert_eq!(1, sp_process.ops.late_included.len());
        assert_eq!(0, sp_process.ops.not_included.len());
    }

    #[test]
    fn transfer_op() {
        let num_logs = 5;
        let mut logs = vec![];
        for i in 0..num_logs {
            logs.push(new_log_test(100 + i, RWBuf::new, RWBuf::new))
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
            logs.push(new_log_test(200 + i, RWBuf::new, RWBuf::new))
        }
        for i in 1..3 {
            // 1 op per log
            add_ops_rand_order(&mut logs, &mut rng, i);
            // 1 sp per log
            add_sps(&mut logs, &mut rng);
        }
    }
}
