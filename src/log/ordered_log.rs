use std::{
    collections::BTreeSet,
    error::Error,
    fmt::{Debug, Display},
    iter::repeat,
    result,
};

use bincode::DefaultOptions;
use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    rw_buf::RWS,
    verification::{Id, TimeInfo},
};

use super::{
    basic_log::Log,
    local_log::{LocalLog, OpCreated, OpResult, SpDetails, SpExactToProcess, SpToProcess},
    log_error::LogError,
    op::{Op, OpData, OpEntryInfo},
    sp::{Sp, SpInfo},
    LogIdx,
};

#[derive(Debug)]
pub enum OrderingError {
    Custom(Box<dyn Error>), // An error related to the ordering.
    LogError(LogError),     // An error from the underlying Log.
}

impl OrderingError {
    pub fn unwrap_log_error(&self) -> &LogError {
        match self {
            OrderingError::LogError(l) => l,
            OrderingError::Custom(_) => panic!("expected log error"),
        }
    }
}

impl Error for OrderingError {}

impl Display for OrderingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

pub type Result<T> = result::Result<T, OrderingError>;

/// LogOrdering is used to order the operations in the Log.
pub trait LogOrdering {
    type Supporter: Supporters;

    /// Called after an operation is added to the log for the first time.
    fn recv_op(&mut self, op: OpEntryInfo) -> Result<()>;
    /// Called after an Sp is added to the log for the first time.
    /// Each successful call returns a list of 0 or more operations
    /// which be input to some state machine in the same order.
    /// The ops should be ones that have previously been input to recv_op.
    fn recv_sp<I: Iterator<Item = OpEntryInfo>>(
        &mut self,
        info: SpInfo,
        deps: I,
    ) -> Result<Vec<PendingOp<Self::Supporter>>>;
}

/// OrderedSate is used to implement a state machine, that creates and receives
/// operations from a LogOrdering.
pub trait OrderedState {
    /// Creates a operation at the local node.
    fn create_local_op(&mut self) -> Result<OpData>;
    /// Called before an operation is processesed at a local node.
    fn check_op(&mut self, op: &OpData) -> Result<()>;
    /// Called after an operation is received successfully at a local node
    fn received_op(&mut self, op: &OpEntryInfo);
    /// Called after an sp is received from Id that supports the ops in deps.
    fn after_recv_sp<'a, I: Iterator<Item = &'a OpEntryInfo>>(&mut self, from: Id, deps: I);
}

/// Implements the given OrderingLog and OrderedState by calling their operation.
pub struct OrderedLogRun<L: OrderedLog, S: OrderedState> {
    id: Id,
    l: L,
    state: S,
}

impl<L: OrderedLog, S: OrderedState> OrderedLogRun<L, S> {
    pub fn new(id: Id, l: L, state: S) -> Self {
        OrderedLogRun { id, l, state }
    }

    pub fn get_l(&self) -> &L {
        &self.l
    }

    pub fn get_l_mut(&mut self) -> &mut L {
        &mut self.l
    }

    pub fn get_s(&self) -> &S {
        &self.state
    }
}

impl<L: OrderedLog, S: OrderedState> OrderedLogRun<L, S> {
    #[inline(always)]
    pub fn serialize_option(&self) -> DefaultOptions {
        self.l.serialize_option()
    }

    pub fn check_sp_exact<T: TimeInfo>(&mut self, ti: &T, sp_p: &SpExactToProcess) -> Result<()> {
        self.l.check_sp_exact(ti, sp_p)
    }

    pub fn new_op<T: TimeInfo>(&mut self, ti: &mut T) -> Result<OpResult> {
        let data = self.state.create_local_op()?;
        let op = Op::new(self.id, data, ti);
        self.l.create_local_op(op, ti)
    }

    pub fn recv_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &mut T) -> Result<OpResult> {
        let op = self.l.received_op(op_c, ti)?;
        self.state.received_op(&op.info);
        Ok(op)
    }

    pub fn create_local_sp<T: TimeInfo>(&mut self, ti: &mut T) -> Result<(SpToProcess, SpDetails)> {
        let spr = self.l.create_local_sp(ti)?;
        self.state.after_recv_sp(
            spr.sp_d.info.id,
            spr.completed_ops.iter().map(|op| &op.data),
        );
        Ok((spr.sp_p, spr.sp_d))
    }

    pub fn get_sp_exact(&mut self, sp: Sp) -> Result<SpExactToProcess> {
        self.l.get_sp_exact(sp)
    }

    pub fn received_sp<T: TimeInfo>(
        &mut self,
        sp_p: SpToProcess,
        ti: &mut T,
    ) -> Result<(SpToProcess, SpDetails)> {
        let spr = self.l.received_sp(ti, sp_p)?;
        self.state.after_recv_sp(
            spr.sp_d.info.id,
            spr.completed_ops.iter().map(|op| &op.data),
        );
        Ok((spr.sp_p, spr.sp_d))
    }

    pub fn received_sp_exact<T: TimeInfo>(
        &mut self,
        sp_e: SpExactToProcess,
        ti: &mut T,
    ) -> Result<(SpToProcess, SpDetails)> {
        for op in sp_e.exact.iter() {
            self.state.check_op(&op.data)?;
        }
        let spr = self.l.received_sp_exact(ti, sp_e)?;
        for op in spr.new_ops {
            self.state.received_op(&op);
        }
        self.state.after_recv_sp(
            spr.o_sp.sp_d.info.id,
            spr.o_sp.completed_ops.iter().map(|op| &op.data),
        );
        Ok((spr.o_sp.sp_p, spr.o_sp.sp_d))
    }
}

/// Contains a local log, plus the LogOrdering trait to keep track.
pub struct OrderedLogContainer<F: RWS, O: LogOrdering> {
    pub(crate) l: LocalLog<F>,
    pub(crate) ordering: O,
}

pub struct OrderedSp<S: Supporters> {
    pub sp_p: SpToProcess,
    pub sp_d: SpDetails,
    pub completed_ops: Vec<PendingOp<S>>,
}

pub struct OrderedExactSp<S: Supporters> {
    pub sp_p: SpExactToProcess,
    pub sp_d: SpDetails,
    pub completed_ops: Vec<PendingOp<S>>,
}

/// The main interface to the underlying log.
pub trait OrderedLog {
    type File: RWS;
    type Ordering: LogOrdering;

    fn get_ordering(&self) -> &Self::Ordering;
    fn get_log_mut(&mut self) -> &mut Log<Self::File>;
    fn create_local_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
    ) -> Result<OrderedSp<<Self::Ordering as LogOrdering>::Supporter>>;
    fn create_local_op<T: TimeInfo>(&mut self, op: Op, ti: &T) -> Result<OpResult>;
    fn serialize_option(&self) -> DefaultOptions;
    fn received_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult>;
    fn get_sp_exact(&mut self, sp: Sp) -> Result<SpExactToProcess>;
    fn received_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpToProcess,
    ) -> Result<OrderedSp<<Self::Ordering as LogOrdering>::Supporter>>;
    fn check_sp_exact<T: TimeInfo>(&mut self, ti: &T, sp_p: &SpExactToProcess) -> Result<()>;
    /// Called when a new Sp is received with the exact set of Ops it contains.
    /// Returns a tuple containing the vector of new operations contained in the Sp
    /// (i.e. the Ops for which received_op was not called previouly) and the
    /// OrderedSp result struct.
    fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpExactToProcess,
    ) -> Result<SpExactResult<<Self::Ordering as LogOrdering>::Supporter>>;
}

pub struct SpExactResult<S: Supporters> {
    pub new_ops: Vec<OpEntryInfo>, // the new operations added to the log by this Sp
    pub o_sp: OrderedSp<S>,
}

/// Inputs operations to the log and the ordering as they are received.
impl<F: RWS, O: LogOrdering> OrderedLogContainer<F, O> {
    pub fn new(l: LocalLog<F>, ordering: O) -> Self {
        OrderedLogContainer { l, ordering }
    }
}

impl<F: RWS, O: LogOrdering> OrderedLog for OrderedLogContainer<F, O> {
    type File = F;
    type Ordering = O;

    #[inline(always)]
    fn get_ordering(&self) -> &O {
        &self.ordering
    }

    #[inline(always)]
    fn get_log_mut(&mut self) -> &mut Log<F> {
        &mut self.l.l
    }

    fn create_local_sp<T: TimeInfo>(&mut self, ti: &mut T) -> Result<OrderedSp<O::Supporter>> {
        let (sp_p, sp_d) = self
            .l
            .create_local_sp(ti)
            .map_err(OrderingError::LogError)?;
        let completed_ops = self.ordering.recv_sp(sp_d.info, sp_d.ops.iter().cloned())?;
        Ok(OrderedSp {
            sp_p,
            sp_d,
            completed_ops,
        })
    }

    fn create_local_op<T: TimeInfo>(&mut self, op: Op, ti: &T) -> Result<OpResult> {
        let res = self
            .l
            .create_local_op(op, ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_op(res.info.clone())?;
        Ok(res)
    }

    #[inline(always)]
    fn serialize_option(&self) -> DefaultOptions {
        self.l.serialize_option()
    }

    /// Inpts an operation from an external node in the log.
    fn received_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult> {
        let res = self
            .l
            .received_op(op_c, ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_op(res.info.clone())?;
        Ok(res)
    }

    #[inline(always)]
    fn get_sp_exact(&mut self, sp: Sp) -> Result<SpExactToProcess> {
        self.l.get_sp_exact(sp).map_err(OrderingError::LogError)
    }

    fn received_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpToProcess,
    ) -> Result<OrderedSp<O::Supporter>> {
        let (sp_p, sp_d) = self
            .l
            .received_sp(ti, sp_p)
            .map_err(OrderingError::LogError)?;
        let completed_ops = self.ordering.recv_sp(sp_d.info, sp_d.ops.iter().cloned())?;
        Ok(OrderedSp {
            sp_p,
            sp_d,
            completed_ops,
        })
    }

    fn check_sp_exact<T: TimeInfo>(&mut self, ti: &T, sp_p: &SpExactToProcess) -> Result<()> {
        let mut ops = vec![];
        for op in sp_p.exact.iter() {
            ops.push(
                op.to_entry_info(self.l.serialize_option())
                    .map_err(OrderingError::LogError)?,
            );
        }
        self.l
            .l
            .check_sp_exact(sp_p.sp.clone(), &ops, ti)
            .map_err(OrderingError::LogError)?;
        Ok(())
    }

    fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpExactToProcess,
    ) -> Result<SpExactResult<O::Supporter>> {
        let (new_ops, sp_p, sp_d) = self
            .l
            .received_sp_exact(ti, sp_p)
            .map_err(OrderingError::LogError)?;
        for op in new_ops.iter().cloned() {
            self.ordering.recv_op(op).unwrap(); // TODO, what to do on error here?
        }
        let completed_ops = self.ordering.recv_sp(sp_d.info, sp_d.ops.iter().cloned())?;
        Ok(SpExactResult {
            new_ops,
            o_sp: OrderedSp {
                sp_p,
                sp_d,
                completed_ops,
            },
        })
    }
}

pub type HMap<K, V> = FxHashMap<K, V>;
pub type HSet<K> = FxHashSet<K>;

/// Dependents is used to track the log indicies of the operations that an Sp supports.
/// Each Op that is not ready that the Sp supports is added to the Dependents set of the Sp.
/// When the operation become ready, got_support is called with the log index of the Op.
/// TODO allow to change between implementations when different number of participants.
pub trait Dependents: Default + Debug {
    /// Add the set of log indicies to the dependents set.
    fn add_idxs<I: Iterator<Item = LogIdx>>(&mut self, i: I);
    /// Add the index to the dependents set.
    fn add_idx(&mut self, idx: LogIdx);
    /// Called once the Op at idx has completed.
    /// Returns true if idx was in the set, false otherwise.
    fn got_support(&mut self, idx: LogIdx) -> bool;
    /// Returns the number of indicies in the set that have not received support through got_support.
    fn remaining_idxs(&self) -> usize;
}

/// Implements the Dependents trait using a vector.
#[derive(Debug)]
pub struct DepVec {
    v: Vec<LogIdx>,
}

impl Default for DepVec {
    fn default() -> Self {
        DepVec { v: vec![] }
    }
}

impl Dependents for DepVec {
    #[inline(always)]
    fn add_idxs<I: Iterator<Item = LogIdx>>(&mut self, i: I) {
        for nxt in i {
            self.add_idx(nxt);
        }
    }

    #[inline(always)]
    fn add_idx(&mut self, idx: LogIdx) {
        debug_assert!(!self.v.contains(&idx));
        self.v.push(idx);
    }

    #[inline(always)]
    fn got_support(&mut self, idx: LogIdx) -> bool {
        for (i, nxt) in self.v.iter().cloned().enumerate() {
            if nxt == idx {
                self.v.swap_remove(i);
                return true;
            }
        }
        false
    }

    #[inline(always)]
    fn remaining_idxs(&self) -> usize {
        self.v.len()
    }
}

/// Implements the Dependents trait using a HashSet.
#[derive(Debug)]
pub struct DepHSet(HSet<LogIdx>);

impl Default for DepHSet {
    #[inline(always)]
    fn default() -> Self {
        DepHSet(HSet::default())
    }
}

impl Dependents for DepHSet {
    #[inline(always)]
    fn add_idxs<I: Iterator<Item = LogIdx>>(&mut self, i: I) {
        for nxt in i {
            self.add_idx(nxt);
        }
    }

    #[inline(always)]
    fn add_idx(&mut self, idx: LogIdx) {
        self.0.insert(idx);
    }

    #[inline(always)]
    fn got_support(&mut self, idx: LogIdx) -> bool {
        self.0.remove(&idx)
    }

    #[inline(always)]
    fn remaining_idxs(&self) -> usize {
        self.0.len()
    }
}

/// Implements the Dependents trait using a BTree.
#[derive(Debug)]
pub struct DepBTree(BTreeSet<LogIdx>);

impl Default for DepBTree {
    #[inline(always)]
    fn default() -> Self {
        DepBTree(BTreeSet::new())
    }
}

impl Dependents for DepBTree {
    #[inline(always)]
    fn add_idxs<I: Iterator<Item = LogIdx>>(&mut self, i: I) {
        for idx in i {
            self.0.insert(idx);
        }
    }

    #[inline(always)]
    fn got_support(&mut self, idx: LogIdx) -> bool {
        self.0.remove(&idx)
    }

    #[inline(always)]
    fn remaining_idxs(&self) -> usize {
        self.0.len()
    }

    #[inline(always)]
    fn add_idx(&mut self, idx: LogIdx) {
        self.0.insert(idx);
    }
}

/// Supporters is used to track the Ids different Sps that have supported an operation.
/// TODO allow to change between implementations when different number of participants.
pub trait Supporters: Default + Sized + Debug {
    /// If the set did not have this id present, `true` is returned.
    /// If the set did have this id present, `false` is returned, and the
    /// entry is not updated.
    fn add_id(&mut self, id: Id) -> bool;
    /// Returns the number of supporters from different Ids.
    fn get_count(&self) -> usize;
}

/// Implements the Supporters trait using a HashSet.
#[derive(Debug)]
pub struct SupHSet(HSet<Id>);

impl Default for SupHSet {
    #[inline(always)]
    fn default() -> Self {
        SupHSet(HSet::default())
    }
}

impl Supporters for SupHSet {
    #[inline(always)]
    fn add_id(&mut self, id: Id) -> bool {
        self.0.insert(id)
    }

    #[inline(always)]
    fn get_count(&self) -> usize {
        self.0.len()
    }
}

/// Implements the Supporters trait using a BTree.
#[derive(Debug)]
pub struct SupBTree(BTreeSet<Id>);

impl Default for SupBTree {
    #[inline(always)]
    fn default() -> Self {
        SupBTree(BTreeSet::new())
    }
}

impl Supporters for SupBTree {
    #[inline(always)]
    fn add_id(&mut self, id: Id) -> bool {
        self.0.insert(id)
    }

    #[inline(always)]
    fn get_count(&self) -> usize {
        self.0.len()
    }
}

/// Implements the Supporters trait using a vector.
#[derive(Debug)]
pub struct SupVec {
    s: Vec<bool>,
    count: usize,
}

const DEFAILT_SUP_SIZE: usize = 6;

impl Default for SupVec {
    #[inline(always)]
    fn default() -> Self {
        SupVec {
            s: vec![false; DEFAILT_SUP_SIZE],
            count: 0,
        }
    }
}

impl Supporters for SupVec {
    #[inline(always)]
    fn add_id(&mut self, id: Id) -> bool {
        if let Some(diff) = (id + 1).checked_sub(self.s.len() as u64) {
            self.s.extend(repeat(false).take(diff as usize));
        }
        if !self.s[id as usize] {
            self.s[id as usize] = true;
            self.count += 1;
            true
        } else {
            false
        }
    }

    #[inline(always)]
    fn get_count(&self) -> usize {
        self.count
    }
}
/// An operation and the Ids of the Sps that have supported it.
#[derive(Debug)]
pub struct PendingOp<S: Supporters> {
    pub supporters: S, // IDs of nodes that have supported this op through SPs
    pub data: OpEntryInfo,
}

impl<S: Supporters> PendingOp<S> {
    #[inline(always)]
    pub fn new(data: OpEntryInfo) -> Self {
        PendingOp {
            supporters: S::default(), // ids SPs that have supported us
            data,
        }
    }
}

pub mod test_structs {
    use std::{
        collections::{HashMap, HashSet, VecDeque},
        hash::{self, Hash},
        mem::replace,
    };

    use bincode::DefaultOptions;
    use log::debug;
    use rand::{
        distributions::Standard,
        prelude::{Distribution, StdRng},
        Rng, SeedableRng,
    };

    use crate::{
        log::{
            basic_log::test_fns::print_log_from_end,
            basic_log::Log,
            local_log::{OpCreated, OpResult, SpExactToProcess, SpToProcess},
            log_error::LogError,
            op::{EntryInfo, Op, OpEntryInfo},
            sp::{Sp, SpInfo},
        },
        rw_buf::RWS,
        utils,
        verification::{Id, TimeInfo, TimeTest},
    };

    use super::{
        LogOrdering, OrderedLog, OrderedLogContainer, OrderedLogRun, OrderedSp, OrderedState,
        OrderingError, PendingOp, Result, SpExactResult, Supporters,
    };

    enum ChooseOp {
        ProcessEntry,
        CreateEntry,
    }

    impl Distribution<ChooseOp> for Standard {
        fn sample<R: rand::Rng + ?Sized>(&self, rng: &mut R) -> ChooseOp {
            match rng.gen_range(0..100) {
                0..=80 => ChooseOp::ProcessEntry,
                _ => ChooseOp::CreateEntry,
            }
        }
    }

    pub fn run_ordered_rand<L: OrderedLog, S: OrderedState>(
        logs: &mut [(OrderedLogRun<L, S>, TimeTest)],
        num_ops: usize,
        seed: u64,
    ) {
        let num_logs = logs.len();
        let mut ops = vec![];
        let mut rng = StdRng::seed_from_u64(seed);
        // choose an op type
        let mut op_count = 0;
        // continue until we have created enough ops and they have all been processed by all nodes
        while op_count < num_ops || !ops.is_empty() {
            match rng.gen() {
                ChooseOp::CreateEntry => {
                    if op_count >= num_ops {
                        // now we just want to process existing ops
                        continue;
                    }
                    let idx = rng.gen_range(0..num_logs) as usize;
                    let others: VecDeque<usize> =
                        utils::gen_shuffled(num_logs as usize, Some(idx), &mut rng).into();
                    debug!("idx {}, others {:?}", idx, others);
                    let (l, ti) = &mut logs[idx];
                    l.new_op(ti).unwrap();
                    ti.set_current_time_valid();
                    let (sp, _) = l.create_local_sp(ti).unwrap();
                    let sp_e = l.get_sp_exact(sp.sp).unwrap();
                    debug!("new sp {:?}, exact {:?}", sp_e.sp, sp_e.exact);
                    ops.push((sp_e, others));
                    op_count += 1;
                }
                ChooseOp::ProcessEntry => {
                    if ops.is_empty() {
                        // we need to generate a new op
                        continue;
                    }
                    // process a random op
                    let op_idx = rng.gen_range(0..ops.len());
                    let (sp, others) = &mut ops[op_idx];
                    let idx = others.pop_back().unwrap();
                    debug!("process log {}", idx);
                    let (l, ti) = &mut logs[idx];
                    ti.set_sp_time_valid(sp.sp.info.time);
                    if let Err(err) = l.check_sp_exact(ti, sp) {
                        if let OrderingError::LogError(err) = &err {
                            match err {
                                LogError::PrevSpNotFound => {
                                    others.push_front(idx);
                                    continue;
                                }
                                LogError::PrevSpHasNoLastOp
                                | LogError::NotEnoughOpsForSP
                                | LogError::SpSkippedOps(_) => {
                                    // ok
                                }
                                _ => panic!("unexpected error from Sp {:?}", err),
                            }
                        } else {
                            panic!("should not be able to process Sp");
                        }
                    }
                    debug!("process sp {:?}", sp);
                    let (n_sp, _) = l.received_sp_exact(sp.clone(), ti).unwrap();
                    *sp = l.get_sp_exact(n_sp.sp).unwrap();
                    if others.is_empty() {
                        // all logs have received this op/sp
                        ops.swap_remove(op_idx);
                    }
                }
            }
        }

        let mut end_sps = vec![];
        for (i, (l, ti)) in logs.iter_mut().enumerate() {
            // create an Sp at each node so all ops are supported by all Sps
            if let Ok((sp, _)) = l.create_local_sp(ti) {
                let sp_e = l.get_sp_exact(sp.sp).unwrap();
                end_sps.push((i, sp_e));
            }
        }
        for (i, sp) in end_sps.into_iter() {
            for (j, (l, ti)) in logs.iter_mut().enumerate() {
                if i == j {
                    continue;
                }
                ti.set_sp_time_valid(sp.sp.info.time);
                l.received_sp_exact(sp.clone(), ti).unwrap();
            }
        }
    }

    pub fn run_ordered_once<L: OrderedLog, S: OrderedState>(
        logs: &mut [(OrderedLogRun<L, S>, TimeTest)],
    ) {
        let num_logs = logs.len();
        let mut ops = vec![];
        let mut sps = vec![];
        // create an op and an SP in each log
        // print_logs(&mut logs);
        for i in 0..num_logs as usize {
            let (l, ti) = &mut logs[i];
            ops.push(l.new_op(ti).unwrap());
            assert_eq!(
                &LogError::OpAlreadyExists,
                l.recv_op(ops[i].create.clone(), ti)
                    .unwrap_err()
                    .unwrap_log_error()
            );
        }
        create_ordered_sp(logs, &mut sps);
        // print_logs(&mut logs);
        // insert the ops in each log
        for (i, (l, ti)) in logs.iter_mut().enumerate() {
            for j in 0..(num_logs - 1) as usize {
                ti.set_current_time_valid();
                let j = (i + j + 1) % num_logs as usize;
                ops[j] = l.recv_op(ops[j].create.clone(), ti).unwrap();
            }
        }
        //print_logs(&mut logs);
        // insert the sps in each log
        insert_ordered_sps(logs, &mut sps);
        // print_logs(&mut logs);
        // create a new Sp at each log so that they each support the exernal ops
        create_ordered_sp(logs, &mut sps);
        insert_ordered_sps(logs, &mut sps);
        //print_logs(&mut logs);
    }

    fn print_ordered_logs<L: OrderedLog, S: OrderedState>(
        logs: &mut [(OrderedLogRun<L, S>, TimeTest)],
    ) {
        for (i, (l, _)) in logs.iter_mut().enumerate() {
            debug!("\n\nprinting log {}", i);
            print_log_from_end(l.get_l_mut().get_log_mut());
        }
        debug!("\n\n");
    }

    fn create_ordered_sp<L: OrderedLog, S: OrderedState>(
        logs: &mut [(OrderedLogRun<L, S>, TimeTest)],
        sps: &mut Vec<SpExactToProcess>,
    ) {
        for (l, ti) in logs.iter_mut() {
            debug!("\nCreateSP\n");
            ti.set_current_time_valid();
            let (sp, _) = l.create_local_sp(ti).unwrap();
            assert_eq!(
                &LogError::SpAlreadyExists,
                l.received_sp(sp.clone(), ti)
                    .unwrap_err()
                    .unwrap_log_error()
            );
            sps.push(l.get_sp_exact(sp.sp).unwrap());
        }
    }

    fn insert_ordered_sps<L: OrderedLog, S: OrderedState>(
        logs: &mut [(OrderedLogRun<L, S>, TimeTest)],
        sps: &mut Vec<SpExactToProcess>,
    ) {
        let num_logs = logs.len();
        for (i, (l, ti)) in logs.iter_mut().enumerate() {
            for j in 0..(num_logs - 1) as usize {
                debug!("\nInsert Sp in log {} from {}\n", i, j);

                let j = (i + j + 1) % num_logs as usize;
                let sp_p = l.received_sp_exact(sps[j].clone(), ti).unwrap().0;
                sps[j] = l.get_sp_exact(sp_p.sp).unwrap();
            }
        }
        sps.clear();
    }

    /// A hashable EntryInfo, should only be used for tests to track EntryInfo structs in a HashMap.
    struct HashEntryInfo(EntryInfo);

    impl Hash for HashEntryInfo {
        fn hash<H: hash::Hasher>(&self, state: &mut H) {
            state.write(self.0.hash.as_bytes())
        }
    }

    impl Eq for HashEntryInfo {}

    impl PartialEq for HashEntryInfo {
        fn eq(&self, other: &Self) -> bool {
            self.0.eq(&other.0)
        }
    }

    impl From<&OpEntryInfo> for HashEntryInfo {
        fn from(op: &OpEntryInfo) -> Self {
            HashEntryInfo(op.into())
        }
    }

    /// Used to check all the logs have the same state of Ops and Sps.
    pub fn check_ordered_logs<F: RWS, O: LogOrdering>(logs: &[&OrderedLogTest<F, O>]) {
        let prev_l = logs[0];
        for &l in &logs[1..] {
            // check the log test objects are equal by checking they contain eachother.
            l.check_other_contains_all(prev_l);
            prev_l.check_other_contains_all(l);
        }
    }

    /// Inputs operations to the log and the ordering as they are received.
    /// Implementation of Ordered log that tracks in memory the state of operations
    /// and Sps. Used for testing, where check_ordered_logs is called
    /// after the test to ensure all logs end up with the same state
    /// of Ops and Sps.
    pub struct OrderedLogTest<F: RWS, O: LogOrdering> {
        l: OrderedLogContainer<F, O>,
        // the number of times each op has been received
        received_ops: HashMap<HashEntryInfo, usize>,
        // the number of times each Op has been supported by each
        supported_ops: HashMap<HashEntryInfo, HashMap<Id, usize>>,
        // the ops that were returned as completed by the ordering
        completed_ops: HashSet<HashEntryInfo>,
    }

    impl<F: RWS, O: LogOrdering> OrderedLogTest<F, O> {
        pub fn new(l: OrderedLogContainer<F, O>) -> Self {
            OrderedLogTest {
                l,
                received_ops: HashMap::default(),
                supported_ops: HashMap::default(),
                completed_ops: HashSet::default(),
            }
        }

        /// Checks if other has at least all the set of events as self.
        pub fn check_other_contains_all(&self, other: &OrderedLogTest<F, O>) {
            // be sure all ops were received by all nodes
            for (op, _) in self.received_ops.iter() {
                other
                    .received_ops
                    .get(op)
                    .unwrap_or_else(|| panic!("didn't recive for {:?}", op.0));
            }
            // be sure all have the same set of Sps supporting the same set of ops
            for (op, prev_sup_map) in self.supported_ops.iter() {
                let sup_map = other
                    .supported_ops
                    .get(op)
                    .unwrap_or_else(|| panic!("didn't find support for {:?}", op.0));
                let mut prev_sup: Vec<_> = prev_sup_map.iter().map(|(a, b)| (*a, *b)).collect();
                prev_sup.sort_unstable();
                let mut sup: Vec<_> = sup_map.iter().map(|(a, b)| (*a, *b)).collect();
                sup.sort_unstable();
                assert_eq!(prev_sup, sup);
            }
            // be sure all completed the same set of ops
            for op in self.completed_ops.iter() {
                if !other.completed_ops.contains(op) {
                    panic!(
                        "id {} did not complete op {:?}, got support {:?}",
                        other.l.l.my_id(),
                        op.0,
                        other.supported_ops.get(op)
                    );
                }
            }
        }

        /// Tracks the ops supported by the Sp.
        fn after_recv_sp(&mut self, o_sp: &OrderedSp<O::Supporter>) {
            let id = o_sp.sp_d.info.id;
            for nxt in o_sp.sp_d.ops.iter() {
                let support_count = self
                    .supported_ops
                    .entry(nxt.into())
                    .or_default()
                    .entry(id)
                    .or_default();
                *support_count += 1;
            }
            debug!("\nSp from {} supports {:?}\n", id, o_sp.sp_d.ops);
            for nxt in o_sp.completed_ops.iter() {
                // make sure we only complete an op once
                assert!(self.completed_ops.replace((&nxt.data).into()).is_none());
            }
        }

        /// Tracks the received op.
        fn after_recv_op(&mut self, op: &OpEntryInfo) {
            let support_count = self.received_ops.entry(op.into()).or_default();
            *support_count += 1;
        }
    }

    impl<F: RWS, O: LogOrdering> OrderedLog for OrderedLogTest<F, O> {
        type File = F;
        type Ordering = O;

        fn get_ordering(&self) -> &O {
            self.l.get_ordering()
        }

        fn get_log_mut(&mut self) -> &mut Log<F> {
            self.l.get_log_mut()
        }

        fn create_local_sp<T: TimeInfo>(&mut self, ti: &mut T) -> Result<OrderedSp<O::Supporter>> {
            let o_sp = self.l.create_local_sp(ti)?;
            self.after_recv_sp(&o_sp);
            Ok(o_sp)
        }
        fn create_local_op<T: TimeInfo>(&mut self, op: Op, ti: &T) -> Result<OpResult> {
            let op = self.l.create_local_op(op, ti)?;
            self.after_recv_op(&op.info);
            Ok(op)
        }
        fn serialize_option(&self) -> DefaultOptions {
            self.l.serialize_option()
        }
        fn received_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult> {
            let op = self.l.received_op(op_c, ti)?;
            self.after_recv_op(&op.info);
            Ok(op)
        }
        fn get_sp_exact(&mut self, sp: Sp) -> Result<SpExactToProcess> {
            self.l.get_sp_exact(sp)
        }
        fn received_sp<T: TimeInfo>(
            &mut self,
            ti: &mut T,
            sp_p: SpToProcess,
        ) -> Result<OrderedSp<O::Supporter>> {
            let o_sp = self.l.received_sp(ti, sp_p)?;
            self.after_recv_sp(&o_sp);
            Ok(o_sp)
        }

        fn check_sp_exact<T: TimeInfo>(&mut self, ti: &T, sp_p: &SpExactToProcess) -> Result<()> {
            self.l.check_sp_exact(ti, sp_p)
        }

        fn received_sp_exact<T: TimeInfo>(
            &mut self,
            ti: &mut T,
            sp_p: SpExactToProcess,
        ) -> Result<SpExactResult<O::Supporter>> {
            let spr = self.l.received_sp_exact(ti, sp_p)?;
            for op in spr.new_ops.iter() {
                self.after_recv_op(op);
            }
            self.after_recv_sp(&spr.o_sp);
            Ok(spr)
        }
    }

    /// Tracks the ops received and commits them once Sps have been received
    /// from commit_count different supporters.
    pub struct CollectOrdered<S: Supporters> {
        commit_count: usize,
        ops: HashMap<HashEntryInfo, CollectedOp<S>>,
    }

    impl<S: Supporters> CollectOrdered<S> {
        pub fn new(commit_count: usize) -> Self {
            CollectOrdered {
                commit_count,
                ops: HashMap::default(),
            }
        }
    }

    enum CollectedOp<S: Supporters> {
        Pending(PendingOp<S>),
        Completed,
    }

    impl<S: Supporters> CollectedOp<S> {
        fn completed(&mut self) -> PendingOp<S> {
            match replace(self, CollectedOp::Completed) {
                CollectedOp::Pending(p) => p,
                _ => panic!("expected pending"),
            }
        }
    }

    impl<S: Supporters> LogOrdering for CollectOrdered<S> {
        type Supporter = S;

        fn recv_sp<I: Iterator<Item = OpEntryInfo>>(
            &mut self,
            info: SpInfo,
            deps: I,
        ) -> Result<Vec<PendingOp<Self::Supporter>>> {
            let sup = info.id;
            let mut completed = vec![];
            for op in deps {
                let c_op = self.ops.get_mut(&(&op).into()).unwrap();
                if let CollectedOp::Pending(p_op) = c_op {
                    // add the support, and if we have enough supporters we are completed
                    if p_op.supporters.add_id(sup)
                        && p_op.supporters.get_count() >= self.commit_count
                    {
                        completed.push(c_op.completed());
                    }
                }
            }
            Ok(completed)
        }
        fn recv_op(&mut self, op: OpEntryInfo) -> Result<()> {
            self.ops
                .entry((&op).into())
                .or_insert_with(|| CollectedOp::Pending(PendingOp::new(op)));
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, error::Error, fmt::Display, fs::File};

    use log::debug;

    use crate::{
        log::basic_log::Log,
        log::local_log::new_local_log,
        log::{
            log_file::open_log_file,
            op::{to_op_data, OpData, OpEntryInfo},
        },
        rw_buf::RWBuf,
        verification::{Id, TimeTest},
    };

    use super::{
        test_structs::{
            check_ordered_logs, run_ordered_once, run_ordered_rand, CollectOrdered, OrderedLogTest,
        },
        DepBTree, DepHSet, DepVec, Dependents, OrderedLogContainer, OrderedLogRun, OrderedState,
        OrderingError, Result, SupBTree, SupHSet, SupVec, Supporters,
    };

    fn supporters<S: Supporters>(mut s: S) {
        assert_eq!(0, s.get_count());
        for i in 1..=100 {
            assert!(s.add_id(i));
            assert!(!s.add_id(i));
            assert_eq!(i as usize, s.get_count());
        }
    }

    #[test]
    fn supporters_btree() {
        supporters(SupBTree::default());
    }

    #[test]
    fn supporters_vec() {
        supporters(SupVec::default());
    }
    #[test]
    fn supporters_hmap() {
        supporters(SupHSet::default());
    }

    fn dependents<D: Dependents>(mut d: D) {
        // fn add_ids<I: Iterator<Item = LogIdx>>(&mut self, i: I);
        // fn add_id(&mut self, idx: LogIdx);
        // fn got_support(&mut self, idx: LogIdx) -> bool;
        // fn remaining_idxs(&self) -> usize;
        assert_eq!(0, d.remaining_idxs());
        d.add_idxs(0..100);
        assert_eq!(100, d.remaining_idxs());
        for i in 100..200 {
            d.add_idx(i);
        }
        for i in 0..200 {
            assert!(d.got_support(i));
            assert!(!d.got_support(i));
            assert_eq!(199 - i as usize, d.remaining_idxs());
        }
        assert_eq!(0, d.remaining_idxs());
    }

    #[test]
    fn dependents_btree() {
        dependents(DepBTree::default());
    }

    #[test]
    fn dependents_vec() {
        dependents(DepVec::default());
    }
    #[test]
    fn dependents_hset() {
        dependents(DepHSet::default());
    }

    // A simple counter state per id
    struct CounterState {
        counts: HashMap<Id, usize>,
    }

    #[derive(Debug, PartialEq, Eq)]
    pub enum CounterError {
        InvalidCounter,
    }

    impl Error for CounterError {}

    impl Display for CounterError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            std::fmt::Debug::fmt(&self, f)
        }
    }

    impl Default for CounterState {
        fn default() -> Self {
            CounterState {
                counts: HashMap::default(),
            }
        }
    }

    impl OrderedState for CounterState {
        fn create_local_op(&mut self) -> Result<OpData> {
            Ok(to_op_data(vec![]))
        }

        fn check_op(&mut self, op: &OpData) -> Result<()> {
            if op.len() > 0 {
                Err(OrderingError::Custom(Box::new(
                    CounterError::InvalidCounter,
                )))
            } else {
                Ok(())
            }
        }

        fn received_op(&mut self, _op: &OpEntryInfo) {
            // nothing
        }

        fn after_recv_sp<'a, I: Iterator<Item = &'a OpEntryInfo>>(&mut self, _from: Id, deps: I) {
            for op in deps {
                let c = self.counts.entry(op.op.info.id).or_default();
                *c += 1;
            }
        }
    }

    #[test]
    fn collect_rand() {
        let num_ops = 20;
        for seed in 100..110 {
            for num_logs in 2..4 {
                for commit_count in 1..=num_logs as usize {
                    let mut logs = vec![];
                    for id in 0..num_logs {
                        logs.push(new_counter(id, id as usize, commit_count));
                    }
                    run_ordered_rand(&mut logs, num_ops, seed);
                    // check the logs have the same vector clock
                    check_logs(&mut logs);
                }
            }
        }
    }

    #[test]
    fn collect_single() {
        let num_logs = 4;
        // an op is not committed until it is supported by 3 nodes
        let commit_count = 3;
        let mut logs = vec![];
        for id in 0..num_logs {
            logs.push(new_counter(id, 100 + id as usize, commit_count));
        }
        run_ordered_once(&mut logs);
        check_logs(&mut logs);
    }

    type CollectTestLog =
        OrderedLogRun<OrderedLogTest<RWBuf<File>, CollectOrdered<SupVec>>, CounterState>;

    fn new_counter(id: Id, test_idx: usize, commit_count: usize) -> (CollectTestLog, TimeTest) {
        let f = new_local_log(
            id,
            Log::new(
                open_log_file(
                    &format!("log_files/ordered_log{}_{}.log", test_idx, id),
                    true,
                    RWBuf::new,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let collect = CollectOrdered::new(commit_count);
        let l = OrderedLogTest::new(OrderedLogContainer::new(f, collect));
        let counter = CounterState::default();
        (OrderedLogRun::new(id, l, counter), TimeTest::new())
    }

    fn check_logs(logs: &mut [(CollectTestLog, TimeTest)]) {
        let test_logs: Vec<_> = logs.iter().map(|(l, _)| l.get_l()).collect();
        check_ordered_logs(&test_logs);

        let mut prev_vec = None;
        for (l, _) in logs {
            let mut v: Vec<_> = l.get_s().counts.iter().collect();
            v.sort();
            if let Some(p) = prev_vec.take() {
                assert_eq!(p, v);
                debug!("p {:?}, v {:?}", p, v);
            }
            prev_vec = Some(v);
        }
    }
}
