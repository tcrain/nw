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
    local_log::{
        LocalLog, OpCreated, OpResult, SpCreated, SpDetails, SpExactToProcess, SpToProcess,
    },
    log_error::LogError,
    op::{Op, OpEntryInfo},
    sp::SpInfo,
    LogIdx,
};

#[derive(Debug)]
pub enum OrderingError {
    Custom(Box<dyn Error>),
    LogError(LogError),
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

pub trait LogOrdering {
    type Supporter: Supporters;

    fn recv_sp<I: Iterator<Item = OpEntryInfo>>(
        &mut self,
        info: SpInfo,
        deps: I,
    ) -> Result<Vec<PendingOp<Self::Supporter>>>;
    fn recv_op(&mut self, op: OpEntryInfo) -> Result<()>;
}

/// Contains a local log, plus the LogOrdering trait to keep track.
pub struct OrderedLog<F: RWS, O: LogOrdering> {
    l: LocalLog<F>,
    pub(crate) ordering: O,
    // phantom: PhantomData<S>,
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

impl<F: RWS, O: LogOrdering> OrderedLog<F, O> {
    pub fn new(l: LocalLog<F>, ordering: O) -> Self {
        OrderedLog { l, ordering }
    }

    pub fn create_local_sp<T: TimeInfo>(&mut self, ti: &mut T) -> Result<OrderedSp<O::Supporter>> {
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

    pub fn create_local_op<T: TimeInfo>(&mut self, op: Op, ti: &T) -> Result<OpResult> {
        let res = self
            .l
            .create_local_op(op, ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_op(res.info.clone())?;
        Ok(res)
    }

    #[inline(always)]
    pub fn serialize_option(&self) -> DefaultOptions {
        self.l.serialize_option()
    }

    /// Inpts an operation from an external node in the log.
    pub fn received_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult> {
        let res = self
            .l
            .received_op(op_c, ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_op(res.info.clone())?;
        Ok(res)
    }

    #[inline(always)]
    pub fn get_sp_exact(&mut self, sp_c: SpCreated) -> Result<SpExactToProcess> {
        self.l.get_sp_exact(sp_c).map_err(OrderingError::LogError)
    }

    pub fn received_sp<T: TimeInfo>(
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

    pub fn check_sp_exact<T: TimeInfo>(&mut self, ti: &T, sp_p: &SpExactToProcess) -> Result<()> {
        let sp = sp_p
            .sp
            .to_sp(self.serialize_option())
            .map_err(OrderingError::LogError)?;
        self.l
            .l
            .check_sp_exact(sp, sp_p.exact.iter().map(|op| op.into()), ti)
            .map_err(OrderingError::LogError)?;
        Ok(())
    }

    pub fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpExactToProcess,
    ) -> Result<OrderedSp<O::Supporter>> {
        let (sp_p, sp_d) = self
            .l
            .received_sp_exact(ti, sp_p)
            .map_err(OrderingError::LogError)?;
        let completed_ops = self.ordering.recv_sp(sp_d.info, sp_d.ops.iter().cloned())?;
        Ok(OrderedSp {
            sp_p,
            sp_d,
            completed_ops,
        })
    }
}

pub type HMap<K, V> = FxHashMap<K, V>;
pub type HSet<K> = FxHashSet<K>;

/// TODO allow to change between implementations when different number of participants.
pub trait Dependents: Default + Debug {
    fn add_idxs<I: Iterator<Item = LogIdx>>(&mut self, i: I);
    fn add_idx(&mut self, idx: LogIdx);
    fn got_support(&mut self, idx: LogIdx) -> bool;
    fn remaining_idxs(&self) -> usize;
}

#[derive(Debug)]
pub struct DepVec {
    v: Vec<LogIdx>,
    // count: usize,
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

/// TODO allow to change between implementations when different number of participants.
pub trait Supporters: Default + Sized + Debug {
    /// If the set did not have this id present, `true` is returned.
    ///
    /// If the set did have this id present, `false` is returned, and the
    /// entry is not updated.
    fn add_id(&mut self, id: Id) -> bool;
    fn get_count(&self) -> usize;
}

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

#[derive(Debug)]
pub struct PendingOp<S: Supporters> {
    pub supporters: S, // IDs of nodes that have supported this op through SPs
    pub data: OpEntryInfo,
    pub local_supported: bool,
    pub dependent_sps: Option<Vec<LogIdx>>, // SPs that depend on this op
}

impl<S: Supporters> PendingOp<S> {
    #[inline(always)]
    pub fn new(data: OpEntryInfo) -> Self {
        PendingOp {
            supporters: S::default(), // ids SPs that have supported us
            data,
            local_supported: false,
            dependent_sps: Some(vec![]), // sps that are waiting until we have local support
        }
    }

    #[inline(always)]
    pub fn is_completed(&self, commit_count: usize) -> bool {
        self.local_supported && self.supporters.get_count() >= commit_count
    }

    #[inline(always)]
    pub fn add_dependent_sp(&mut self, log_idx: LogIdx) {
        self.dependent_sps.as_mut().unwrap().push(log_idx);
    }

    #[inline(always)]
    pub fn got_supporter(&mut self, id: Id) -> bool {
        let new_id = self.supporters.add_id(id);
        if new_id && !self.local_supported && id == self.data.op.info.id {
            self.local_supported = true;
        }
        new_id
    }

    #[inline(always)]
    pub fn local_supported(&self) -> bool {
        self.local_supported
    }
}
