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
pub struct OrderedLogContainer<F: RWS, O: LogOrdering> {
    pub(crate) l: LocalLog<F>,
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

pub trait OrderedLog<F: RWS, O: LogOrdering> {
    fn get_ordering(&self) -> &O;
    fn get_log_mut(&mut self) -> &mut Log<F>;
    fn create_local_sp<T: TimeInfo>(&mut self, ti: &mut T) -> Result<OrderedSp<O::Supporter>>;
    fn create_local_op<T: TimeInfo>(&mut self, op: Op, ti: &T) -> Result<OpResult>;
    fn serialize_option(&self) -> DefaultOptions;
    fn received_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult>;
    fn get_sp_exact(&mut self, sp_c: SpCreated) -> Result<SpExactToProcess>;
    fn received_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpToProcess,
    ) -> Result<OrderedSp<O::Supporter>>;
    fn check_sp_exact<T: TimeInfo>(&mut self, ti: &T, sp_p: &SpExactToProcess) -> Result<()>;
    fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpExactToProcess,
    ) -> Result<OrderedSp<O::Supporter>>;
}

impl<F: RWS, O: LogOrdering> OrderedLogContainer<F, O> {
    pub fn new(l: LocalLog<F>, ordering: O) -> Self {
        OrderedLogContainer { l, ordering }
    }
}

impl<F: RWS, O: LogOrdering> OrderedLog<F, O> for OrderedLogContainer<F, O> {
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
    fn get_sp_exact(&mut self, sp_c: SpCreated) -> Result<SpExactToProcess> {
        self.l.get_sp_exact(sp_c).map_err(OrderingError::LogError)
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
        let sp = sp_p
            .sp
            .to_sp(self.serialize_option())
            .map_err(OrderingError::LogError)?;
        let exact: Vec<_> = sp_p.exact.iter().map(|op| op.into()).collect();
        self.l
            .l
            .check_sp_exact(sp, &exact, ti)
            .map_err(OrderingError::LogError)?;
        Ok(())
    }

    fn received_sp_exact<T: TimeInfo>(
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

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub struct SupportInfo {
    local_supported: bool, // true if received an Sp from the local node supporting this op
    creator_supported: bool, // true if received an SP from the creator of this op supporting this op
}

impl Default for SupportInfo {
    fn default() -> Self {
        SupportInfo {
            local_supported: false,
            creator_supported: false,
        }
    }
}

impl SupportInfo {
    /// Creates a new SupportInfo with support set to true.
    pub fn new_supported() -> SupportInfo {
        SupportInfo {
            local_supported: true,
            creator_supported: true,
        }
    }

    /// Returns true if supported both by the local node and the op creator.
    #[inline(always)]
    pub fn is_supported(&self) -> bool {
        self.local_supported && self.creator_supported
    }

    /// Returns true if supported by the op creator.
    #[inline(always)]
    pub fn creator_supported(&self) -> bool {
        self.creator_supported
    }

    /// Returns true if supported by the local node.
    #[inline(always)]
    pub fn local_supported(&self) -> bool {
        self.local_supported
    }
}

#[derive(Debug)]
pub struct PendingOp<S: Supporters> {
    pub supporters: S, // IDs of nodes that have supported this op through SPs
    pub data: OpEntryInfo,
    pub dependent_sps: Option<Vec<LogIdx>>, // SPs that depend on this op
    support: SupportInfo,
}

impl<S: Supporters> PendingOp<S> {
    #[inline(always)]
    pub fn new(data: OpEntryInfo) -> Self {
        PendingOp {
            supporters: S::default(), // ids SPs that have supported us
            data,
            support: SupportInfo::default(),
            dependent_sps: Some(vec![]), // sps that are waiting until we have local support
        }
    }

    #[inline(always)]
    pub fn get_support_info(&self) -> SupportInfo {
        self.support
    }

    #[inline(always)]
    pub fn is_completed(&self, commit_count: usize) -> bool {
        self.support.is_supported() && self.supporters.get_count() >= commit_count
    }

    #[inline(always)]
    pub fn add_dependent_sp(&mut self, log_idx: LogIdx) {
        self.dependent_sps.as_mut().unwrap().push(log_idx);
    }

    /// Called when id supports the op.
    /// local_id is the id of the local node.
    /// Returns true if id is a new supporter.
    #[inline(always)]
    pub fn got_supporter(&mut self, id: Id, local_id: Id) -> bool {
        let new_id = self.supporters.add_id(id);
        if new_id && !self.support.creator_supported && id == self.data.op.info.id {
            self.support.creator_supported = true;
        }
        if new_id && !self.support.local_supported && id == local_id {
            self.support.local_supported = true;
        }
        new_id
    }
}

pub mod test_structs {
    use std::{
        collections::{HashMap, HashSet},
        hash::{self, Hash},
    };

    use bincode::DefaultOptions;
    use log::debug;

    use crate::{
        log::{
            basic_log::Log,
            local_log::{OpCreated, OpResult, SpCreated, SpExactToProcess, SpToProcess},
            op::{EntryInfo, Op, OpEntryInfo},
        },
        rw_buf::RWS,
        verification::{Id, TimeInfo},
    };

    use super::{LogOrdering, OrderedLog, OrderedLogContainer, OrderedSp, Result};

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

    pub fn check_ordered_logs<F: RWS, O: LogOrdering>(logs: &[&OrderedLogTest<F, O>]) {
        let prev_l = logs[0];
        for &l in &logs[1..] {
            // check the log test objects are equal by checking they contain eachother.
            l.check_other_contains_all(prev_l);
            prev_l.check_other_contains_all(l);
        }
    }

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

    impl<F: RWS, O: LogOrdering> OrderedLog<F, O> for OrderedLogTest<F, O> {
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
        fn get_sp_exact(&mut self, sp_c: SpCreated) -> Result<SpExactToProcess> {
            self.l.get_sp_exact(sp_c)
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
        ) -> Result<OrderedSp<O::Supporter>> {
            for op in sp_p.exact.iter() {
                self.after_recv_op(op);
            }
            let o_sp = self.l.received_sp_exact(ti, sp_p)?;
            self.after_recv_sp(&o_sp);
            Ok(o_sp)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DepBTree, DepHSet, DepVec, Dependents, SupBTree, SupHSet, SupVec, Supporters};

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
}
