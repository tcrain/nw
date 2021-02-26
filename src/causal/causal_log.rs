use std::{
    collections::{BTreeSet, VecDeque},
    error::Error,
    fmt::Display,
    iter::{repeat, repeat_with},
    mem,
};

use rustc_hash::{FxHashMap, FxHashSet};

use crate::{
    log::log_error::LogError,
    log::{local_log::LocalLog, op::OpEntryInfo, sp::SpInfo, LogIdx},
    rw_buf::RWS,
    verification::Id,
};

pub type HMap<K, V> = FxHashMap<K, V>;
pub type HSet<K> = FxHashSet<K>;

#[derive(Debug, PartialEq, Eq)]
pub enum CausalError {
    OpAlreadyCommitted,
    LogError(LogError),
}

impl Error for CausalError {}

impl Display for CausalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

/// TODO allow to change between implementations when different number of participants.
pub trait Dependents: Default {
    fn add_idxs<I: Iterator<Item = LogIdx>>(&mut self, i: I);
    fn add_idx(&mut self, idx: LogIdx);
    fn got_support(&mut self, idx: LogIdx) -> bool;
    fn remaining_idxs(&self) -> usize;
}

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
pub trait Supporters: Default {
    /// If the set did not have this id present, `true` is returned.
    ///
    /// If the set did have this id present, `false` is returned, and the
    /// entry is not updated.
    fn add_id(&mut self, id: Id) -> bool;
    fn get_count(&self) -> usize;
}

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

struct PendingOp<S: Supporters> {
    supporters: S, // IDs of nodes that have supported this op through SPs
    data: OpEntryInfo,
    local_supported: bool,
    dependent_sps: Option<Vec<LogIdx>>, // SPs that depend on this op
}

impl<S: Supporters> PendingOp<S> {
    #[inline(always)]
    fn new(data: OpEntryInfo) -> Self {
        PendingOp {
            supporters: S::default(), // ids SPs that have supported us
            data,
            local_supported: false,
            dependent_sps: Some(vec![]), // sps that are waiting until we have local support
        }
    }

    #[inline(always)]
    fn is_completed(&self, commit_count: usize) -> bool {
        self.local_supported && self.supporters.get_count() >= commit_count
    }

    #[inline(always)]
    fn add_dependent_sp(&mut self, log_idx: LogIdx) {
        self.dependent_sps.as_mut().unwrap().push(log_idx);
    }

    #[inline(always)]
    fn got_supporter(&mut self, id: Id) -> bool {
        let new_id = self.supporters.add_id(id);
        if new_id && !self.local_supported && id == self.data.op.info.basic.id {
            self.local_supported = true;
        }
        new_id
    }

    #[inline(always)]
    fn local_supported(&self) -> bool {
        self.local_supported
    }
}

enum PendingEntry<S: Supporters, D: Dependents> {
    Unknown,
    Completed,
    Sp(PendingSp<D>),
    Op(PendingOp<S>),
}

impl<S: Supporters, D: Dependents> PendingEntry<S, D> {
    #[inline(always)]
    fn completed(&mut self) -> PendingEntry<S, D> {
        mem::replace(self, PendingEntry::Completed)
    }

    #[inline(always)]
    fn is_unknown(&self) -> bool {
        matches!(*self, PendingEntry::Unknown)
    }
    #[inline(always)]
    fn is_completed(&self) -> bool {
        matches!(*self, PendingEntry::Completed)
    }
    #[inline(always)]
    fn is_sp(&self) -> bool {
        matches!(*self, PendingEntry::Sp(_))
    }
    #[inline(always)]
    fn is_op(&self) -> bool {
        matches!(*self, PendingEntry::Op(_))
    }
    #[inline(always)]
    fn as_op_mut(&mut self) -> &mut PendingOp<S> {
        match self {
            PendingEntry::Op(op) => op,
            _ => panic!("expected op"),
        }
    }
    #[inline(always)]
    fn unwrap_op(self) -> PendingOp<S> {
        match self {
            PendingEntry::Op(op) => op,
            _ => panic!("expected op"),
        }
    }
    #[inline(always)]
    fn as_sp_mut(&mut self) -> &mut PendingSp<D> {
        match self {
            PendingEntry::Sp(sp) => sp,
            _ => panic!("expected op"),
        }
    }
    #[inline(always)]
    fn unwrap_ref_sp(&mut self) -> &PendingSp<D> {
        match self {
            PendingEntry::Sp(sp) => sp,
            _ => panic!("expected op"),
        }
    }

    /// returns a log index if the entry is pending, otherwise returns None
    #[inline(always)]
    fn get_pending_log_idx(&self) -> Option<LogIdx> {
        match self {
            PendingEntry::Unknown => None,
            PendingEntry::Completed => None,
            PendingEntry::Sp(sp) => sp.data.as_ref().map(|d| d.log_index),
            PendingEntry::Op(op) => Some(op.data.log_index),
        }
    }
}

enum PendingSPState<D: Dependents> {
    Pending(PendingSp<D>),
    Ready,
}

struct PendingSp<D: Dependents> {
    data: Option<SpInfo>,
    prev_completed: bool,
    pending_ops: Option<D>, // log indices of non-local ops that this SP depends on
    local_pending_ops: Option<Vec<LogIdx>>, // log indicies of local ops that this SP depends on,
    // must be in the same order as given in the SP as they will be applied in that order
    dependent_sps: Option<Vec<LogIdx>>, // log indices of the SPs that depend on this sp
}

impl<D: Dependents> PendingSp<D> {
    fn is_completed(&self) -> bool {
        self.prev_completed
            && self
                .pending_ops
                .as_ref()
                .map_or(true, |po| po.remaining_idxs() == 0)
    }
}

enum PendingOpState<S: Supporters> {
    Completed, // when supported by creator SP, and have commit_count supporters
    PendingOp(PendingOp<S>),
}

impl<'a, S: Supporters> PendingOpState<S> {
    #[inline(always)]
    fn completed(&mut self) -> PendingOp<S> {
        match mem::replace(self, PendingOpState::Completed) {
            PendingOpState::PendingOp(op) => op,
            _ => panic!("expected op"),
        }
    }

    /*
    fn take_completed_op(&mut self) -> PendingOp<S> {
        match mem::replace(self, PendingEntry::Unknown) {
            PendingEntry::PendingOp(op) => op,
            _ => panic!("expected op"),
        }
    }*/

    /* fn completed(&mut self) {
        match *self {
            PendingEntry::PendingOp(op) => *self = PendingEntry::Completed(op),
            _ => panic!("expected pending op"),
        }
    }*/

    #[inline(always)]
    fn is_completed(&self) -> bool {
        matches!(*self, PendingOpState::Completed)
    }
    #[inline(always)]
    fn is_pending_op(&self) -> bool {
        matches!(*self, PendingOpState::PendingOp(_))
    }

    #[inline(always)]
    fn as_pending_op_mut(&mut self) -> &mut PendingOp<S> {
        match self {
            PendingOpState::PendingOp(p) => p,
            _ => panic!("expected pending op"),
        }
    }
}

enum GetOp {
    LogIndex(LogIdx),
    OpData(OpEntryInfo),
}

struct PendingEntries<S: Supporters, D: Dependents> {
    by_log_index: VecDeque<PendingEntry<S, D>>,
    ordered: VecDeque<LogIdx>, // maps to log_index, entries ordered in causal order
    commit_count: usize,       // number of supporters needed to commit
    last_committed: LogIdx,    // log entries start at index 1
                               // old_ops: HMap<u64, PendingOpState<S>>, // ops that are no longer in log_index TODO
}

impl<'a, S: Supporters, D: Dependents> PendingEntries<S, D> {
    fn new(commit_count: usize) -> Self {
        let by_log_index = VecDeque::new();
        // by_log_index.push_back(PendingEntry::Completed); // we have nothing at the first index
        PendingEntries {
            by_log_index,
            ordered: VecDeque::new(),
            commit_count,
            last_committed: 0,
            // old_ops: HMap::default(), // TODO
        }
    }

    fn process_commited(&mut self) -> Vec<PendingOp<S>> {
        let min_idx = self.min_idx();
        let by_log_index = &mut self.by_log_index;
        let mut committed = vec![];
        for &log_idx in self.ordered.iter() {
            let pe = &mut by_log_index[log_idx.checked_sub(min_idx).unwrap() as usize];
            match pe {
                PendingEntry::Unknown => break,
                PendingEntry::Sp(_) => panic!("sp should not be ordered by ops"),
                PendingEntry::Completed => panic!("should have consumed completed"),
                PendingEntry::Op(po) => {
                    debug_assert_eq!(log_idx, po.data.log_index);
                    if po.is_completed(self.commit_count) {
                        committed.push(pe.completed().unwrap_op());
                    } else {
                        break;
                    }
                }
            }
        }
        // remove committed entries form both dequeues
        self.ordered.drain(..committed.len());
        let mut remove_until = 0;
        for (i, nxt) in by_log_index.iter().enumerate() {
            if !nxt.is_completed() {
                break;
            }
            self.last_committed += 1;
            remove_until = i;
        }
        by_log_index.drain(..remove_until);
        committed
    }

    #[inline(always)]
    fn min_idx(&self) -> u64 {
        // self.by_log_index.first().map(|f| f.data.log_index)
        self.last_committed
    }

    fn sp_got_op_support(&mut self, sp_idx: LogIdx, op_idx: LogIdx) -> Result<(), CausalError> {
        // prev_completed: bool,
        // pending_ops: D, // log indices of non-local ops that this SP depends on
        // local_pending_ops: Option<Vec<LogIdx>>, // log indicies of local ops that this SP depends on,
        // // must be in the same order as given in the SP as they will be applied in that order
        // dependent_sps: Option<Vec<LogIdx>>, // log indices of the SPs that depend on this sp

        let completed = {
            let sp = self.get_sp_by_idx(sp_idx)?;
            let found = sp.pending_ops.as_mut().unwrap().got_support(op_idx);
            debug_assert!(found);
            sp.is_completed()
        };
        if completed {
            self.sp_completed(sp_idx);
        }
        Ok(())
    }

    /// Called when intitially processesing an Sp for each op it supports.
    /// Returns true if the op has support from a local Sp, false otherwise.
    fn op_supported(
        &mut self,
        op_data: GetOp,
        supporter: Id,
        supporter_idx: LogIdx,
        supporter_prev_ready: bool,
    ) -> Result<bool, CausalError> {
        //supporters: S, // IDs of nodes that have supported this op through SPs
        //data: OpEntryInfo,
        //local_supported: bool,
        //dependent_sps: Option<Vec<LogIdx>>, // SPs that depend on this op

        let op_log_idx = match &op_data {
            GetOp::LogIndex(idx) => *idx,
            GetOp::OpData(info) => info.log_index,
        };
        let (added_local_support, has_local_support, dependent_sps) = {
            let op = match op_data {
                GetOp::LogIndex(idx) => self.get_op_by_idx(idx)?,
                GetOp::OpData(data) => self.get_op(data)?,
            };
            // check if the supporter is the op creator
            if supporter == op.data.op.info.basic.id {
                if !op.local_supported() && supporter_prev_ready {
                    // the op is in causal order
                    // let the dependent SPs know the op is supported
                    (true, true, op.dependent_sps.take())
                } else {
                    (false, false, None)
                }
            } else {
                // the op has a new supporter
                op.got_supporter(supporter);
                if !op.local_supported() {
                    // if the op is not yet supported locally, the supporter is now dependent on the op
                    op.add_dependent_sp(supporter_idx);
                    (false, false, None)
                } else {
                    (false, true, None)
                }
            }
        };
        if added_local_support {
            // the op is in causal order
            self.ordered.push_back(op_log_idx);
        }

        if let Some(dependent_sps) = dependent_sps {
            // let mut completed_sps = vec![];
            for sp_idx in dependent_sps {
                let _ = self.sp_got_op_support(sp_idx, op_log_idx);
            }
        }
        Ok(has_local_support)
    }

    fn sp_prev_completed(&mut self, sp_idx: LogIdx) -> Result<(), CausalError> {
        let (completed, local_pending, sp_id) = {
            let sp = self.get_sp_by_idx(sp_idx)?;
            // update the sp
            sp.prev_completed = true;
            (
                sp.is_completed(),
                sp.local_pending_ops.take(),
                sp.data.unwrap().id,
            )
        };
        if let Some(local_pending) = local_pending {
            // let the local ops know they are supported locally
            for op_idx in local_pending {
                let _ = self.op_supported(GetOp::LogIndex(op_idx), sp_id, sp_idx, true);
            }
        }
        if completed {
            self.sp_completed(sp_idx);
        }
        Ok(())
    }

    // swaps out the entry at the given index for PendingOps::Completed
    fn completed_index(&mut self, idx: LogIdx) -> Result<PendingEntry<S, D>, CausalError> {
        let entry_idx = self.get_entry_idx(idx)?;
        Ok(self.by_log_index[entry_idx].completed())
    }

    fn sp_completed(&mut self, sp_idx: LogIdx) {
        // prev_completed: bool,
        // pending_ops: D, // log indices of non-local ops that this SP depends on
        // local_pending_ops: Option<Vec<LogIdx>>, // log indicies of local ops that this SP depends on,
        // // must be in the same order as given in the SP as they will be applied in that order
        // dependent_sps: Option<Vec<LogIdx>>, // log indices of the SPs that depend on this sp
        let dependent_sps = {
            let sp = self.get_sp_by_idx(sp_idx).unwrap();
            debug_assert!(sp.is_completed());
            sp.dependent_sps.take()
        };
        let _ = self.completed_index(sp_idx).unwrap();
        if let Some(dependent_sps) = dependent_sps {
            for dep_idx in dependent_sps {
                let _ = self.sp_prev_completed(dep_idx);
            }
        }
    }

    fn recv_sp(&mut self, info: SpInfo, deps: Vec<OpEntryInfo>) {
        let prev_sp = self.get_sp_by_idx(info.prev_sp_log_index);
        let prev_completed = match prev_sp {
            Ok(psp) => {
                psp.dependent_sps.as_mut().unwrap().push(info.log_index);
                false
            }
            Err(err) => {
                if err == CausalError::OpAlreadyCommitted {
                    true
                } else {
                    panic!(err)
                }
            }
        };
        debug_assert!(!deps.is_empty());
        // prev_completed: bool,
        // pending_ops: D, // log indices of non-local ops that this SP depends on
        // local_pending_ops: Option<Vec<LogIdx>>, // log indicies of local ops that this SP depends on,
        // // must be in the same order as given in the SP as they will be applied in that order
        // dependent_sps: Option<Vec<LogIdx>>, // log indices of the SPs that depend on this sp

        let (mut local_pending_ops, mut pending_ops) = if !prev_completed {
            (Some(vec![]), Some(D::default()))
        } else {
            (None, None)
        };
        for op_info in deps {
            let op_id = op_info.op.info.basic.id;
            let op_idx = op_info.log_index;
            // if op_supported returns an error, then the op has already completed, so it must have support
            let op_has_local_support = self
                .op_supported(
                    GetOp::OpData(op_info),
                    info.id,
                    info.log_index,
                    prev_completed,
                )
                .unwrap_or(true);
            if op_id == info.id {
                // if the sp and op have the same owner, then we track it as a local op only if the prev sp is not completed
                if !prev_completed {
                    local_pending_ops.as_mut().unwrap().push(op_idx);
                }
            } else if !op_has_local_support {
                // the op does not have support from a local Sp, so we are not ready
                pending_ops.as_mut().unwrap().add_idx(op_idx);
            }
        }

        let sp_idx = info.log_index;
        let completed = {
            let sp = self
                .get_sp(info)
                .expect("sould always generate SP on reception");
            sp.local_pending_ops = local_pending_ops;
            sp.pending_ops = pending_ops;
            sp.is_completed()
        };
        if completed {
            self.sp_completed(sp_idx);
        }
    }

    /// Called when an op is received.
    #[inline(always)]
    fn recv_op(&mut self, op: OpEntryInfo) -> Result<(), CausalError> {
        self.get_op(op).and(Ok(()))
    }

    #[inline(always)]
    fn get_op(&mut self, op: OpEntryInfo) -> Result<&mut PendingOp<S>, CausalError> {
        let vec_idx = self.get_entry_idx(op.log_index)?;
        let entry = &mut self.by_log_index[vec_idx];
        if entry.is_unknown() {
            *entry = PendingEntry::Op(PendingOp::new(op));
        }
        Ok(entry.as_op_mut())
    }

    #[inline(always)]
    fn get_op_by_idx(&mut self, log_index: LogIdx) -> Result<&mut PendingOp<S>, CausalError> {
        let vec_idx = self.get_entry_idx(log_index)?;
        let entry = &mut self.by_log_index[vec_idx];
        debug_assert!(entry.is_op());
        Ok(entry.as_op_mut())
    }

    #[inline(always)]
    fn get_sp(&mut self, sp: SpInfo) -> Result<&mut PendingSp<D>, CausalError> {
        let vec_idx = self.get_entry_idx(sp.log_index)?;
        let entry = &mut self.by_log_index[vec_idx];
        match entry {
            PendingEntry::Unknown => {
                *entry = PendingEntry::Sp(PendingSp {
                    data: Some(sp),
                    prev_completed: false,
                    pending_ops: None,
                    dependent_sps: Some(vec![]),
                    local_pending_ops: None,
                })
            }
            PendingEntry::Sp(psp) => psp.data = Some(sp),
            _ => panic!("expected sp"),
        }
        Ok(entry.as_sp_mut())
    }

    #[inline(always)]
    fn get_sp_by_idx(&mut self, log_index: LogIdx) -> Result<&mut PendingSp<D>, CausalError> {
        let vec_idx = self.get_entry_idx(log_index)?;
        let entry = &mut self.by_log_index[vec_idx];
        if entry.is_unknown() {
            panic!("should not be uknown state if we know the index");
            /* entry = PendingEntry::Sp(PendingSp {
                data: None,
                prev_completed: false,
                pending_ops: D::default(),
                dependent_sps: Some(vec![]),
                local_pending_ops: None,
            }); */
        }
        Ok(entry.as_sp_mut())
    }

    /// Call when an operation gets support from an external SP with a different id
    /// than the one that created the op.
    #[inline(always)]
    fn get_entry_idx(&mut self, log_index: LogIdx) -> Result<usize, CausalError> {
        let vec_max = self.min_idx() + self.by_log_index.len() as LogIdx;
        if log_index >= vec_max {
            // we need to extend the vector
            let diff = log_index - vec_max;
            self.by_log_index
                .extend(repeat_with(|| PendingEntry::Unknown).take(diff as usize))
        }
        let min_idx = self.min_idx();
        if log_index <= min_idx {
            return Err(CausalError::OpAlreadyCommitted);
        }
        let vec_idx = (log_index - min_idx) as usize;
        if let PendingEntry::Completed = self.by_log_index[vec_idx] {
            Err(CausalError::OpAlreadyCommitted)
        } else {
            Ok(vec_idx)
        }
    }
}

// (instead of support for SPs, has to be support for ops)

pub struct CausalLog<F: RWS, S: Supporters, D: Dependents> {
    l: LocalLog<F>,
    pending_entries: PendingEntries<S, D>,
    commit_count: usize, // number of supporters needed before an op/sp is committed
}

impl<F, S, D> CausalLog<F, S, D>
where
    F: RWS,
    S: Supporters,
    D: Dependents,
{
    fn new(l: LocalLog<F>, commit_count: usize) -> Self {
        CausalLog {
            l,
            pending_entries: PendingEntries::new(commit_count),
            commit_count,
        }
    }
}

/*
// receive an op - add it to log, keep as pending
// receive an sp - take all supported ops with the same id, and add return them for the state machine to process
//               - update the count for supported ops pending commit, any that match commit_count return as committed
//               - committed needs to replay ops in causal order -> just go by the order of SP processed?
//               - each SP receives a counter when first processed, then they must be committed in this order
pub struct Causal<F: RWS> {
    l: LocalLog<F>,
    sps_ready_for_commit: Vec<PendingSP>, // SPs with enough supporters, but SPs with smaller processed count not yet supported
    ops_pending_commit: HMap<EntryInfo, PendingSP>, // when we get commit_count for an entry, it is committed, map is SP -> ids of supporters
    commit_count: usize, // number of supporters needed before an op/sp is committed
    last_commited_sp: u64, // most recent index of the SP that has been committed
    last_processed_sp: u64, // most recent SP processed
    ops_pending_sp: HMap<Id, Vec<EntryInfoData>>, // when we get an SP from this item, we will process it
}

impl<F: RWS> Causal<F> {
    fn new(commit_count: usize, l: LocalLog<F>) -> Self {
        Causal {
            l,
            ops_pending_commit: HMap::default(),
            commit_count,
            ops_pending_sp: HMap::default(),
            sps_ready_for_commit: vec![],
            last_commited_sp: 0,
            last_processed_sp: 0,
        }
    }
}

*/

#[cfg(test)]
mod test {
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