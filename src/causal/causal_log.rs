use std::{
    collections::VecDeque,
    error::Error,
    fmt::{Debug, Display},
    iter::repeat_with,
    mem,
};

use log::debug;

use crate::{
    log::log_error::LogError,
    log::{
        local_log::LocalLog,
        op::OpEntryInfo,
        ordered_log::{self, Dependents, LogOrdering, OrderingError, PendingOp, Supporters},
        sp::SpInfo,
        LogIdx,
    },
    rw_buf::RWS,
    verification::Id,
};

#[derive(Debug, PartialEq, Eq)]
pub enum CausalError {
    EntryAlreadyCommitted,
    LogError(LogError),
}

impl Error for CausalError {}

impl Display for CausalError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
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
struct CausalOp<S: Supporters> {
    support: SupportInfo,
    dependent_sps: Option<Vec<LogIdx>>, // SPs that depend on this op
    p: PendingOp<S>,
}

impl<S: Supporters> CausalOp<S> {
    #[inline(always)]
    pub fn new(data: OpEntryInfo) -> Self {
        CausalOp {
            p: PendingOp::new(data),
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
        self.support.is_supported() && self.p.supporters.get_count() >= commit_count
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
        let new_id = self.p.supporters.add_id(id);
        if new_id && !self.support.creator_supported && id == self.p.data.op.info.id {
            self.support.creator_supported = true;
        }
        if new_id && !self.support.local_supported && id == local_id {
            self.support.local_supported = true;
        }
        new_id
    }
}

#[derive(Debug)]
enum PendingEntry<S: Supporters, D: Dependents> {
    Unknown,
    Completed,
    Sp(PendingSp<D>),
    Op(CausalOp<S>),
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
    fn as_op_mut(&mut self) -> &mut CausalOp<S> {
        match self {
            PendingEntry::Op(op) => op,
            _ => panic!(format!("expected op, {:?}", self)),
        }
    }
    #[inline(always)]
    fn unwrap_op(self) -> CausalOp<S> {
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
            PendingEntry::Op(op) => Some(op.p.data.log_index),
        }
    }
}

enum PendingSPState<D: Dependents> {
    Pending(PendingSp<D>),
    Ready,
}

#[derive(Debug)]
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

impl<S: Supporters> PendingOpState<S> {
    #[inline(always)]
    fn completed(&mut self) -> PendingOp<S> {
        match mem::replace(self, PendingOpState::Completed) {
            PendingOpState::PendingOp(op) => op,
            _ => panic!("expected op"),
        }
    }

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

pub struct PendingEntries<S: Supporters, D: Dependents> {
    id: Id,
    by_log_index: VecDeque<(LogIdx, PendingEntry<S, D>)>,
    ordered: VecDeque<LogIdx>, // maps to log_index, entries ordered in causal order
    commit_count: usize,       // number of supporters needed to commit
    last_committed: LogIdx,    // log entries start at index 1
                               // old_ops: HMap<u64, PendingOpState<S>>, // ops that are no longer in log_index TODO
}

impl<S: Supporters, D: Dependents> LogOrdering for PendingEntries<S, D> {
    type Supporter = S;

    /// Each call to recv_sp returns the operations in causal order that have been committed by
    /// the reception of the Sp (if there are any).
    /// Each Op must be supported by its creators Sp, the local Sp and a total of self.commit_count
    /// Sps.
    fn recv_sp<I: Iterator<Item = OpEntryInfo>>(
        &mut self,
        info: SpInfo,
        deps: I,
    ) -> ordered_log::Result<Vec<PendingOp<Self::Supporter>>> {
        let prev_sp = self.get_sp_by_idx(info.supported_sp_log_index.unwrap());
        let prev_completed = match prev_sp {
            Ok(psp) => {
                psp.dependent_sps.as_mut().unwrap().push(info.log_index);
                false
            }
            Err(err) => {
                if err == CausalError::EntryAlreadyCommitted {
                    true
                } else {
                    panic!(format!("{:?}", err))
                }
            }
        };

        let mut local_pending_ops = None;
        let mut pending_ops = None;
        let mut has_dep = false;
        let mut ids = vec![];
        for op_info in deps {
            has_dep = true;
            let op_id = op_info.op.info.id;
            ids.push(op_id);
            let op_idx = op_info.log_index;
            // if op_supported returns an error, then the op has already completed, so it must have support
            let op_support = self
                .op_supported(
                    GetOp::OpData(op_info),
                    info.id,
                    info.log_index,
                    prev_completed,
                    true,
                )
                .unwrap_or_else(|_| SupportInfo::new_supported());
            // if the sp and op have the same owner, or is a local supporter,
            // then we track it as a local op if the prev sp is not completed
            // or the op hasnt been supported both locally and by its creator
            if op_id == info.id && !op_support.creator_supported()
                || info.id == self.id && !op_support.local_supported()
            {
                local_pending_ops.get_or_insert_with(Vec::new).push(op_idx);
            }

            // if the op is not supported, then we need to wait until it receives support from
            // the local node and its creator before we process it
            if !op_support.is_supported() {
                // the op does not have support from both the local Sp and its creator, so we are not ready
                pending_ops.get_or_insert_with(D::default).add_idx(op_idx);
            }
        }
        debug!(
            "got sp from {}, with deps from {:?}, at my id {}",
            info.id, ids, self.id
        );
        debug_assert!(has_dep);

        let sp_idx = info.log_index;
        let completed = {
            let sp = self
                .get_sp(info)
                .expect("sould always generate SP on reception");
            sp.local_pending_ops = local_pending_ops;
            sp.prev_completed = prev_completed;
            sp.pending_ops = pending_ops;
            sp.is_completed()
        };
        if completed {
            self.sp_completed(sp_idx);
        }
        Ok(self.process_commited())
    }

    /// Called when an op is received.
    #[inline(always)]
    fn recv_op(&mut self, op: OpEntryInfo) -> ordered_log::Result<()> {
        self.get_op(op)
            .and(Ok(()))
            .map_err(|e| OrderingError::Custom(Box::new(e)))
    }
}

impl<S: Supporters, D: Dependents> PendingEntries<S, D> {
    fn new(commit_count: usize, id: Id) -> Self {
        let mut by_log_index = VecDeque::new();
        by_log_index.push_back((1, PendingEntry::Completed)); // the first log index is the initial SP
        PendingEntries {
            by_log_index,
            ordered: VecDeque::new(),
            commit_count,
            last_committed: 1,
            id,
            // old_ops: HMap::default(), // TODO
        }
    }

    fn process_commited(&mut self) -> Vec<PendingOp<S>> {
        let min_idx = self.min_idx();
        let by_log_index = &mut self.by_log_index;
        let mut committed = vec![];
        for &log_idx in self.ordered.iter() {
            let pe = &mut by_log_index[log_idx.checked_sub(min_idx).unwrap() as usize];
            assert_eq!(log_idx, pe.0);
            match &mut pe.1 {
                PendingEntry::Unknown => break,
                PendingEntry::Sp(_) => panic!("sp should not be ordered by ops"),
                PendingEntry::Completed => panic!("should have consumed completed"),
                PendingEntry::Op(po) => {
                    debug_assert_eq!(log_idx, po.p.data.log_index);
                    if po.is_completed(self.commit_count) {
                        committed.push(pe.1.completed().unwrap_op().p);
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
            if !nxt.1.is_completed() {
                break;
            }
            self.last_committed += 1;
            remove_until = i + 1;
        }
        by_log_index.drain(..remove_until);
        committed
    }

    #[inline(always)]
    fn min_idx(&self) -> u64 {
        // self.by_log_index.first().map(|f| f.data.log_index)
        self.last_committed
    }

    /// Called when an operation is supported by Sps from its creator and the local node.
    /// The Sp at sp_idx was previously received and depended on the the op at op_idx.
    fn sp_got_op_support(&mut self, sp_idx: LogIdx, op_idx: LogIdx) -> Result<(), CausalError> {
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
    /// Supporter is the Sp information.
    /// on_recv is true is this method is called when first receving an Sp.
    /// Otherwise is is called when the Sp has had its previous Sp completed,
    /// and is now informing the Op of this change.
    fn op_supported(
        &mut self,
        op_data: GetOp,
        supporter: Id,
        supporter_idx: LogIdx,
        supporter_prev_ready: bool,
        on_recv: bool,
    ) -> Result<SupportInfo, CausalError> {
        // if on_recv is false, then this is called for the Sp for a second time
        // when the Sp's prev Sp becomes ready
        debug_assert!((!on_recv && supporter_prev_ready) || on_recv);

        let my_id = self.id;
        let op_log_idx = match &op_data {
            GetOp::LogIndex(idx) => *idx,
            GetOp::OpData(info) => info.log_index,
        };
        let (added_support, support_info, dependent_sps) = {
            let op = match op_data {
                GetOp::LogIndex(idx) => self.get_op_by_idx(idx)?,
                GetOp::OpData(data) => self.get_op(data)?,
            };
            let prev_supported = op.get_support_info();
            let op_id = op.p.data.op.info.id;
            if supporter == op_id || supporter == my_id {
                // if creator/local support we also need the previous to be ready
                if supporter_prev_ready {
                    debug!(
                        "got creator/local support for {}, my id {}",
                        supporter, my_id
                    );
                    op.got_supporter(supporter, my_id);
                }
            } else {
                op.got_supporter(supporter, my_id);
            }
            let support_info = op.get_support_info();
            if !support_info.is_supported() && on_recv {
                // if the op is not ready, then the completion of the Sp is dependent on this Op.
                // if on_recv is false, then this is being called for a second time for the Sp
                // when it's previous is ready, so the Sp has already been added to this list
                op.add_dependent_sp(supporter_idx);
            }
            let added_support = prev_supported.is_supported() != support_info.is_supported();
            let dependent_sps = {
                if added_support {
                    // we need to inform the dependent Sps that support has been added
                    op.dependent_sps.take()
                } else {
                    None
                }
            };
            (added_support, support_info, dependent_sps)
        };
        if added_support {
            // the op is in causal order
            self.ordered.push_back(op_log_idx);
        }

        if let Some(dependent_sps) = dependent_sps {
            // let mut completed_sps = vec![];
            for sp_idx in dependent_sps {
                let _ = self.sp_got_op_support(sp_idx, op_log_idx);
            }
        }
        Ok(support_info)
    }

    fn sp_prev_completed(&mut self, sp_idx: LogIdx) -> Result<(), CausalError> {
        let (local_pending, sp_id) = {
            let sp = self.get_sp_by_idx(sp_idx)?;
            // update the sp
            sp.prev_completed = true;
            (sp.local_pending_ops.take(), sp.data.unwrap().id)
        };
        if let Some(local_pending) = local_pending {
            // let the local ops know they are supported locally
            for op_idx in local_pending {
                let _ = self.op_supported(GetOp::LogIndex(op_idx), sp_id, sp_idx, true, false);
            }
        }
        // now we can check if the SP is completed now that the ops have a new supporter
        if self.get_sp_by_idx(sp_idx)?.is_completed() {
            self.sp_completed(sp_idx);
        }
        Ok(())
    }

    #[inline(always)]
    fn check_idx(&self, idx: LogIdx, arr_idx: usize) {
        debug_assert_eq!(self.by_log_index[arr_idx].0, idx);
    }

    // swaps out the entry at the given index for PendingOps::Completed
    fn completed_index(&mut self, idx: LogIdx) -> Result<PendingEntry<S, D>, CausalError> {
        let entry_idx = self.get_entry_idx(idx)?;
        self.check_idx(idx, entry_idx);
        Ok(self.by_log_index[entry_idx].1.completed())
    }

    fn sp_completed(&mut self, sp_idx: LogIdx) {
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

    // #[inline(always)]
    fn get_op(&mut self, op: OpEntryInfo) -> Result<&mut CausalOp<S>, CausalError> {
        let vec_idx = self.get_entry_idx(op.log_index)?;
        self.check_idx(op.log_index, vec_idx);
        let entry = &mut self.by_log_index[vec_idx].1;
        if entry.is_unknown() {
            *entry = PendingEntry::Op(CausalOp::new(op));
        }
        Ok(entry.as_op_mut())
    }

    #[inline(always)]
    fn get_op_by_idx(&mut self, log_index: LogIdx) -> Result<&mut CausalOp<S>, CausalError> {
        let vec_idx = self.get_entry_idx(log_index)?;
        self.check_idx(log_index, vec_idx);
        let entry = &mut self.by_log_index[vec_idx].1;
        debug_assert!(entry.is_op());
        Ok(entry.as_op_mut())
    }

    #[inline(always)]
    fn get_sp(&mut self, sp: SpInfo) -> Result<&mut PendingSp<D>, CausalError> {
        let vec_idx = self.get_entry_idx(sp.log_index)?;
        self.check_idx(sp.log_index, vec_idx);
        let entry = &mut self.by_log_index[vec_idx].1;
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
        self.check_idx(log_index, vec_idx);
        let entry = &mut self.by_log_index[vec_idx].1;
        if entry.is_unknown() {
            panic!("should not be uknown state if we know the index");
        }
        Ok(entry.as_sp_mut())
    }

    /// Call when an operation gets support from an external SP with a different id
    /// than the one that created the op.
    #[inline(always)]
    fn get_entry_idx(&mut self, log_index: LogIdx) -> Result<usize, CausalError> {
        let vec_max = self.min_idx() + self.by_log_index.len() as LogIdx;
        debug!(
            "log_index {}, vec_max {}, min_idx {}, by_log_index.len {}",
            log_index,
            vec_max,
            self.min_idx(),
            self.by_log_index.len(),
        );
        if log_index >= vec_max {
            // we need to extend the vector
            let diff = log_index + 1 - vec_max;
            let mut nxt_idx = vec_max - 1;
            self.by_log_index.extend(
                repeat_with(|| {
                    nxt_idx += 1;
                    (nxt_idx, PendingEntry::Unknown)
                })
                .take(diff as usize),
            );
        }
        let min_idx = self.min_idx();
        if log_index < min_idx {
            return Err(CausalError::EntryAlreadyCommitted);
        }
        let vec_idx = (log_index - min_idx) as usize;
        debug!(
            "len {}, vec idx {}, log_index {}, min_idx {}",
            self.by_log_index.len(),
            vec_idx,
            log_index,
            min_idx
        );
        if let PendingEntry::Completed = self.by_log_index[vec_idx].1 {
            Err(CausalError::EntryAlreadyCommitted)
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
        let id = l.my_id();
        CausalLog {
            l,
            pending_entries: PendingEntries::new(commit_count, id),
            commit_count,
        }
    }
}

pub mod test_structs {
    use std::fmt::Debug;
    use std::fs::File;

    use bincode::{deserialize, serialize};
    use log::debug;
    use serde::{de::DeserializeOwned, Serialize};

    use crate::{
        causal::vector_clock::{VecClock, VectorClock},
        log::{
            basic_log::Log,
            local_log::{new_local_log, LocalLog},
            log_file::open_log_file,
            op::{to_op_data, OpData, OpEntryInfo},
            ordered_log::{
                test_structs::{OrderedLogTest, OrderedStateTest},
                DepVec, OrderedLogContainer, OrderedLogRun, OrderedState, OrderingError, Result,
                SupVec,
            },
        },
        rw_buf::RWS,
        verification::{Id, TimeTest},
    };

    use super::PendingEntries;

    // Implementation of OrderedState that tracks a vector clock.
    pub struct CausalLogClock<K: Debug, V: VectorClock<K = K, V = u64>> {
        pub(crate) c: V,
        pub(crate) c_all: V, // keeps max of all operaions added, to make sure we commit all operations in the end
        op_count: u64,
        id: K,
    }

    impl<K: Debug, V: DeserializeOwned + VectorClock<K = K, V = u64>> CausalLogClock<K, V> {
        fn update_c_all(&mut self, data: &OpData) {
            let v: V = deserialize(data).unwrap();
            self.c_all.max_in_place(&v);
        }
    }

    impl<K: Debug + Clone, V: Clone + Serialize + VectorClock<K = K, V = u64>> OrderedStateTest
        for CausalLogClock<K, V>
    {
        fn create_local_op(&mut self) -> Result<OpData> {
            self.op_count += 1;
            let mut c = self.c.clone();
            c.set_entry(self.id.clone(), self.op_count);
            self.c_all.max_in_place(&c);
            Ok(to_op_data(serialize(&c).unwrap()))
        }
    }

    impl<K: Debug, V: Debug + DeserializeOwned + VectorClock<K = K, V = u64>> OrderedState
        for CausalLogClock<K, V>
    {
        fn check_op(&mut self, op: &OpData) -> Result<()> {
            // just check is deserialzes ok
            match deserialize::<VecClock<Id, u64>>(op) {
                Ok(_) => Ok(()),
                Err(e) => Err(OrderingError::Custom(e)),
            }
        }

        fn received_op(&mut self, op: &OpEntryInfo) {
            self.update_c_all(&op.op.data);
        }

        fn after_recv_sp<'a, I: Iterator<Item = &'a OpEntryInfo>>(&mut self, from: Id, deps: I) {
            for op in deps {
                let v: V = deserialize(&op.op.data).unwrap();
                debug!(
                    "got vec {:?} from {}, my id {:?}, mine {:?}",
                    v, from, self.id, self.c
                );
                if self.c > v {
                    panic!("received clock {:?}, when already at {:?}", v, self.c);
                }
                self.c.max_in_place(&v);
            }
        }
    }

    pub type CausalTestLog<F> = OrderedLogRun<
        OrderedLogTest<F, PendingEntries<SupVec, DepVec>>,
        CausalLogClock<Id, VecClock<Id, u64>>,
    >;

    pub type CausalTestOrderedLog<F> = OrderedLogRun<
        OrderedLogContainer<F, PendingEntries<SupVec, DepVec>>,
        CausalLogClock<Id, VecClock<Id, u64>>,
    >;

    pub type CausalTestSetup<F> = (
        LocalLog<F>,
        CausalLogClock<Id, VecClock<Id, u64>>,
        PendingEntries<SupVec, DepVec>,
    );

    fn new_causal_setup<F: RWS, G: Fn(File) -> F + Copy>(
        id: Id,
        test_idx: usize,
        commit_count: usize,
        open_fn: G,
    ) -> CausalTestSetup<F> {
        let f = new_local_log(
            id,
            Log::new(
                open_log_file(
                    &format!("log_files/causal_log{}_{}.log", test_idx, id),
                    true,
                    open_fn,
                )
                .unwrap(),
            ),
        )
        .unwrap();
        let pe = PendingEntries::new(commit_count, id);
        let c = CausalLogClock {
            c: VecClock::default(),
            c_all: VecClock::default(),
            op_count: 0,
            id,
        };
        (f, c, pe)
    }

    pub(crate) fn new_causal_ordered_test<F: RWS, G: Fn(File) -> F + Copy>(
        id: Id,
        test_idx: usize,
        commit_count: usize,
        open_fn: G,
    ) -> (CausalTestLog<F>, TimeTest) {
        let (f, c, pe) = new_causal_setup(id, test_idx, commit_count, open_fn);
        let l = OrderedLogTest::new(OrderedLogContainer::new(f, pe));
        (OrderedLogRun::new(id, l, c), TimeTest::new())
    }

    pub fn new_causal_test<F: RWS, G: Fn(File) -> F + Copy>(
        id: Id,
        test_idx: usize,
        commit_count: usize,
        open_fn: G,
    ) -> (CausalTestOrderedLog<F>, TimeTest) {
        let (f, c, pe) = new_causal_setup(id, test_idx, commit_count, open_fn);
        let l = OrderedLogContainer::new(f, pe);
        (OrderedLogRun::new(id, l, c), TimeTest::new())
    }
}

#[cfg(test)]
mod test {
    use log::debug;
    use std::time::Instant;

    use crate::{
        log::ordered_log::{
            test_structs::{
                check_ordered_logs, run_ordered_once, run_ordered_rand, run_ordered_standard,
            },
            OrderedLog,
        },
        rw_buf::{RWBuf, RWS},
        verification::TimeTest,
    };

    use super::test_structs::{new_causal_ordered_test, CausalTestLog};

    #[test]
    fn causal_single() {
        let num_logs = 4;
        // an op is not committed until it is supported by 3 nodes
        let commit_count = 3;
        let mut logs = vec![];
        for id in 0..num_logs {
            logs.push(new_causal_ordered_test(
                id,
                id as usize,
                commit_count,
                RWBuf::new,
            ));
        }
        run_ordered_once(&mut logs);
        check_logs(&mut logs);
    }

    #[test]
    fn causal_rand() {
        let num_ops = 20;
        for seed in 100..110 {
            for num_logs in 2..4 {
                for commit_count in 1..=num_logs as usize {
                    let mut logs = vec![];
                    for id in 0..num_logs {
                        logs.push(new_causal_ordered_test(
                            id,
                            100 + id as usize,
                            commit_count,
                            RWBuf::new,
                        ));
                    }
                    run_ordered_rand(&mut logs, num_ops, seed);
                    // check the logs have the same vector clock
                    check_logs(&mut logs);
                }
            }
        }
    }

    #[test]
    fn causal_standard() {
        let num_iterations = 3;
        let num_ops = 20;
        let start = Instant::now();
        for seed in 100..110 {
            for num_logs in 2..4 {
                for commit_count in 1..=num_logs as usize {
                    let mut logs = vec![];
                    for id in 0..num_logs {
                        logs.push(new_causal_ordered_test(
                            id,
                            200 + id as usize,
                            commit_count,
                            RWBuf::new,
                        ));
                    }
                    run_ordered_standard(&mut logs, num_ops, num_iterations, seed);
                    // check the logs have the same vector clock
                    check_logs(&mut logs);
                }
            }
        }
        debug!("test took {:?}", start.elapsed());
    }

    fn check_logs<F: RWS>(logs: &mut [(CausalTestLog<F>, TimeTest)]) {
        for (i, (l, _)) in logs.iter_mut().enumerate() {
            debug!(
                "\nPending entries in log {}: {:?}\n",
                i,
                l.get_l().get_ordering().by_log_index
            );
            // debug!("the log is: ");
            // print_log_from_end(l.get_l_mut().get_log_mut());
        }

        let mut prev_vec = None;
        for (l, _) in logs.iter() {
            if let Some(p) = prev_vec.take() {
                assert_eq!(p, l.get_s().c);
                assert_eq!(p, l.get_s().c_all);
                debug!("{:?}, {:?}", p, l.get_s().c);
            }
            prev_vec = Some(l.get_s().c.clone())
        }
        let test_logs: Vec<_> = logs.iter().map(|(l, _)| l.get_l()).collect();
        check_ordered_logs(&test_logs);
    }
}
