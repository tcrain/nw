use std::{
    cmp::Ordering,
    collections::VecDeque,
    error::Error,
    fmt::{Debug, Display},
    iter::{repeat, repeat_with},
    mem,
};

use log::debug;
use serde::{Deserialize, Serialize};

use crate::{
    log::log_error::LogError,
    log::{
        local_log::LocalLog,
        op::OpEntryInfo,
        ordered_log::{
            self, Dependents, LogOrdering, OrderingError, PendingOp, SupportInfo, Supporters,
        },
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

#[derive(Debug)]
enum PendingEntry<S: Supporters, D: Dependents> {
    Unknown,
    Completed,
    Sp(PendingSp<D>),
    Op(PendingOp<S>),
}

/*
impl<S: Supporters, D: Dependents> Debug for PendingEntry<S, D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let str = match self {
            PendingEntry::Unknown => "Unknown",
            PendingEntry::Completed => "Completed",
            PendingEntry::Sp(_) => "Sp",
            PendingEntry::Op(_) => "Op",
        };
        f.write_str(str)
    }
}
*/

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
            _ => panic!(format!("expected op, {:?}", self)),
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
    id: Id,
    by_log_index: VecDeque<(LogIdx, PendingEntry<S, D>)>,
    ordered: VecDeque<LogIdx>, // maps to log_index, entries ordered in causal order
    commit_count: usize,       // number of supporters needed to commit
    last_committed: LogIdx,    // log entries start at index 1
                               // old_ops: HMap<u64, PendingOpState<S>>, // ops that are no longer in log_index TODO
}

impl<S: Supporters, D: Dependents> LogOrdering for PendingEntries<S, D> {
    type Supporter = S;

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
                    panic!(err)
                }
            }
        };
        // prev_completed: bool,
        // pending_ops: D, // log indices of non-local ops that this SP depends on
        // local_pending_ops: Option<Vec<LogIdx>>, // log indicies of local ops that this SP depends on,
        // // must be in the same order as given in the SP as they will be applied in that order
        // dependent_sps: Option<Vec<LogIdx>>, // log indices of the SPs that depend on this sp

        let mut local_pending_ops = None;
        let mut pending_ops = None;

        // let (mut local_pending_ops, mut pending_ops) = if !prev_completed {
        //     (Some(vec![]), Some(D::default()))
        // } else {
        //     (None, None)
        // };
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
                || op_id == self.id && !op_support.local_supported()
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
        println!(
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
                    debug_assert_eq!(log_idx, po.data.log_index);
                    if po.is_completed(self.commit_count) {
                        committed.push(pe.1.completed().unwrap_op());
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
        //supporters: S, // IDs of nodes that have supported this op through SPs
        //data: OpEntryInfo,
        //local_supported: bool,
        //dependent_sps: Option<Vec<LogIdx>>, // SPs that depend on this op

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
            let op_id = op.data.op.info.id;
            if supporter == op_id || my_id == op_id {
                // if creator/local support we also need the previous to be ready
                if supporter_prev_ready {
                    println!(
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
            // let pre_support = op.get_support_info();
            // if supporter == op.data.op.info.id {
            //     if !op.support.local_supported() && supporter_prev_ready {
            //         op.got_supporter(supporter, self.id);
            //         // the op is in causal order
            //         // let the dependent SPs know the op is supported
            //         println!("got local support for {}, my id {}", supporter, my_id);
            //         (true, true, op.dependent_sps.take())
            //     } else {
            //         (false, false, None)
            //     }
            // } else {
            //     // the op has a new supporter
            //     op.got_supporter(supporter, self.id);
            //     if !op.local_supported() {
            //         // if the op is not yet supported locally, the supporter is now dependent on the op
            //         op.add_dependent_sp(supporter_idx);
            //         (false, false, None)
            //     } else {
            //         (false, true, None)
            //     }
            // }
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

    #[inline(always)]
    fn get_op(&mut self, op: OpEntryInfo) -> Result<&mut PendingOp<S>, CausalError> {
        let vec_idx = self.get_entry_idx(op.log_index)?;
        self.check_idx(op.log_index, vec_idx);
        let entry = &mut self.by_log_index[vec_idx].1;
        if entry.is_unknown() {
            *entry = PendingEntry::Op(PendingOp::new(op));
        }
        Ok(entry.as_op_mut())
    }

    #[inline(always)]
    fn get_op_by_idx(&mut self, log_index: LogIdx) -> Result<&mut PendingOp<S>, CausalError> {
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

#[derive(Serialize, Deserialize, Debug, Clone)]
struct VecClock(Vec<u64>);

impl Default for VecClock {
    fn default() -> Self {
        VecClock(vec![])
    }
}

impl VecClock {
    fn from_vec(vec: Vec<u64>) -> Self {
        VecClock(vec)
    }

    #[inline(always)]
    fn get_val(&self, id: Id) -> u64 {
        if id as usize >= self.0.len() {
            return 0;
        }
        self.0[id as usize]
    }

    #[inline(always)]
    fn set_entry(&mut self, id: Id, val: u64) {
        let l = self.0.len();
        if id as usize >= l {
            self.0.extend(repeat(0).take((id + 1) as usize - l));
        }
        self.0[id as usize] = val;
    }

    fn max_in_place(&mut self, other: &Self) {
        let l = self.0.len();
        let other_l = other.0.len();
        if other_l > l {
            self.0.extend(repeat(0).take(other_l - l));
        }
        for (o_v, s_v) in other.0.iter().cloned().zip(self.0.iter_mut()) {
            if o_v > *s_v {
                *s_v = o_v;
            }
        }
    }
}

impl PartialEq for VecClock {
    #[inline(always)]
    fn eq(&self, other: &Self) -> bool {
        self.partial_cmp(other)
            .map_or(false, |o| matches!(o, Ordering::Equal))
    }
}

fn check_eq_iter<I: Iterator<Item = (u64, u64)>>(iter: I) -> Option<Ordering> {
    let mut res = None;
    for (l, r) in iter {
        match l.cmp(&r) {
            Ordering::Less => {
                if let Some(o) = res {
                    if matches!(o, Ordering::Greater) {
                        return None;
                    }
                }
                res = Some(Ordering::Less)
            }
            Ordering::Equal => {
                let _ = res.get_or_insert(Ordering::Equal);
            }
            Ordering::Greater => {
                if let Some(o) = res {
                    if matches!(o, Ordering::Less) {
                        return None;
                    }
                }
                res = Some(Ordering::Greater)
            }
        }
    }
    res
}

impl PartialOrd for VecClock {
    #[inline(always)]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let dif = self.0.len() as isize - other.0.len() as isize;
        match dif.cmp(&0) {
            Ordering::Equal => check_eq_iter(self.0.iter().cloned().zip(other.0.iter().cloned())),
            Ordering::Less => check_eq_iter(
                self.0
                    .iter()
                    .cloned()
                    .chain(repeat(0).take(dif.abs() as usize))
                    .zip(other.0.iter().cloned()),
            ),
            Ordering::Greater => check_eq_iter(
                self.0
                    .iter()
                    .cloned()
                    .zip(other.0.iter().cloned().chain(repeat(0).take(dif as usize))),
            ),
        }
    }
}

#[cfg(test)]
mod test {
    use bincode::{deserialize, serialize, DefaultOptions};
    use log::debug;
    use rand::{
        distributions::Standard,
        prelude::{Distribution, StdRng},
        Rng, SeedableRng,
    };
    use std::{collections::VecDeque, fs::File};

    use crate::{
        log::{
            basic_log::test_fns::print_log_from_end,
            basic_log::Log,
            local_log::{
                new_local_log, OpCreated, OpResult, SpDetails, SpExactToProcess, SpToProcess,
            },
            log_error::LogError,
            log_file::open_log_file,
            op::{to_op_data, Op, OpData},
            ordered_log::{DepVec, OrderedLog, OrderingError, Result, SupVec},
        },
        rw_buf::RWBuf,
        utils,
        verification::{Id, TimeTest},
    };

    use super::{PendingEntries, VecClock};

    #[test]
    fn vec_clock() {
        assert!(VecClock::from_vec(vec![1, 2, 3, 4]) == VecClock::from_vec(vec![1, 2, 3, 4]));
        assert!(
            VecClock::from_vec(vec![1, 2, 3, 4]) == VecClock::from_vec(vec![1, 2, 3, 4, 0, 0, 0])
        );
        assert!(VecClock::from_vec(vec![2, 2, 3, 4]) >= VecClock::from_vec(vec![1, 2, 3, 4]));
        assert!(VecClock::from_vec(vec![2, 2, 3, 4, 5]) > VecClock::from_vec(vec![1, 2, 3, 4]));
        assert!(VecClock::from_vec(vec![1, 2, 3, 4]) > VecClock::from_vec(vec![]));
        assert!(VecClock::from_vec(vec![]) < VecClock::from_vec(vec![1, 2, 3, 4]));

        assert!(VecClock::from_vec(vec![1, 2])
            .partial_cmp(&VecClock::from_vec(vec![2, 1]))
            .is_none());
        assert!(VecClock::from_vec(vec![1, 1, 1])
            .partial_cmp(&VecClock::from_vec(vec![2, 2]))
            .is_none());

        let mut v1 = VecClock::default();
        let mut v2 = VecClock::default();
        for (i, v) in (0..10).rev().enumerate() {
            v1.set_entry(v, i as u64);
            v2.set_entry(i as u64, v);
        }
        assert!(v1 == v2);

        let mut v1 = VecClock::from_vec(vec![1, 2, 3, 4]);
        let v2 = VecClock::from_vec(vec![4, 1, 1, 1, 1]);
        v1.max_in_place(&v2);
        assert_eq!(v1, VecClock::from_vec(vec![4, 2, 3, 4, 1]));
    }

    struct CausalLog {
        l: OrderedLog<RWBuf<File>, PendingEntries<SupVec, DepVec>>,
        c: VecClock,
        c_all: VecClock, // keeps max of all operaions added, to make sure we commit all operations in the end
        op_count: u64,
        id: Id,
        ti: TimeTest,
    }

    impl CausalLog {
        fn new(id: Id, test_idx: usize, commit_count: usize) -> Self {
            let f = new_local_log(
                id,
                Log::new(
                    open_log_file(
                        &format!("log_files/causal_log{}_{}.log", test_idx, id),
                        true,
                        RWBuf::new,
                    )
                    .unwrap(),
                ),
            )
            .unwrap();
            let pe = PendingEntries::new(commit_count, id);
            let l = OrderedLog::new(f, pe);
            CausalLog {
                l,
                c: VecClock::default(),
                c_all: VecClock::default(),
                op_count: 0,
                id,
                ti: TimeTest::new(),
            }
        }

        #[inline(always)]
        pub fn serialize_option(&self) -> DefaultOptions {
            self.l.serialize_option()
        }

        fn new_op(&mut self) -> Result<OpResult> {
            self.op_count += 1;
            let mut c = self.c.clone();
            c.set_entry(self.id, self.op_count);
            self.c_all.max_in_place(&c);
            let data = to_op_data(serialize(&c).unwrap());
            let op = Op::new(self.id, data, &mut self.ti);
            self.l.create_local_op(op, &self.ti)
        }

        fn recv_op(&mut self, op_c: OpCreated) -> Result<OpResult> {
            let op = self.l.received_op(op_c, &self.ti)?;
            self.update_c_all(&op.info.op.data);
            Ok(op)
        }

        fn update_c_all(&mut self, data: &OpData) {
            let v: VecClock = deserialize(data).unwrap();
            self.c_all.max_in_place(&v);
        }

        pub fn create_local_sp(&mut self) -> Result<(SpToProcess, SpDetails)> {
            let spr = self.l.create_local_sp(&mut self.ti)?;
            self.after_recv_sp(
                spr.sp_d.info.id,
                spr.completed_ops.iter().map(|op| &op.data.op.data),
            );
            Ok((spr.sp_p, spr.sp_d))
        }

        pub fn received_sp(&mut self, sp_p: SpToProcess) -> Result<(SpToProcess, SpDetails)> {
            let spr = self.l.received_sp(&mut self.ti, sp_p)?;
            self.after_recv_sp(
                spr.sp_d.info.id,
                spr.completed_ops.iter().map(|op| &op.data.op.data),
            );
            Ok((spr.sp_p, spr.sp_d))
        }

        pub fn received_sp_exact(
            &mut self,
            sp_e: SpExactToProcess,
        ) -> Result<(SpToProcess, SpDetails)> {
            for op in sp_e.exact.iter() {
                self.update_c_all(&op.op.data);
            }
            let spr = self.l.received_sp_exact(&mut self.ti, sp_e)?;
            self.after_recv_sp(
                spr.sp_d.info.id,
                spr.completed_ops.iter().map(|op| &op.data.op.data),
            );
            Ok((spr.sp_p, spr.sp_d))
        }

        fn after_recv_sp<'a, I: Iterator<Item = &'a OpData>>(&mut self, from: Id, deps: I) {
            for op in deps {
                let v: VecClock = deserialize(op).unwrap();
                println!(
                    "got vec {:?} from {}, my id {}, mine {:?}",
                    v, from, self.id, self.c
                );
                if self.c > v {
                    panic!("received clock {:?}, when already at {:?}", v, self.c);
                }
                self.c.max_in_place(&v);
            }
        }
    }

    #[test]
    fn causal() {
        let num_logs = 3;
        let commit_count = 1;
        let mut logs = vec![];
        for id in 0..num_logs {
            logs.push(CausalLog::new(id, id as usize, commit_count));
        }
        let mut ops = vec![];
        let mut sps = vec![];
        // create an op and an SP in each log
        print_logs(&mut logs);
        for i in 0..num_logs as usize {
            ops.push(logs[i].new_op().unwrap());
            assert_eq!(
                &LogError::OpAlreadyExists,
                logs[i]
                    .recv_op(ops[i].create.clone())
                    .unwrap_err()
                    .unwrap_log_error()
            );
        }
        create_sp(&mut logs, &mut sps);
        print_logs(&mut logs);
        // insert the ops in each log
        for (i, l) in logs.iter_mut().enumerate() {
            for j in 0..(num_logs - 1) as usize {
                l.ti.set_current_time_valid();
                let j = (i + j + 1) % num_logs as usize;
                ops[j] = l.recv_op(ops[j].create.clone()).unwrap();
            }
        }
        print_logs(&mut logs);
        // insert the sps in each log
        insert_sps(&mut logs, &mut sps);
        print_logs(&mut logs);
        // create a new Sp at each log so that they each support the exernal ops
        create_sp(&mut logs, &mut sps);
        print_logs(&mut logs);

        check_causal_logs(logs.iter());
    }
    fn print_logs(logs: &mut Vec<CausalLog>) {
        for (i, l) in logs.iter_mut().enumerate() {
            println!("\n\nprinting log {}", i);
            print_log_from_end(&mut l.l.l.l);
        }
        println!("\n\n");
    }

    fn create_sp(logs: &mut Vec<CausalLog>, sps: &mut Vec<SpExactToProcess>) {
        for l in logs.iter_mut() {
            println!("\nCreateSP\n");
            l.ti.set_current_time_valid();
            let (sp, _) = l.create_local_sp().unwrap();
            assert_eq!(
                &LogError::SpAlreadyExists,
                l.received_sp(sp.clone()).unwrap_err().unwrap_log_error()
            );
            sps.push(l.l.get_sp_exact(sp.sp).unwrap());
        }
    }

    fn insert_sps(logs: &mut Vec<CausalLog>, sps: &mut Vec<SpExactToProcess>) {
        let num_logs = logs.len();
        for (i, l) in logs.iter_mut().enumerate() {
            for j in 0..(num_logs - 1) as usize {
                println!("\nInsert Sp in log {} from {}\n", i, j);

                let j = (i + j + 1) % num_logs as usize;
                let sp_p = l.received_sp_exact(sps[j].clone()).unwrap().0;
                sps[j] = l.l.get_sp_exact(sp_p.sp).unwrap();
            }
        }
    }

    fn check_causal_logs<'a>(i: impl Iterator<Item = &'a CausalLog>) {
        let mut prev_vec = None;
        for l in i {
            println!("vec: {:?}", l.l.ordering.by_log_index);
            if let Some(p) = prev_vec.take() {
                assert_eq!(p, l.c);
                assert_eq!(p, l.c_all);
                println!("{:?}, {:?}", p, l.c);
            }
            prev_vec = Some(l.c.clone())
        }
    }

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

    fn run_causal_rand(num_logs: u64, commit_count: usize, num_ops: usize, seed: u64) {
        let mut logs = vec![];
        for id in 0..num_logs {
            logs.push(CausalLog::new(id, id as usize, commit_count));
        }
        let mut ops = vec![];
        let mut rng = StdRng::seed_from_u64(seed);
        // choose an op type
        let mut op_count = 0;
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
                    let l = &mut logs[idx];
                    l.new_op().unwrap();
                    l.ti.set_current_time_valid();
                    let (sp, _) = l.create_local_sp().unwrap();
                    let sp_e = l.l.get_sp_exact(sp.sp).unwrap();
                    debug!(
                        "new sp {:?}, exact {:?}",
                        sp_e.sp.to_sp(l.serialize_option()).unwrap(),
                        sp_e.exact
                    );
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
                    let l = &mut logs[idx];
                    l.ti.set_sp_time_valid(sp.sp.to_sp(l.serialize_option()).unwrap().info.time);
                    if let Err(err) = l.l.check_sp_exact(&l.ti, sp) {
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
                    let (n_sp, _) = l.received_sp_exact(sp.clone()).unwrap();
                    *sp = l.l.get_sp_exact(n_sp.sp).unwrap();
                    if others.is_empty() {
                        // all logs have received this op/sp
                        ops.swap_remove(op_idx);
                    }
                }
            }
        }

        let mut end_sps = vec![];
        for (i, l) in logs.iter_mut().enumerate() {
            // create an Sp at each node so all ops are supported by all Sps
            if let Ok((sp, _)) = l.create_local_sp() {
                let sp_e = l.l.get_sp_exact(sp.sp).unwrap();
                end_sps.push((i, sp_e));
            }
        }
        for (i, sp) in end_sps.into_iter() {
            for (j, l) in logs.iter_mut().enumerate() {
                if i == j {
                    continue;
                }
                l.ti.set_sp_time_valid(sp.sp.to_sp(l.serialize_option()).unwrap().info.time);
                l.received_sp_exact(sp.clone()).unwrap();
            }
        }
        // check the logs have the same vector clock
        check_causal_logs(logs.iter());
    }

    #[test]
    fn causal_rand() {
        let num_logs = 3;
        let commit_count = 3;
        let num_ops = 5;
        // let seed = 102;
        for seed in 105..106 {
            run_causal_rand(num_logs, commit_count, num_ops, seed);
        }
    }
}
