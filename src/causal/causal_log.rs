use rustc_hash::FxHashMap;

use crate::{
    log::local_log::LocalLog,
    log::op::OpEntryInfo,
    log::op::{EntryInfo, EntryInfoData, OpData},
    rw_buf::RWS,
    verification::Id,
};

pub type HMap<K, V> = FxHashMap<K, V>;

struct PendingSP {
    supporters: Vec<Id>,
    process_index: u64,
    data: OpData,
}

struct PendingOp {
    supporters: Vec<Id>,
    data: OpEntryInfo,
}

struct PendingOps<'a> {
    by_log_index: Vec<PendingOp>,
    by_recv: Vec<&'a PendingOp>,
    commit_count: usize,
    last_committed: Option<u64>,
}

impl<'a> PendingOps<'a> {
    #[inline(always)]
    fn min_idx(&self) -> Option<u64> {
        // self.by_log_index.first().map(|f| f.data.log_index)
        self.last_committed
    }

    #[inline(always)]
    fn max_idx(&self) -> Option<u64> {
        self.by_log_index.last().map(|f| f.data.log_index)
    }

    /* #[inline(always)]
    fn check_idx(&self, log_idx: usize) -> Result<(), Error> {
        if let Some(min) = self.min_idx() {
            if log_idx <= min {

            }
        }
    }*/

    // fn got_support(from: Id, op: OpEntryInfo) {}
}

// (instead of support for SPs, has to be support for ops)

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
