use super::{
    log_error::{LogError, Result},
    LogIdx,
};
use crate::verification;
use crate::verification::{Hash, Id};
use crate::{config::Time, errors::EncodeError};
use bincode::Options;
use rand::{thread_rng, Rng};
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    fmt::{Debug, Display, Formatter},
};
use std::{fmt, rc::Rc};
use verification::{hash, TimeInfo};

const OP_LEN: usize = 5;
pub type OpData = Rc<Vec<u8>>; // [u8; OP_LEN];

pub fn to_op_data(v: Vec<u8>) -> OpData {
    Rc::new(v)
}

pub fn gen_rand_data() -> OpData {
    let mut rng = thread_rng();
    Rc::new(rng.gen::<[u8; OP_LEN]>().to_vec())
}

pub fn get_empty_data() -> OpData {
    Rc::new([0; OP_LEN].to_vec())
}

/// Time field is first since we order by time, then Id, then hash,
/// also see OpState and EntryInfo which use the same ordering.
#[derive(Clone, Copy, Ord, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct BasicInfo {
    pub time: Time, // real time of event creation
    pub id: Id,     // id of creator
}

/// Time field is first since we order by time, then Id, then hash,
/// also see OpState and BasicInfo which use the same ordering.
#[derive(Clone, Copy, Ord, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct EntryInfo {
    // order first by time, then by ID, then by hash
    pub basic: BasicInfo,
    pub hash: Hash, // hash of the data of the event
}

impl From<EntryInfoData> for EntryInfo {
    fn from(ei: EntryInfoData) -> Self {
        ei.info
    }
}

impl From<&EntryInfoData> for EntryInfo {
    fn from(ei: &EntryInfoData) -> Self {
        ei.info
    }
}

impl From<OpEntryInfo> for EntryInfo {
    fn from(op: OpEntryInfo) -> Self {
        EntryInfo {
            basic: op.op.info,
            hash: op.hash,
        }
    }
}

impl From<&OpEntryInfo> for EntryInfo {
    fn from(op: &OpEntryInfo) -> Self {
        EntryInfo {
            basic: op.op.info,
            hash: op.hash,
        }
    }
}

impl Default for EntryInfo {
    fn default() -> Self {
        EntryInfo {
            basic: BasicInfo { time: 0, id: 0 },
            hash: hash(b""),
        }
    }
}
#[derive(Debug, Clone)]
pub struct OpEntryInfo {
    pub op: Op,
    pub hash: Hash,
    pub log_index: LogIdx,
}

impl Eq for OpEntryInfo {}

impl PartialEq for OpEntryInfo {
    fn eq(&self, other: &Self) -> bool {
        self.hash.eq(&other.hash)
    }
}

/// EntryInfoData is used in Sp.additional_ops to give details about ops that the Sp includes, possibily with some
/// additional information stored in the data field.
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct EntryInfoData {
    pub info: EntryInfo,
    pub data: OpData,
}

pub fn min_entry(e1: Option<&EntryInfo>, e2: Option<&EntryInfoData>) -> Option<EntryInfo> {
    match e1 {
        Some(t1) => e2.map_or(Some(*t1), |e2| Some(*t1.min(&e2.into()))),
        None => e2.map(|e2| e2.into()),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpState {
    pub op: Op,
    pub hash: Hash,
}

impl OpState {
    pub fn new<T, O>(id: Id, data: OpData, ti: &mut T, o: O) -> Result<OpState>
    where
        T: TimeInfo,
        O: Options,
    {
        let op = Op::new(id, data, ti);
        OpState::from_op(op, o)
    }

    pub fn check_hash<O: Options>(&self, o: O) -> Result<Vec<u8>> {
        verification::check_hash(&self.op, &self.hash, o).map_err(LogError::VerifyError)
    }

    pub fn from_op<O: Options>(op: Op, o: O) -> Result<OpState> {
        let enc = o
            .serialize(&op)
            .map_err(|err| LogError::EncodeError(EncodeError(err)))?;
        let hash = verification::hash(&enc);
        Ok(OpState { op, hash })
    }

    pub fn get_entry_info(&self) -> EntryInfo {
        EntryInfo {
            hash: self.hash,
            basic: self.op.info,
        }
    }

    pub fn is_in_list(&self, list: &[EntryInfo]) -> bool {
        list.binary_search(&self.get_entry_info()).is_ok()
    }
}

impl PartialOrd for OpState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Time field is first checked since we order by time, then Id, then hash,
/// also see BasicInfo and EntryInfo which use the same ordering.
impl Ord for OpState {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.op.info.cmp(&other.op.info) {
            Ordering::Less => Ordering::Less,
            Ordering::Equal => self.hash.cmp(&other.hash),
            Ordering::Greater => Ordering::Greater,
        }
    }
}

impl Eq for OpState {}

impl PartialEq for OpState {
    fn eq(&self, other: &Self) -> bool {
        self.hash.eq(&other.hash)
    }
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Op {
    pub info: BasicInfo,
    pub data: OpData,
}

impl Op {
    pub fn new<T>(id: Id, data: OpData, ti: &mut T) -> Op
    where
        T: TimeInfo,
    {
        // let hash = verification::hash(&data);
        Op {
            info: BasicInfo {
                id,
                time: ti.now_monotonic(),
            },
            data,
        }
    }
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(id: {}, time: {})", self.info.id, self.info.time)
    }
}

impl Debug for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::io::Cursor;

    use bincode::{deserialize, deserialize_from, serialize, serialize_into, Options};

    use crate::{config::INCLUDE_IN_HASH_TIMEOUT, verification::TimeTest};

    use super::{gen_rand_data, Op, OpState};

    pub fn make_op_late<O: Options>(mut op: OpState, o: O) -> OpState {
        op.op.info.time -= INCLUDE_IN_HASH_TIMEOUT + 10; // TODO this needs to be large enough to make thest test fail
        OpState::from_op(op.op, o).unwrap()
    }

    #[test]
    fn check_op() {
        let mut ti = TimeTest::new();
        let id = 1;
        let op1 = Op::new(id, gen_rand_data(), &mut ti);
        let op2 = Op::new(id, gen_rand_data(), &mut ti);
        assert!(op1.info < op2.info);
        let mut op3 = op1.clone();
        assert_eq!(op1.info, op3.info);
        assert_eq!(op1.data, op3.data);
        op3.info.id += 1;
        assert!(op1.info < op3.info);
    }

    #[test]
    fn op_serialize() {
        let mut ti = TimeTest::new();
        let id = 1;
        let op1 = Op::new(id, gen_rand_data(), &mut ti);

        let enc = serialize(&op1).unwrap();

        let dec: Op = deserialize(&enc).unwrap();

        assert_eq!(op1.info, dec.info);
        assert_eq!(op1.data, dec.data);

        let mut buf = vec![];
        let mut ops = vec![];
        let count = 5;
        for _ in 0..count {
            ops.push(Op::new(id, gen_rand_data(), &mut ti));
            serialize_into(&mut buf, ops.last().unwrap()).unwrap();
        }
        let mut reader = Cursor::new(buf);
        for nxt in ops {
            let op: Op = deserialize_from(&mut reader).unwrap();
            assert_eq!(nxt.info, op.info);
            assert_eq!(nxt.data, op.data);
        }
    }
}
