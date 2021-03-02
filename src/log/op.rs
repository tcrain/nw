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

#[derive(Clone, Copy, Ord, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct BasicInfo {
    pub time: Time, // real time of event creation
    pub id: Id,     // id of creator
}

#[derive(Clone, Copy, Ord, Eq, PartialEq, PartialOrd, Serialize, Deserialize, Debug)]
pub struct EntryInfo {
    // order first by time, then by ID, then by hash
    pub basic: BasicInfo,
    pub hash: Hash, // hash of the data of the event
}

impl Default for EntryInfo {
    fn default() -> Self {
        EntryInfo {
            basic: BasicInfo { time: 0, id: 0 },
            hash: hash(b""),
        }
    }
}
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct OpEntryInfo {
    pub op: EntryInfoData,
    pub log_index: LogIdx,
}
#[derive(Clone, Serialize, Deserialize, Debug, PartialEq, Eq)]
pub struct EntryInfoData {
    pub info: EntryInfo,
    pub data: OpData,
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

    pub fn get_entry_info_data(&self) -> EntryInfoData {
        EntryInfoData {
            info: self.get_entry_info(),
            data: self.op.data.clone(), // TODO should make ref cell?
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

#[derive(Clone, Ord, Eq, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct Op {
    pub info: BasicInfo,
    data: OpData,
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
        assert!(op1 < op2);
        let mut op3 = op1.clone();
        assert!(op1 == op3);
        op3.info.id += 1;
        assert!(op1 < op3);
    }

    #[test]
    fn op_serialize() {
        let mut ti = TimeTest::new();
        let id = 1;
        let op1 = Op::new(id, gen_rand_data(), &mut ti);

        let enc = serialize(&op1).unwrap();
        // println!("op enc len: {}", enc.len());

        let dec = deserialize(&enc).unwrap();

        assert_eq!(op1, dec);

        let mut buf = vec![];
        let mut ops = vec![];
        let count = 5;
        for _ in 0..count {
            ops.push(Op::new(id, gen_rand_data(), &mut ti));
            serialize_into(&mut buf, ops.last().unwrap()).unwrap();
        }
        let mut reader = Cursor::new(buf);
        for nxt in ops {
            let op = deserialize_from(&mut reader).unwrap();
            assert_eq!(nxt, op);
        }
    }
}
