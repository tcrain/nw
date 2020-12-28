use std::{cmp::Ordering, fmt::{self, Debug, Display, Formatter}};

use crate::verification::{Hash, Id, Time, now, hash};

use super::op::gen_rand_data;
use super::op::OpData;

type SupportCount = u64;

#[derive(Clone)]
pub struct Sp {
    pub time: Time, // support operations up to this time
    pub id: Id, // signer
    pub hash: Hash, // hash of this msg
    pub new_ops_supported: SupportCount, // number of new operations supported by this sp
    data: OpData,
    pub prev_sp: Hash, // hash of the sp that this one comes after
    pub unsupported_ops: Vec<UnsupportedOp>, // operations before this entry in the log not supported by this item
    // verify: Verify,
}

impl Display for Sp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(id: {}, time: {})", self.id, self.time)
    }
}

impl Debug for Sp {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl PartialOrd for Sp {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Sp {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.time.cmp(&other.time) {
            Ordering::Greater => Ordering::Greater,
            Ordering::Less => Ordering::Less,
            Ordering::Equal => {
                match self.id.cmp(&other.id) {
                    Ordering::Greater => Ordering::Greater,
                    Ordering::Less => Ordering::Less,
                    Ordering::Equal => {
                        match self.hash.cmp(&other.hash) {
                            Ordering::Greater => Ordering::Greater,
                            Ordering::Less => Ordering::Less,
                            Ordering::Equal => Ordering::Equal,
                        }
                    }
                }
            }
        }
    }
}

impl Eq for Sp {}

impl PartialEq for Sp {
    fn eq(&self, other: &Self) -> bool {
        self.time == other.time && self.id == other.id && self.hash == other.hash
    }
}

impl Sp {
    pub fn new(id: Id, prev_sp_hash: Option<Hash>) -> Sp {
        let data = gen_rand_data();
        let hsh = match prev_sp_hash {
            None => hash(b""),
            Some(hsh) => hsh,
        };
        Sp{
            time: now(),
            id,
            hash: hash(&data),
            new_ops_supported: 0,
            data,
            prev_sp: hsh,
            unsupported_ops: vec![],
        }
    }
}

#[derive(Clone, Copy, Ord, Eq, PartialEq, PartialOrd)]
pub struct UnsupportedOp {
    id: Id,
    time: Time,
    hash: Hash,
}
