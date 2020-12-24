use std::fmt::{Debug, Display, Formatter};
use std::{fmt};
use rand::{thread_rng, Rng};
use crate::verification::{Id, Time, Hash};
use crate::verification;

const OP_LEN: usize = 5;
pub type OpData = [u8; OP_LEN];

fn gen_rand_data() -> OpData {
    let mut rng = thread_rng();
    rng.gen::<OpData>()
}
#[derive(Copy, Clone, Ord, Eq, PartialEq, PartialOrd)]
pub struct Op {
    time: Time, // order first by time, then by ID, then by hash
    id: Id,
    hash: Hash,
    data: OpData,
}

impl Op {
    pub fn new(id: Id) -> Op {
        let data = gen_rand_data();
        let hash = verification::hash(&data);
        Op{
            id: id,
            time: verification::now(),
            hash: hash,
            data: data,
        }
    }
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "(id: {}, time: {})", self.id, self.time)
    }
}

impl Debug for Op {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}