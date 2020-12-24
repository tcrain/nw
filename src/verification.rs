use ed25519_dalek::Signature;
use blake3;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Hash, Ord, Eq, PartialEq, PartialOrd, Debug)]
pub struct Blake3Hash([u8; blake3::OUT_LEN]);

pub fn hash(b: &[u8]) -> Hash {
    cast_blake3(blake3::hash(b))
}

pub fn cast_blake3(hash: blake3::Hash) -> Blake3Hash {
    Blake3Hash(hash.into())
}

pub type Hash = Blake3Hash;
pub type Verify = Signature;
pub type Id = u64;

pub fn check_verify(_v: Verify, _h: Hash) -> bool {
    true
}

pub type Time = u128;

static mut LAST_TIME: Time = 0;

pub fn now() -> Time {
    let t = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
    unsafe { // TODO make thread safe
        if t <= LAST_TIME {
            LAST_TIME += 1;
            LAST_TIME
        } else {
            LAST_TIME = t;
            t
        }
    }
}