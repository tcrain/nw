// use ed25519_dalek::Signature;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Copy, Hash, Ord, Eq, PartialEq, PartialOrd, Debug)]
pub struct Blake3Hash([u8; blake3::OUT_LEN]);

pub fn hash(b: &[u8]) -> Hash {
    cast_blake3(blake3::hash(b))
}

pub fn cast_blake3(hash: blake3::Hash) -> Blake3Hash {
    Blake3Hash(hash.into())
}

pub fn new_hasher() -> Hasher {
    Blake3Hasher(blake3::Hasher::new())
}
pub struct Blake3Hasher(blake3::Hasher);

impl Blake3Hasher {
    fn update(&mut self,input: &[u8]) {
        self.0.update(input);
    }

    fn finalize(&self) -> Blake3Hash {
        cast_blake3(self.0.finalize())
    }
}

pub type Hash = Blake3Hash;
pub type Hasher = Blake3Hasher;

// pub type Verify = Signature;
pub type Id = u64;

// pub fn check_verify(_v: Verify, _h: Hash) -> bool {
//    true
// }

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

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn hash_test() {
        let v = b"some string";
        let h = hash(v);
        
        let v2 = b"some string 2";
        let h2 = hash(v2);

        assert_ne!(h, h2);
    }

    #[test]
    fn hasher_test() {
        let v1 = b"some string1";
        let v2 = b"some string2";
        let v3 = v1.iter().chain(v2).cloned().collect::<Vec<_>>();

        let mut h = new_hasher();
        h.update(v1);
        h.update(v2);
        assert_eq!(hash(&v3), h.finalize())
    }

}