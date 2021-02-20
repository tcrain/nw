// use ed25519_dalek::Signature;
use std::{
    error::Error,
    fmt::Display,
    time::{SystemTime, UNIX_EPOCH},
};

use crate::{
    config::{Time, ARRIVED_LATE_TIMEOUT, INCLUDE_IN_HASH_TIMEOUT},
    errors::EncodeError,
};
use bincode::Options;
use serde::{Deserialize, Serialize};

#[derive(Debug, PartialEq, Eq)]
pub enum VerifyError {
    EncodeError(EncodeError),
    InvalidHash,
}

impl Error for VerifyError {}

impl Display for VerifyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

#[derive(Clone, Copy, Hash, Ord, Eq, PartialEq, PartialOrd, Debug, Serialize, Deserialize)]
pub struct Blake3Hash([u8; blake3::OUT_LEN]);

impl Blake3Hash {
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

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
    pub fn update(&mut self, input: &[u8]) {
        self.0.update(input);
    }

    pub fn finalize(&self) -> Blake3Hash {
        cast_blake3(self.0.finalize())
    }
}

pub fn check_hash<T, O>(entry: &T, entry_hash: &Hash, o: O) -> Result<Vec<u8>, VerifyError>
where
    T: ?Sized + serde::Serialize,
    O: Options,
{
    let enc = o
        .serialize(entry)
        .map_err(|err| VerifyError::EncodeError(EncodeError(err)))?;
    let new_hash = hash(&enc);
    if *entry_hash != new_hash {
        return Err(VerifyError::InvalidHash);
    }
    Ok(enc)
}

pub type Hash = Blake3Hash;
pub type Hasher = Blake3Hasher;

// pub type Verify = Signature;
pub type Id = u64;

pub struct TimeCheck {
    pub time_not_passed: bool,
    pub include_in_hash: bool,
    pub arrived_late: bool,
}

pub trait TimeInfo {
    fn now(&self) -> Time;
    fn now_monotonic(&mut self) -> Time;
    fn get_largest_sp_time(&mut self) -> Time {
        self.now_monotonic() - (INCLUDE_IN_HASH_TIMEOUT + 5)
    }
    fn arrived_time_check(&self, t: Time) -> TimeCheck {
        let time_late = {
            let n = self.now();
            if n > t {
                n - t
            } else {
                // the time of the event has not yet passed
                return TimeCheck {
                    time_not_passed: true,
                    include_in_hash: true,
                    arrived_late: false,
                };
            }
        };
        TimeCheck {
            time_not_passed: false,
            include_in_hash: time_late < INCLUDE_IN_HASH_TIMEOUT,
            arrived_late: time_late > ARRIVED_LATE_TIMEOUT,
        }
    }
}

struct TimeState {
    last_time: Time,
}

impl TimeInfo for TimeState {
    fn now(&self) -> Time {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros()
    }

    fn now_monotonic(&mut self) -> Time {
        let t = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros();
        if t <= self.last_time {
            self.last_time += 1;
            self.last_time
        } else {
            self.last_time = t;
            t
        }
    }
}

pub struct TimeTest {
    last_time: Time,
}

impl TimeInfo for TimeTest {
    fn now(&self) -> Time {
        self.last_time
    }

    fn now_monotonic(&mut self) -> Time {
        self.last_time += 1;
        self.last_time
    }
}

impl TimeTest {
    pub fn new() -> TimeTest {
        TimeTest {
            last_time: 10_000_000_000,
        }
    }

    pub fn set_current_time_valid(&mut self) {
        self.last_time += INCLUDE_IN_HASH_TIMEOUT + 10;
    }

    pub fn sleep_op_until_late(&mut self, t: Time) {
        let sleep_time = (t + INCLUDE_IN_HASH_TIMEOUT + 10).saturating_sub(self.now());
        self.last_time += sleep_time;
    }

    pub fn set_sp_time_valid(&mut self, t: Time) {
        let sleep_time = (t + INCLUDE_IN_HASH_TIMEOUT + 10).saturating_sub(self.now());
        self.last_time += sleep_time;
    }
}

#[cfg(test)]
mod test {
    use bincode::{deserialize, serialize};

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

    #[test]
    fn hash_serialize() {
        let v = b"some string";
        let h = hash(v);

        let enc = serialize(&h).unwrap();

        let dec = deserialize(&enc).unwrap();

        assert_eq!(h, dec);
    }
}
