use crate::verification::{Hash, Verify};
use std::time::SystemTime;

pub struct SP {
    id: u64,
    time: SystemTime,
    prev_sp: Hash,
    unsupported: Vec<Hash>,
    verify: Verify,
}
