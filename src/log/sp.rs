use crate::verification::{Hash, Verify};
use std::borrow::Borrow;
use std::cell::RefCell;
use std::io::Error;
use std::rc::{Rc, Weak};
use std::slice::Iter;
use std::time::SystemTime;

struct SP {
    id: u64,
    time: SystemTime,
    prev_sp: Hash,
    unsupported: Vec<Hash>,
    verify: Verify,
}
