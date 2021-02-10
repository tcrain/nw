pub type Time = u128;

pub const ARRIVED_LATE_TIMEOUT: Time = 100;
pub const INCLUDE_IN_HASH_TIMEOUT: Time = ARRIVED_LATE_TIMEOUT + 100;

pub const DEFAULT_BUF_SIZE: usize = 8024;
