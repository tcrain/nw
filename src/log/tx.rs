use std::time::SystemTime;

pub struct TX {
    id: u64,
    time: SystemTime,
    data: [u8; 5],
}
