use std::time::SystemTime;

struct TX {
    id: uint64,
    time: SystemTime,
    data: [u8],
}
