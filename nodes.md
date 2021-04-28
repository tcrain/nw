Profiling:

install flamegraph:
cargo install flamegraph

see:
https://github.com/flamegraph-rs/flamegraph

in cargo.toml set
[profile.release]
debug = true

run for example:
cargo flamegraph --bin {executable name, eg node} -- {program arguments}

Debug info:

Only run code while debugging
Put the following before each line that should only execute while debugging
#[cfg(debug_assertions)]

or use the block:
if cfg!(debug_assertions) {}
