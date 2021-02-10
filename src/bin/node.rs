use nw::log::local_log::test_setup::{add_ops_rand_order, add_sps, LogTest};
use rand::{prelude::StdRng, SeedableRng};

fn main() {
    // this test inserts one op per participant in a random order at each log
    let mut rng = StdRng::seed_from_u64(100);
    let num_logs = 5;
    let mut logs = vec![];
    for i in 0..num_logs {
        logs.push(LogTest::new(200 + i))
    }
    for i in 1..100 {
        // 1 op per log
        add_ops_rand_order(&mut logs, &mut rng, i);
        // 1 sp per log
        add_sps(&mut logs, &mut rng);
    }
}
