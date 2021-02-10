use criterion::{criterion_group, criterion_main, Criterion};
use nw::log::local_log::test_setup::{LogTest, add_ops_rand_order, add_sps};
use rand::{SeedableRng, prelude::StdRng};

fn transfer_bench() {
    // this test inserts one op per participant in a random order at each log
    let mut rng = StdRng::seed_from_u64(100);
    let num_logs = 5;
    let mut logs = vec![];
    for i in 0..num_logs {
        logs.push(LogTest::new(200 + i))
    }
    for i in 1..3 {
        // 1 op per log
        add_ops_rand_order(&mut logs, &mut rng, i);
        // 1 sp per log
        add_sps(&mut logs, &mut rng);
    }
}


pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("log transfer", |b| b.iter(transfer_bench));
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);