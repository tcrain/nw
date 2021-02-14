use std::fs::File;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use nw::{
    file_sr::{CursorSR, FileSR},
    log::local_log::test_setup::LogTest,
    rw_buf::RWBuf,
};
use nw::{
    log::local_log::test_setup::{add_ops_rand_order, add_sps, new_log_test},
    rw_buf::RWS,
};
use rand::{prelude::StdRng, SeedableRng};

fn transfer_bench_file() -> Vec<LogTest<FileSR>> {
    transfer_bench_setup(FileSR::new)
}

fn transfer_bench_mem() -> Vec<LogTest<CursorSR>> {
    transfer_bench_setup(|_| CursorSR::new())
}

fn transfer_bench_buf() -> Vec<LogTest<RWBuf<File>>> {
    transfer_bench_setup(RWBuf::new)
}

fn transfer_bench_setup<F: RWS, G: Fn(File) -> F + Copy>(open_fn: G) -> Vec<LogTest<F>> {
    // this test inserts one op per participant in a random order at each log
    let num_logs = 5;
    let mut logs = vec![];
    for i in 0..num_logs {
        logs.push(new_log_test(200 + i, open_fn))
    }
    logs
}

fn run_bench<F: RWS>(mut logs: Vec<LogTest<F>>) -> Vec<LogTest<F>> {
    // input the logs so their initialization is not timed
    let mut rng = StdRng::seed_from_u64(100);
    for i in 1..3 {
        // 1 op per log
        add_ops_rand_order(&mut logs, &mut rng, i);
        // 1 sp per log
        add_sps(&mut logs, &mut rng);
    }
    logs // return the logs so the drop is not timed
}

pub fn criterion_benchmark(c: &mut Criterion) {
    c.bench_function("transfer_log_file", move |b| {
        b.iter_batched(transfer_bench_file, run_bench, BatchSize::SmallInput)
    });
    c.bench_function("transfer_log_buffered", move |b| {
        b.iter_batched(transfer_bench_buf, run_bench, BatchSize::SmallInput)
    });
    c.bench_function("transfer_log_mem", move |b| {
        b.iter_batched(transfer_bench_mem, run_bench, BatchSize::SmallInput)
    });
}

criterion_group!(benches, criterion_benchmark);
criterion_main!(benches);
