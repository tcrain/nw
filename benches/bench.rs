use std::fs::File;

use criterion::{criterion_group, criterion_main, BatchSize, Criterion};
use nw::{
    causal::causal_log::test_structs::{new_causal, CausalTestLog},
    file_sr::{CursorSR, FileSR},
    log::ordered_log::{
        DepBTree, DepHSet, DepVec, Dependents, SupBTree, SupHSet, SupVec, Supporters,
    },
    log::{
        local_log::test_setup::LogTest,
        ordered_log::{
            test_structs::{new_counter, run_ordered_rand, run_ordered_standard, CollectTestLog},
            OrderedLog, OrderedLogRun, OrderedState,
        },
        LogIdx,
    },
    rw_buf::RWBuf,
    verification::{Id, TimeTest},
};
use nw::{
    log::local_log::test_setup::{add_ops_rand_order, add_sps, new_log_test},
    rw_buf::RWS,
};
use rand::{
    prelude::{SliceRandom, StdRng},
    thread_rng, SeedableRng,
};

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

fn counter_bench_file() -> Vec<(CollectTestLog<FileSR>, TimeTest)> {
    counter_bench_setup(FileSR::new)
}

fn counter_bench_mem() -> Vec<(CollectTestLog<CursorSR>, TimeTest)> {
    counter_bench_setup(|_| CursorSR::new())
}

fn counter_bench_buf() -> Vec<(CollectTestLog<RWBuf<File>>, TimeTest)> {
    counter_bench_setup(RWBuf::new)
}

fn counter_bench_setup<F: RWS, G: Fn(File) -> F + Copy>(
    open_fn: G,
) -> Vec<(CollectTestLog<F>, TimeTest)> {
    let num_logs = 4;
    let commit_count = 3;
    let mut logs = vec![];
    for i in 0..num_logs {
        logs.push(new_counter(i, 100, commit_count, open_fn))
    }
    logs
}

fn causal_bench_file() -> Vec<(CausalTestLog<FileSR>, TimeTest)> {
    causal_bench_setup(FileSR::new)
}

fn causal_bench_mem() -> Vec<(CausalTestLog<CursorSR>, TimeTest)> {
    causal_bench_setup(|_| CursorSR::new())
}

fn causal_bench_buf() -> Vec<(CausalTestLog<RWBuf<File>>, TimeTest)> {
    causal_bench_setup(RWBuf::new)
}

fn causal_bench_setup<F: RWS, G: Fn(File) -> F + Copy>(
    open_fn: G,
) -> Vec<(CausalTestLog<F>, TimeTest)> {
    let num_logs = 4;
    let commit_count = 3;
    let mut logs = vec![];
    for i in 0..num_logs {
        logs.push(new_causal(i, 100, commit_count, open_fn))
    }
    logs
}

fn run_ordered_standard_bench<L: OrderedLog, S: OrderedState>(
    mut logs: Vec<(OrderedLogRun<L, S>, TimeTest)>,
) -> Vec<(OrderedLogRun<L, S>, TimeTest)> {
    let num_ops = 10;
    let iterations = 2;
    let seed = 100;
    run_ordered_standard(&mut logs, num_ops, iterations, seed);
    logs
}

fn run_ordered_rand_bench<L: OrderedLog, S: OrderedState>(
    mut logs: Vec<(OrderedLogRun<L, S>, TimeTest)>,
) -> Vec<(OrderedLogRun<L, S>, TimeTest)> {
    let num_ops = 20;
    let seed = 100;
    run_ordered_rand(&mut logs, num_ops, seed);
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

fn log_transfer_benchmark(c: &mut Criterion) {
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

fn ordered_counter_benchmark(c: &mut Criterion) {
    c.bench_function("ordered_counter_file", move |b| {
        b.iter_batched(
            counter_bench_file,
            run_ordered_standard_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("ordered_counter_buffered", move |b| {
        b.iter_batched(
            counter_bench_buf,
            run_ordered_standard_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("ordered_counter_mem", move |b| {
        b.iter_batched(
            counter_bench_mem,
            run_ordered_standard_bench,
            BatchSize::SmallInput,
        )
    });
}

fn rand_counter_benchmark(c: &mut Criterion) {
    c.bench_function("rand_counter_file", move |b| {
        b.iter_batched(
            counter_bench_file,
            run_ordered_rand_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("rand_counter_buffered", move |b| {
        b.iter_batched(
            counter_bench_buf,
            run_ordered_rand_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("rand_counter_mem", move |b| {
        b.iter_batched(
            counter_bench_mem,
            run_ordered_rand_bench,
            BatchSize::SmallInput,
        )
    });
}

fn ordered_causal_benchmark(c: &mut Criterion) {
    c.bench_function("ordered_causal_file", move |b| {
        b.iter_batched(
            causal_bench_file,
            run_ordered_standard_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("ordered_causal_buffered", move |b| {
        b.iter_batched(
            causal_bench_buf,
            run_ordered_standard_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("ordered_causal_mem", move |b| {
        b.iter_batched(
            causal_bench_mem,
            run_ordered_standard_bench,
            BatchSize::SmallInput,
        )
    });
}

fn rand_causal_benchmark(c: &mut Criterion) {
    c.bench_function("rand_causal_file", move |b| {
        b.iter_batched(
            causal_bench_file,
            run_ordered_rand_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("rand_causal_buffered", move |b| {
        b.iter_batched(
            causal_bench_buf,
            run_ordered_rand_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("rand_causal_mem", move |b| {
        b.iter_batched(
            causal_bench_mem,
            run_ordered_rand_bench,
            BatchSize::SmallInput,
        )
    });
}

struct SupBench<S: Supporters> {
    s: S,
    v: Vec<Id>,
}

fn supporters_bench_gen<S: Supporters>() -> SupBench<S> {
    let mut rng = StdRng::from_rng(thread_rng()).unwrap();
    let mut v: Vec<LogIdx> = (0..DEP_MAX).collect();

    v.shuffle(&mut rng);
    SupBench {
        s: S::default(),
        v: v.iter().cloned().take(DEP_COUNT as usize).collect(),
    }
}

fn supporters_bench<S: Supporters>(mut sb: SupBench<S>) {
    // fn add_id(&mut self, id: Id) -> bool;
    // fn get_count(&self) -> usize;
    for id in sb.v {
        assert!(sb.s.add_id(id));
    }
}

fn supporters_benchmark(c: &mut Criterion) {
    c.bench_function("causal_supporters_btree", move |b| {
        b.iter_batched(
            supporters_bench_gen::<SupBTree>,
            supporters_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("causal_supporters_vec", move |b| {
        b.iter_batched(
            supporters_bench_gen::<SupVec>,
            supporters_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("causal_supporters_hset", move |b| {
        b.iter_batched(
            supporters_bench_gen::<SupHSet>,
            supporters_bench,
            BatchSize::SmallInput,
        )
    });
}

struct DepBench<D: Dependents> {
    // rng: StdRng,
    d: D,
    v: Vec<LogIdx>,
}

const DEP_MAX: LogIdx = 100;
const DEP_COUNT: LogIdx = 50;

fn dependents_bench_gen<D: Dependents>() -> DepBench<D> {
    let mut rng = StdRng::from_rng(thread_rng()).unwrap();
    let mut v: Vec<LogIdx> = (0..DEP_MAX).collect();

    v.shuffle(&mut rng);
    DepBench {
        // rng,
        d: D::default(),
        v: v.iter().cloned().take(DEP_COUNT as usize).collect(),
    }
}

fn dependents_bench<D: Dependents>(mut db: DepBench<D>) {
    // fn add_ids<I: Iterator<Item = LogIdx>>(&mut self, i: I);
    // fn add_id(&mut self, idx: LogIdx);
    // fn got_support(&mut self, idx: LogIdx) -> bool;
    // fn remaining_idxs(&self) -> usize;
    let count = db.v.len();
    db.d.add_idxs(db.v.iter().cloned());
    assert_eq!(count, db.d.remaining_idxs());
    for (i, idx) in db.v.into_iter().enumerate() {
        assert!(db.d.got_support(idx));
        assert_eq!(count - i - 1, db.d.remaining_idxs());
    }
}

fn dependents_benchmark(c: &mut Criterion) {
    c.bench_function("causal_dependents_btree", move |b| {
        b.iter_batched(
            dependents_bench_gen::<DepBTree>,
            dependents_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("causal_dependents_vec", move |b| {
        b.iter_batched(
            dependents_bench_gen::<DepVec>,
            dependents_bench,
            BatchSize::SmallInput,
        )
    });
    c.bench_function("causal_dependents_hset", move |b| {
        b.iter_batched(
            dependents_bench_gen::<DepHSet>,
            dependents_bench,
            BatchSize::SmallInput,
        )
    });
}

criterion_group!(
    benches,
    log_transfer_benchmark,
    dependents_benchmark,
    supporters_benchmark,
    ordered_counter_benchmark,
    rand_counter_benchmark,
    ordered_causal_benchmark,
    rand_causal_benchmark
);
criterion_main!(benches);
