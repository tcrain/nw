use std::{env::args, fs::File, time::Instant};

use log::debug;
use nw::{
    file_sr::{CursorSR, FileSR},
    log::local_log::test_setup::{add_ops_rand_order, add_sps, new_log_test},
    rw_buf::{RWBuf, RWS},
};
use rand::{prelude::StdRng, SeedableRng};

fn main() {
    // this test inserts one op per participant in a random order at each log
    let msg = "expected an interger arg, 0 = RWBuf, 1 = FileSR, 2 = CursorSR";
    let idx = args()
        .into_iter()
        .nth(1)
        .expect(msg)
        .parse::<usize>()
        .expect(msg);
    match idx {
        0 => run_node(RWBuf::new),
        1 => run_node(FileSR::new),
        2 => run_node(|_| CursorSR::new()),
        _ => panic!("{}", msg),
    }
}

fn run_node<F: RWS, G: Fn(File) -> F + Copy>(open_fn: G) {
    let mut rng = StdRng::seed_from_u64(100);
    let num_logs = 5;
    let mut logs = vec![];
    for i in 0..num_logs {
        logs.push(new_log_test(200 + i, open_fn))
    }
    let now = Instant::now();
    for i in 1..1000 {
        // 1 op per log
        let ops_time = Instant::now();
        add_ops_rand_order(&mut logs, &mut rng, i);
        debug!("done ops iter {}: {}ms", i, ops_time.elapsed().as_millis());
        // 1 sp per log
        let sp_time = Instant::now();
        add_sps(&mut logs, &mut rng);
        debug!("done iter {}: {}ms", i, sp_time.elapsed().as_millis());
    }
    println!("duration: {}ms", now.elapsed().as_millis());
}
