use rand::{prelude::SliceRandom, Rng};

pub fn result_to_val<V>(res: Result<V, V>) -> V {
    match res {
        Ok(v) => v,
        Err(v) => v,
    }
}

pub fn gen_shuffled<T: Rng>(len: usize, subtract: Option<usize>, rng: &mut T) -> Vec<usize> {
    let mut v: Vec<usize> = (0..len).collect();
    if let Some(s) = subtract {
        v.swap_remove(s);
    }
    v.shuffle(rng);
    v
}

pub fn gen_rand<T: Rng>(len: usize, rng: &mut T) -> Vec<u8> {
    let mut v = vec![0; len];
    rng.fill(v.as_mut_slice());
    v
}
