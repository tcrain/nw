use rand::Rng;

pub fn result_to_val<V>(res: Result<V, V>) -> V {
    match res {
        Ok(v) => v,
        Err(v) => v,
    }
}

pub fn gen_rand<T: Rng>(len: usize, rng: &mut T) -> Vec<u8> {
    let mut v = vec![0; len];
    rng.fill(v.as_mut_slice());
    v
}
