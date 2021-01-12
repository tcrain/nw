
pub fn result_to_val<V>(res: Result<V, V>) -> V {
    match res {
        Ok(v) => v,
        Err(v) => v,
    }
}
