#![allow(dead_code)]
mod config;
mod errors;
mod log;
mod verification;
mod utils;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

}
