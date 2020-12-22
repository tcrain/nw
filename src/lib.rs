mod errors;
mod log;
mod verification;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }

    #[test]
    fn test2() {
        print!("test {}", 2)
    }
}
