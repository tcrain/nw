
#[derive(Debug)]
pub enum Error {
    LogError(LogError)
}

#[derive(Debug)]
pub enum LogError{
    EmptyLogError
}