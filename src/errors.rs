
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    LogError(LogError)
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogError{
    EmptyLogError
}