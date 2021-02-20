use std::error::Error;
use std::{fmt::Display, io};

#[derive(Debug)]
pub struct EncodeError(pub bincode::Error);

impl PartialEq for EncodeError {
    fn eq(&self, _other: &Self) -> bool {
        true // TODO
    }
}

impl Eq for EncodeError {}

impl Error for EncodeError {}

impl Display for EncodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

#[derive(Debug)]
pub struct LogIOError(pub io::Error);

impl PartialEq for LogIOError {
    fn eq(&self, other: &Self) -> bool {
        self.0.kind() == other.0.kind()
    }
}

impl Eq for LogIOError {}

impl Error for LogIOError {}

impl Display for LogIOError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}
