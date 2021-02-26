use crate::{errors::EncodeError, errors::LogIOError, verification::VerifyError};
use std::{error::Error, fmt::Display, result};

pub type Result<T> = result::Result<T, LogError>;

#[derive(Debug, PartialEq, Eq)]
pub enum LogError {
    EncodeError(EncodeError),
    VerifyError(VerifyError),
    IOError(LogIOError),
    EOFError,
    // FileError,
    // FileReadError,
    // FileWriteError,
    // FileSeekError,
    FileSeekPastEndError,
    FileSeekBeforeStartError,
    SerializeError,
    DeserializeError,
    NoNewOps,
    OpAlreadyDropped,
    OpAlreadyExists,
    // InvalidHash,
    EmptyLogError,
    SingleItemLog,
    PrevSpHasNoLastOp,
    IdHasNoSp,
    PrevSpNotFound,
    NotEnoughOpsForSP,
    SpHashNotComputed,
    SpNoOpsSupported,
    SpPrevLaterTime,
    SpPrevIdDifferent,
    SpInvalidInitHash,
    SpArrivedEarly,
}

impl LogError {
    pub fn is_io_error(&self) -> bool {
        matches!(*self, LogError::IOError(_))
    }
}

impl Error for LogError {}

impl Display for LogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}