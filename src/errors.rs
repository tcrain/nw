#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    LogError(LogError),
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogError {
    EOFError,
    FileError,
    FileReadError,
    FileWriteError,
    FileSeekError,
    FileSeekPastEndError,
    FileSeekBeforeStartError,
    SerializeError,
    DeserializeError,
    NoNewOps,
    OpAlreadyDropped,
    OpAlreadyExists,
    InvalidHash,
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
