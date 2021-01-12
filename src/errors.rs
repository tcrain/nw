
#[derive(Debug, PartialEq, Eq)]
pub enum Error {
    LogError(LogError)
}

#[derive(Debug, PartialEq, Eq)]
pub enum LogError{
    SerializeError,
    NoNewOps,
    OpAlreadyExists,
    EmptyLogError,
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