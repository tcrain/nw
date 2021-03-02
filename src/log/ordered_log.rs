use std::{error::Error, fmt::Display, result};

use crate::{rw_buf::RWS, verification::TimeInfo};

use super::{
    local_log::{LocalLog, OpCreated, OpResult, SpExactToProcess, SpToProcess},
    log_error::LogError,
    op::{Op, OpEntryInfo},
    sp::SpInfo,
};

#[derive(Debug)]
pub enum OrderingError {
    Custom(Box<dyn Error>),
    LogError(LogError),
}

impl OrderingError {
    pub fn unwrap_log_error(&self) -> &LogError {
        match self {
            OrderingError::LogError(l) => l,
            OrderingError::Custom(_) => panic!("expected log error"),
        }
    }
}

impl Error for OrderingError {}

impl Display for OrderingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Debug::fmt(&self, f)
    }
}

pub type Result<T> = result::Result<T, OrderingError>;

pub trait LogOrdering {
    fn recv_sp<I: Iterator<Item = OpEntryInfo>>(&mut self, info: SpInfo, deps: I);
    fn recv_op(&mut self, op: OpEntryInfo) -> Result<()>;
}

/// Contains a local log, plus the LogOrdering trait to keep track.
pub struct OrderedLog<F: RWS, O: LogOrdering> {
    l: LocalLog<F>,
    ordering: O,
}

impl<F: RWS, O: LogOrdering> OrderedLog<F, O> {
    pub fn new(l: LocalLog<F>, ordering: O) -> Self {
        OrderedLog { l, ordering }
    }

    pub fn create_local_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
    ) -> Result<(SpToProcess, Vec<OpEntryInfo>)> {
        let (spp, deps) = self
            .l
            .create_local_sp(ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_sp(spp.info, deps.iter().cloned());
        Ok((spp, deps))
    }

    pub fn create_local_op<T: TimeInfo>(&mut self, op: Op, ti: &T) -> Result<OpResult> {
        let res = self
            .l
            .create_local_op(op, ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_op(res.info.clone())?;
        Ok(res)
    }

    /// Inpts an operation from an external node in the log.
    pub fn received_op<T: TimeInfo>(&mut self, op_c: OpCreated, ti: &T) -> Result<OpResult> {
        let res = self
            .l
            .received_op(op_c, ti)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_op(res.info.clone())?;
        Ok(res)
    }

    pub fn received_sp<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpToProcess,
    ) -> Result<(SpToProcess, Vec<OpEntryInfo>)> {
        let (spp, deps) = self
            .l
            .received_sp(ti, sp_p)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_sp(spp.info, deps.iter().cloned());
        Ok((spp, deps))
    }

    pub fn received_sp_exact<T: TimeInfo>(
        &mut self,
        ti: &mut T,
        sp_p: SpExactToProcess,
    ) -> Result<(SpExactToProcess, Vec<OpEntryInfo>)> {
        let (spp, deps) = self
            .l
            .received_sp_exact(ti, sp_p)
            .map_err(OrderingError::LogError)?;
        self.ordering.recv_sp(spp.info, deps.iter().cloned());
        Ok((spp, deps))
    }
}
