use crate::ops::WriteOp;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::{SendTimeoutError, TrySendError};
use std::time::Duration;

/// Cheap-clone handle for sending WriteOps to the writer task.
///
/// Use `try_send` for non-stateful ops (drop on full channel with warn).
/// Use `send_with_timeout` for stateful ops (backpressure, but bounded).
#[derive(Clone)]
pub struct StorageHandle {
    tx: mpsc::Sender<WriteOp>,
}

impl StorageHandle {
    pub fn new(tx: mpsc::Sender<WriteOp>) -> Self {
        Self { tx }
    }

    /// Non-blocking send. Returns Err if the channel is full.
    pub fn try_send(&self, op: WriteOp) -> Result<(), TrySendError<WriteOp>> {
        self.tx.try_send(op)
    }

    /// Send with a short timeout for stateful ops. Blocks briefly if channel is full.
    pub async fn send_with_timeout(&self, op: WriteOp) -> Result<(), SendTimeoutError<WriteOp>> {
        self.tx.send_timeout(op, Duration::from_secs(5)).await
    }

    /// Unbounded send.
    pub async fn send(&self, op: WriteOp) -> Result<(), mpsc::error::SendError<WriteOp>> {
        self.tx.send(op).await
    }
}
