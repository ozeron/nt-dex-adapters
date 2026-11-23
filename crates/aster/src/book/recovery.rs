//! REST snapshot attempts. The session owns pacing, reconnect fences, and book updates.

use std::{sync::Arc, time::Duration};

use nautilus_common::live::get_runtime;
use nautilus_live::book::recovery::BookRecovery;
use tokio::sync::{Mutex, mpsc::UnboundedSender, oneshot};
use tokio_util::sync::CancellationToken;
use ustr::Ustr;

use crate::{http::client::AsterHttpClient, websocket::pool::ReconnectFence};

#[derive(Clone, Debug, thiserror::Error)]
pub(crate) enum SnapshotError {
    #[error("{0}")]
    Retryable(String),
    #[error("{0}")]
    Permanent(String),
    #[error("depth snapshot deadline expired")]
    DeadlineExpired,
    #[error("Aster session stopped")]
    SessionClosed,
    #[error("book stream is reconnecting")]
    Reconnecting,
    #[error("obsolete depth snapshot")]
    Obsolete,
    #[error("depth snapshot did not bridge the stream")]
    Unbridged,
}

impl SnapshotError {
    fn retryable(&self) -> bool {
        !matches!(self, Self::Permanent(_))
    }
}

pub(crate) type SnapshotRecovery = BookRecovery<SnapshotError>;

pub(crate) struct SnapshotAdmission {
    pub symbol: Ustr,
    pub owner: Arc<SnapshotRecovery>,
    pub ready: oneshot::Sender<Option<ReconnectFence>>,
}

pub(crate) struct SnapshotResult {
    pub symbol: Ustr,
    pub owner: Arc<SnapshotRecovery>,
    pub fence: ReconnectFence,
    pub cancellation: CancellationToken,
    pub body: String,
    pub applied: oneshot::Sender<Result<(), SnapshotError>>,
}

pub(crate) enum SnapshotMessage {
    Admit(SnapshotAdmission),
    Apply(SnapshotResult),
}

async fn reserve(
    messages: &UnboundedSender<SnapshotMessage>,
    symbol: Ustr,
    owner: &Arc<SnapshotRecovery>,
) -> Result<Option<ReconnectFence>, SnapshotError> {
    let (ready, permit) = oneshot::channel();
    messages
        .send(SnapshotMessage::Admit(SnapshotAdmission {
            symbol,
            owner: Arc::clone(owner),
            ready,
        }))
        .map_err(|_| SnapshotError::SessionClosed)?;
    permit.await.map_err(|_| SnapshotError::SessionClosed)
}

pub(crate) fn spawn(
    symbol: Ustr,
    owner: Arc<SnapshotRecovery>,
    http: AsterHttpClient,
    messages: UnboundedSender<SnapshotMessage>,
    depth: u16,
    deadline: Duration,
) {
    let guard = owner.cancellation.clone().drop_guard();
    get_runtime().spawn(async move {
        let _guard = guard;
        // Cold-start books wait their turn outside the shared retry budget.
        let first = tokio::select! {
            biased;
            () = owner.cancellation.cancelled() => return,
            permit = reserve(&messages, symbol, &owner) => match permit {
                Ok(permit) => permit,
                Err(_) => return,
            },
        };
        let first = Mutex::new(Some(first));
        owner
            .run(
                symbol,
                deadline,
                |cancellation, gate| {
                    let first = &first;
                    let http = &http;
                    let messages = &messages;
                    let owner = &owner;
                    async move {
                        let initial = first.lock().await.take();
                        let fence = match initial {
                            Some(permit) => permit,
                            None => reserve(messages, symbol, owner).await?,
                        }
                        .filter(ReconnectFence::is_current)
                        .ok_or(SnapshotError::Reconnecting)?;
                        let attempt = async {
                            let body = http.fetch_depth(symbol.as_str(), depth).await?;
                            let (applied, outcome) = oneshot::channel();
                            gate.open();
                            messages
                                .send(SnapshotMessage::Apply(SnapshotResult {
                                    symbol,
                                    owner: Arc::clone(owner),
                                    fence,
                                    cancellation,
                                    body,
                                    applied,
                                }))
                                .map_err(|_| SnapshotError::SessionClosed)?;
                            outcome.await.map_err(|_| SnapshotError::SessionClosed)?
                        };
                        if deadline.is_zero() {
                            attempt.await
                        } else {
                            tokio::time::timeout(deadline, attempt)
                                .await
                                .map_err(|_| SnapshotError::DeadlineExpired)?
                        }
                    }
                },
                SnapshotError::retryable,
                SnapshotError::Retryable,
                || SnapshotError::DeadlineExpired,
            )
            .await;
    });
}
