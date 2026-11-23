//! Loopback relay for the adapter socket. The partial-depth oracle bypasses it.

use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering::Relaxed},
    },
    time::Duration,
};

use anyhow::{Context, Result};
use futures_util::{SinkExt, StreamExt};
use nt_dex_aster::{common::WS_URL, websocket::messages::Envelope};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::watch,
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{accept_async, connect_async, tungstenite::Message};

use super::SYMBOLS;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Fault {
    Pass,
    Cut,
    Freeze,
}

#[derive(Debug, Default)]
struct Counters {
    connections: AtomicUsize,
    cuts: AtomicUsize,
    frozen_depth: [AtomicUsize; SYMBOLS.len()],
}

#[derive(Debug)]
pub(super) struct Relay {
    pub(super) base_url: String,
    directive: watch::Sender<Fault>,
    counters: Arc<Counters>,
    task: JoinHandle<()>,
}

impl Relay {
    pub(super) async fn bind() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let base_url = format!("ws://{}", listener.local_addr()?);
        let (directive, mut fault) = watch::channel(Fault::Pass);
        let counters = Arc::new(Counters::default());
        let task_counters = Arc::clone(&counters);
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                match forward(socket, &mut fault, &task_counters).await {
                    Ok(true) => {
                        task_counters.cuts.fetch_add(1, Relaxed);
                    }
                    Ok(false) => {}
                    Err(error) => log::warn!("adapter relay: {error:#}"),
                }
            }
        });
        Ok(Self {
            base_url,
            directive,
            counters,
            task,
        })
    }

    pub(super) fn inject(&self, fault: Fault) -> Result<()> {
        self.directive.send(fault).context("adapter relay exited")
    }

    pub(super) fn connection_counts(&self) -> (usize, usize) {
        (
            self.counters.connections.load(Relaxed),
            self.counters.cuts.load(Relaxed),
        )
    }

    pub(super) fn frozen_depth(&self) -> [usize; SYMBOLS.len()] {
        self.counters
            .frozen_depth
            .each_ref()
            .map(|count| count.load(Relaxed))
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn forward(
    socket: TcpStream,
    fault: &mut watch::Receiver<Fault>,
    counters: &Counters,
) -> Result<bool> {
    // A cut belongs to the socket that was active when the directive arrived.
    fault.borrow_and_update();
    let url = format!("{WS_URL}/stream");
    let (mut upstream, _) = timeout(Duration::from_secs(8), connect_async(url)).await??;
    let mut adapter = timeout(Duration::from_secs(8), accept_async(socket)).await??;
    counters.connections.fetch_add(1, Relaxed);
    loop {
        tokio::select! {
            changed = fault.changed() => {
                changed.context("relay controller exited")?;
                if *fault.borrow_and_update() == Fault::Cut {
                    // Drop both sockets without a WebSocket close handshake.
                    return Ok(true);
                }
            }
            incoming = upstream.next() => match incoming.transpose()? {
                Some(Message::Text(text)) => {
                    let freezing = *fault.borrow() == Fault::Freeze;
                    if !freezing || !withhold_depth(&text, counters)? {
                        adapter.send(Message::Text(text)).await?;
                    }
                }
                Some(Message::Binary(data)) => adapter.send(Message::Binary(data)).await?,
                Some(Message::Ping(_)) => upstream.flush().await?,
                Some(Message::Close(_)) | None => return Ok(false),
                Some(_) => {}
            },
            outgoing = adapter.next() => match outgoing.transpose()? {
                Some(message @ (Message::Text(_) | Message::Binary(_))) => {
                    upstream.send(message).await?;
                }
                Some(Message::Ping(_)) => adapter.flush().await?,
                Some(Message::Close(_)) | None => return Ok(false),
                Some(_) => {}
            },
        }
    }
}

// Keep heartbeats and control replies flowing. Discarding depth frames creates a
// real discontinuity when depth delivery resumes, without waiting for venue timeouts.
fn withhold_depth(text: &str, counters: &Counters) -> Result<bool> {
    let envelope: Envelope<'_> = serde_json::from_str(text)?;
    let Some(symbol) = envelope
        .stream
        .and_then(|stream| stream.strip_suffix("@depth@100ms"))
    else {
        return Ok(false);
    };
    let count = SYMBOLS
        .iter()
        .zip(&counters.frozen_depth)
        .find_map(|(expected, count)| expected.eq_ignore_ascii_case(symbol).then_some(count))
        .with_context(|| format!("unexpected adapter depth stream {symbol}"))?;
    count.fetch_add(1, Relaxed);
    Ok(true)
}
