#[cfg(feature = "nats")]
pub mod nats;
pub mod twilight;

use std::collections::BTreeMap;

use async_trait::async_trait;
use fuizu_protocol::{IdentifyAllowance, Request};
use tokio::sync::{mpsc, oneshot};

#[derive(Debug, Clone)]
pub struct Fuizu {
    tx: mpsc::UnboundedSender<QueuedIdentify>
}

struct QueuedIdentify {
    tx: oneshot::Sender<()>,
    id: u32
}

#[derive(Debug, Clone)]
struct Inner<T: Transport> {
    host_name: String,
    transport: T
}

#[async_trait]
pub trait Transport: Send + Sync {
    type Error: std::error::Error + Send + Sync;

    /// Send an identification request.
    async fn send(&self, message: Request) -> Result<(), Self::Error>;

    /// Receive an allowance message.
    async fn recv(&mut self) -> Result<Option<IdentifyAllowance>, Self::Error>;
}

type RequestMap = BTreeMap<u32, oneshot::Sender<()>>;
impl<T: Transport> Inner<T> {
    async fn actor(mut self, mut rx: mpsc::UnboundedReceiver<QueuedIdentify>) {
        let mut requests = RequestMap::new();
        loop {
            tokio::select! {
                event = self.transport.recv() => Self::handle_allowance(event, &mut requests),
                Some(request) = rx.recv() => self.handle_request(request, &mut requests).await,
            }
        }
    }

    async fn handle_request(&self, request: QueuedIdentify, requests: &mut RequestMap) {
        if let Err(err) = self
            .transport
            .send(Request::Identify { id: request.id, host_name: self.host_name.clone() })
            .await
        {
            tracing::error!(%err, shard_id=request.id, "Failed to send identify request");
            return;
        }

        requests.insert(request.id, request.tx);
    }

    fn handle_allowance(
        message: Result<Option<IdentifyAllowance>, T::Error>, requests: &mut RequestMap
    ) {
        match message {
            Ok(Some(request)) => {
                if let Some(tx) = requests.remove(&request.id) {
                    let _ = tx.send(());
                }
            }

            Err(err) => {
                tracing::error!(%err, "Failed to receive allowance");
            }

            _ => {}
        }
    }
}

impl Fuizu {
    #[must_use]
    pub fn waiter(&self, id: u32) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        let _ = self.tx.send(QueuedIdentify { tx, id });

        rx
    }

    #[must_use]
    pub fn new<T: Transport + 'static>(host_name: String, transport: T) -> Self {
        let inner = Inner { host_name, transport };

        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        tokio::spawn(inner.actor(msg_rx));

        Self { tx: msg_tx }
    }
}
