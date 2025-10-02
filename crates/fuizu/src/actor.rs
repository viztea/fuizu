use std::collections::BTreeMap;
use std::sync::Arc;

use async_nats::{Client, Message, Subject};
use fuizu_protocol::{IdentifyAllowance, Request};
use futures_util::StreamExt;
use futures_util::future::{Either, select};
use tokio::sync::{mpsc, oneshot};

pub(crate) struct QueuedIdentify {
    tx: oneshot::Sender<()>,
    id: u32
}

impl QueuedIdentify {
    pub fn new(shard_id: u32) -> (Self, oneshot::Receiver<()>) {
        let (tx, rx) = oneshot::channel();
        (Self { tx, id: shard_id }, rx)
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Fuizu {
    pub(crate) host_name: String,
    pub(crate) client: Client,
    pub(crate) request_subject: Subject,
    pub(crate) inbox_subject: Subject
}

type RequestMap = BTreeMap<u32, oneshot::Sender<()>>;

impl Fuizu {
    pub(crate) async fn actor(self: Arc<Self>, mut rx: mpsc::UnboundedReceiver<QueuedIdentify>) {
        let mut requests = RequestMap::new();

        let mut inbox = self
            .client
            .subscribe(self.inbox_subject.clone())
            .await
            .expect("Failed to subscribe to inbox subject");

        loop {
            match select(inbox.next(), std::pin::pin!(rx.recv())).await {
                Either::Left((Some(message), _)) => Self::handle_allowance(&message, &mut requests),
                Either::Right((Some(request), _)) => {
                    self.handle_request(request, &mut requests).await;
                }
                _ => {}
            }
        }
    }

    async fn handle_request(&self, request: QueuedIdentify, requests: &mut RequestMap) {
        let payload = Request::RequestIdentify { id: request.id, host_name: self.host_name.clone() };

        let payload = match serde_json::to_vec(&payload) {
            Ok(payload) => payload,
            Err(err) => {
                tracing::error!(%err, shard_id=request.id, "Failed to serialize identify request");
                return;
            }
        };

        if let Err(err) = self
            .client
            .publish_with_reply(
                self.request_subject.clone(),
                self.inbox_subject.clone(),
                payload.into()
            )
            .await
        {
            tracing::error!(%err, shard_id=request.id, "Failed to send identify request");
            return;
        }

        requests.insert(request.id, request.tx);
    }

    fn handle_allowance(message: &Message, requests: &mut RequestMap) {
        let message: IdentifyAllowance = match serde_json::from_slice(&message.payload) {
            Ok(message) => message,
            Err(err) => {
                tracing::error!(%err, "Failed to deserialize allowance message");
                return;
            }
        };

        tracing::debug!(shard_id = message.id, "Received allowance");
        if let Some(tx) = requests.remove(&message.id) {
            let _ = tx.send(());
        }
    }
}
