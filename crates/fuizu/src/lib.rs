mod actor;

use std::sync::Arc;

use actor::{Fuizu, QueuedIdentify};
use async_nats::Client;
use async_nats::subject::ToSubject;
use fuizu_protocol::Request;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
#[cfg(feature = "twilight")]
use twilight_gateway_queue::Queue;

#[derive(Debug, Clone)]
pub struct FuizuClient {
    tx: mpsc::UnboundedSender<QueuedIdentify>,
    inner: Arc<Fuizu>
}

#[derive(Debug, Error)]
pub enum FuizuError {
    /// Failed to serialize the request
    #[error("Failed to serialize request")]
    Serialize(serde_json::Error),

    /// Failed to send the request
    #[error("Failed to send request")]
    Send(#[from] async_nats::RequestError),

    /// Failed to deserialize the response
    #[error("Failed to deserialize response")]
    Deserialize(serde_json::Error)
}

#[allow(clippy::needless_pass_by_value)]
impl FuizuClient {
    #[must_use]
    pub fn new<RS: ToSubject, IS: ToSubject>(
        host_name: String, client: Client, request_subject: RS, inbox_subject: IS
    ) -> Self {
        let fuizu = Fuizu {
            host_name,
            client,
            request_subject: request_subject.to_subject(),
            inbox_subject: inbox_subject.to_subject()
        };

        let inner = Arc::new(fuizu);

        let (msg_tx, msg_rx) = mpsc::unbounded_channel();
        tokio::spawn(inner.clone().actor(msg_rx));

        Self { inner, tx: msg_tx }
    }

    /// Retrieve a `oneshot::Receiver` that will be resolved when the IDENITFY
    /// requst is allowed by the server.
    #[must_use]
    pub fn waiter(&self, id: u32) -> oneshot::Receiver<()> {
        let (req, rx) = QueuedIdentify::new(id);
        let _ = self.tx.send(req);
        rx
    }

    /// Retrieve the gateway information from the server.
    ///
    /// This method should be used instead of directly calling the
    /// `/gateway/bot` Discord API endpoint. As the identify server already
    /// stores this info for itself, it can be retrieved without fear of
    /// running into API rate-limits or retrieving inaccurate information.
    pub async fn gateway_info<G: DeserializeOwned>(&self) -> Result<G, FuizuError> {
        let payload =
            serde_json::to_vec(&Request::RetrieveGateway).map_err(FuizuError::Serialize)?;

        let response = self
            .inner
            .client
            .request(self.inner.request_subject.clone(), payload.into())
            .await?;

        let response: G =
            serde_json::from_slice(&response.payload).map_err(FuizuError::Deserialize)?;

        Ok(response)
    }
}

#[cfg(feature = "twilight")]
impl Queue for FuizuClient {
    fn enqueue(&self, id: u32) -> oneshot::Receiver<()> {
        self.waiter(id)
    }
}
