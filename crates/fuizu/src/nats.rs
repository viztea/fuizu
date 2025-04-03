use async_nats::subject::ToSubject;
use async_nats::{Client, Subject, SubscribeError, Subscriber};
use async_trait::async_trait;
use fuizu_protocol::{IdentifyAllowance, Request};
use futures_util::StreamExt;
use thiserror::Error;

use crate::Transport;

pub struct NatsTransport {
    client: Client,
    request_subject: Subject,
    inbox: Subscriber
}

#[derive(Debug, Error)]
pub enum NatsError {
    #[error("Failed to subscribe to queue allowances")]
    Subscribe(#[from] async_nats::Error),

    #[error("Failed to send identification request")]
    Send(#[from] async_nats::PublishError),

    #[error("Failed to (de)serialize message")]
    Serialization(#[from] serde_json::Error)
}

impl NatsTransport {
    pub async fn new<RS: ToSubject, I: ToString>(
        client: Client, request_subject: RS, inbox: I
    ) -> Result<Self, SubscribeError> {
        let inbox = client.subscribe(inbox.to_string()).await?;

        Ok(Self {
            client,
            request_subject: request_subject.to_subject(),
            inbox
        })
    }
}

#[async_trait]
impl Transport for NatsTransport {
    type Error = NatsError;

    /// Send an identification request.
    async fn send(&self, message: Request) -> Result<(), Self::Error> {
        let message = serde_json::to_vec(&message)?;
        self.client
            .publish(self.request_subject.clone(), message.into())
            .await?;

        Ok(())
    }

    /// Receive an allowance message.
    async fn recv(&mut self) -> Result<Option<IdentifyAllowance>, Self::Error> {
        let Some(message) = self.inbox.next().await else {
            return Ok(None);
        };

        Ok(serde_json::from_slice(&message.payload)?)
    }
}
