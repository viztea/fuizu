use async_nats::subject::ToSubject;
use bytes::Bytes;
use serde::Serialize;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum SendError {
    #[error("Failed to serialize message: {0}")]
    Serialize(#[from] serde_json::Error),

    #[error("Failed to publish message: {0}")]
    Publish(#[from] async_nats::PublishError)
}

pub type NatsCoreClient = async_nats::Client;

pub async fn send<S: ToSubject, R: Serialize>(
    nats_core: &NatsCoreClient, subject: S, response: &R
) -> Result<(), SendError> {
    let response = serde_json::to_vec(response)?;
    nats_core.publish(subject, Bytes::from(response)).await?;

    Ok(())
}

pub async fn send_request<S: ToSubject, R: ToSubject, T: Serialize>(
    nats_core: &NatsCoreClient, subject: S, reply: R, response: &T
) -> Result<(), SendError> {
    let response = serde_json::to_vec(response)?;
    nats_core
        .publish_with_reply(subject, reply, Bytes::from(response))
        .await?;

    Ok(())
}
