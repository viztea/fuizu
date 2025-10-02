#![feature(impl_trait_in_assoc_type)]
mod queue;

use std::pin::Pin;

use async_nats::Message;
use color_eyre::eyre::Result;
use fuizu_protocol::{IdentifyAllowance, Request};
use futures_util::StreamExt;
use nats_util::{NatsCoreClient, send};
use tokio::spawn;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Duration, Instant, Sleep};
use twilight_http::Client;
use twilight_http::client::ClientBuilder;
use twilight_model::gateway::connection_info::BotConnectionInfo;

use crate::queue::Queue;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    // setup nats
    let nats_url = std::env::var("NATS_URL").expect("NATS_URL is required");
    let nats = async_nats::connect(nats_url.clone()).await?;
    tracing::info!("Connected to NATS server: {}", nats_url);

    // setup discord api client.
    let mut client = ClientBuilder::new()
        .token(std::env::var("GATEWAY_QUEUE_TOKEN").expect("Missing GATEWAY_QUEUE_TOKEN"));
    if let Ok(proxy_url) = std::env::var("GATEWAY_HTTP_PROXY_URL") {
        client = client.proxy(proxy_url, true).ratelimiter(None);
    }

    let client = client.build();

    // create the server
    let server = EaraIdentifyServer::new(
        nats,
        std::env::var("GATEWAY_QUEUE_SUBJECT_REQUESTS").expect("Missing request subject"),
        std::env::var("GATEWAY_QUEUE_UPDATE_INTERVAL")
            .ok()
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_secs),
        client
    )
    .await?;

    // wait for ctrl+c
    let _ = tokio::signal::ctrl_c().await;

    // cancel the server
    server.cancel().await?;

    Ok(())
}

struct Inner {
    /// NATS client.
    nats: async_nats::Client,
    /// Subject to listen for identify requests on.
    request_subject: String,
    /// The interval in which to update the identify queue with new session
    /// start limits.
    queue_update_interval: Option<Duration>,
    /// Queue for identifying shards.
    queue: Queue,
    /// Discord API client.
    client: Client,
    /// Current session start limit.
    current_gateway: BotConnectionInfo
}

enum UpdateInterval {
    Interval(Pin<Box<Sleep>>, Duration),
    Disabled
}

impl UpdateInterval {
    fn new(interval: Option<Duration>) -> Self {
        match interval {
            Some(interval) => {
                let inner = Box::pin(tokio::time::sleep(interval));

                Self::Interval(inner, interval)
            }
            None => Self::Disabled
        }
    }

    fn reset(&mut self, now: Instant) {
        match self {
            Self::Interval(inner, interval) => inner.as_mut().reset(now + *interval),
            Self::Disabled => {}
        }
    }
}

impl Future for UpdateInterval {
    type Output = ();

    fn poll(
        self: Pin<&mut Self>, cx: &mut std::task::Context<'_>
    ) -> std::task::Poll<Self::Output> {
        match self.get_mut() {
            Self::Interval(pin, _) => pin.as_mut().poll(cx),
            Self::Disabled => std::task::Poll::Pending
        }
    }
}

impl Inner {
    fn runner(self, cancel: oneshot::Receiver<()>) -> JoinHandle<()> {
        spawn(async move {
            if let Err(err) = self.run(cancel).await {
                tracing::error!(%err, "Identify server failed");
            }
        })
    }

    async fn run(mut self, mut cancel: oneshot::Receiver<()>) -> Result<()> {
        let mut requests = self.nats.subscribe(self.request_subject.clone()).await?;

        let mut allowances = JoinSet::new();

        let mut update_interval = UpdateInterval::new(self.queue_update_interval);

        tracing::info!("Identify server started");

        loop {
            tokio::select! {
                () = &mut update_interval => {
                    let now = Instant::now();
                    if let Err(err) = self.handle_update().await {
                        tracing::error!(%err, "Failed to update session start limits");
                    }

                    update_interval.reset(now);
                }

                _ = &mut cancel => {
                    break;
                }

                event = requests.next() => {
                    let Some(message) = event else {
                        break;
                    };

                    self.handle_identify_request(&message, &mut allowances).await;
                }
            }
        }

        tracing::error!("Subscription for identify requests ended abruptly.");

        Ok(())
    }

    async fn handle_update(&mut self) -> Result<()> {
        let gateway = self.client.gateway().authed().await?.model().await?;
        tracing::info!(
            "Updating session start limit: {:?}",
            gateway.session_start_limit
        );

        self.queue.update(&gateway.session_start_limit);
        self.current_gateway = gateway;

        Ok(())
    }

    async fn handle_identify_request(&self, message: &Message, join_set: &mut JoinSet<()>) {
        let request = match serde_json::from_slice::<Request>(&message.payload) {
            Ok(request) => request,
            Err(err) => {
                tracing::error!(%err, "Failed to parse Fuizu request");
                return;
            }
        };

        match request {
            Request::RetrieveGateway => {
                let Some(requester) = message.reply.clone() else {
                    tracing::error!("Received request without reply subject");
                    return;
                };

                let gateway = BotConnectionInfo {
                    session_start_limit: self.queue.get_current().await,
                    shards: self.current_gateway.shards,
                    url: self.current_gateway.url.clone()
                };

                if let Err(err) = send(&self.nats, requester.clone(), &gateway).await {
                    tracing::error!(%err, %requester, "--- IDENTIFY: failed to send gateway info");
                } else {
                    tracing::info!(%requester, "--- IDENTIFY: sent gateway info");
                }
            }
            Request::RequestIdentify { id: shard_id, host_name } => {
                let Some(requester) = message.reply.clone() else {
                    tracing::error!("Received request without reply subject");
                    return;
                };

                tracing::info!(
                    shard_id,
                    requester = host_name,
                    ">>> IDENTIFY: received identify request for shard"
                );

                let shard = self.queue.enqueue(shard_id);
                join_set.spawn({
                    let nats = self.nats.clone();
                    async move {
                        let Ok(allowed) = shard.await else {
                            return;
                        };

                        if allowed {
                            tracing::info!(shard_id, %requester, host_name, "<<< IDENTIFY: sending identify allowance for shard");
                        } else {
                            tracing::info!(shard_id, %requester, host_name, "--- IDENTIFY: shard identify was canceled");
                        }

                        if let Err(err) = send(&nats, requester.clone(), &IdentifyAllowance { id: shard_id, canceled: !allowed }).await {
                            tracing::error!(%err, shard_id, %requester, host_name, "--- IDENTIFY: failed to publish identify allowance");
                        }
                    }
                });
            }
            Request::CancelIdentify { id, host_name } => {
                tracing::info!(shard_id=id, host_name, ">>> IDENTIFY: received cancel identify request");
                self.queue.dequeue(id);
            }
        }
    }
}

struct EaraIdentifyServer {
    cancel_tx: oneshot::Sender<()>,
    handle: JoinHandle<()>
}

impl EaraIdentifyServer {
    async fn new(
        nats: NatsCoreClient, request_subject: String, queue_update_interval: Option<Duration>,
        client: Client
    ) -> Result<Self> {
        let initial_gateway = client.gateway().authed().await?.model().await?;
        tracing::info!(
            "Received initial session start limit: {:?}",
            initial_gateway.session_start_limit
        );

        let queue = Queue::new(
            initial_gateway.session_start_limit.max_concurrency,
            initial_gateway.session_start_limit.remaining,
            Duration::from_millis(initial_gateway.session_start_limit.reset_after),
            initial_gateway.session_start_limit.total
        );

        let (cancel_tx, cancel_rx) = oneshot::channel();

        let handle = Inner {
            nats,
            request_subject,
            queue_update_interval,
            queue,
            client,
            current_gateway: initial_gateway
        }
        .runner(cancel_rx);

        Ok(Self { cancel_tx, handle })
    }

    async fn cancel(self) -> Result<()> {
        let _ = self.cancel_tx.send(());
        self.handle.await?;
        Ok(())
    }
}
