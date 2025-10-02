//! Memory based [`Queue`] implementation and supporting items.

use std::collections::VecDeque;
use std::fmt::Debug;
use std::iter;

use tokio::sync::{mpsc, oneshot};
use tokio::task::yield_now;
use tokio::time::{Duration, Instant, sleep_until};
use twilight_model::gateway::SessionStartLimit;

/// Period between buckets.
pub const IDENTIFY_DELAY: Duration = Duration::from_secs(5);

/// Duration from the first identify until the remaining count resets to the
/// total count.
pub const LIMIT_PERIOD: Duration = Duration::from_secs(60 * 60 * 24);

/// Possible messages from the [`Queue`] to the [`runner`].
#[derive(Debug)]
enum Message {
    /// Request a permit.
    Request {
        /// For this shard.
        shard: u32,
        /// Indicate readiness through this sender.
        tx: oneshot::Sender<bool>
    },
    /// Cancel a permit.
    Cancel(u32),
    /// Update the runner's settings.
    Update(Settings),
    /// Retrieve the runner's settings.
    GetSettings(oneshot::Sender<Settings>)
}

/// [`runner`]'s settings.
#[derive(Debug)]
struct Settings {
    /// The maximum number of concurrent permits to grant. `0` instantly grants
    /// all permits.
    max_concurrency: u16,
    /// Remaining daily permits.
    remaining: u32,
    /// Time until the daily permits reset.
    reset_after: Duration,
    /// The number of permits to reset to.
    total: u32
}

/// [`Queue`]'s background task runner.
///
/// Buckets requests such that only one timer is necessary.
async fn runner(
    mut rx: mpsc::UnboundedReceiver<Message>,
    Settings {
        max_concurrency,
        mut remaining,
        reset_after,
        mut total
    }: Settings
) {
    let (interval, reset_at) = {
        let now = Instant::now();
        (sleep_until(now), sleep_until(now + reset_after))
    };

    tokio::pin!(interval, reset_at);

    let mut queues = iter::repeat_with(VecDeque::new)
        .take(max_concurrency.into())
        .collect::<Box<_>>();

    #[allow(clippy::ignored_unit_patterns)]
    loop {
        tokio::select! {
            biased;
            _ = &mut reset_at, if remaining != total => {
                remaining = total;
            }
            message = rx.recv() => {
                match message {
                    Some(Message::Request { shard, tx }) => {
                        if queues.is_empty() {
                            _ = tx.send(true);
                            continue;
                        }

                        queues[shard as usize % queues.len()].push_back((shard, tx));
                    }
                    Some(Message::Cancel(shard)) => {
                        if queues.is_empty() {
                            continue;
                        }

                        let queue = &mut queues[shard as usize % queues.len()];

                        let Some(idx) = queue.iter().position(|(s, _)| *s == shard) else {
                            continue;
                        };

                        let (_, tx) = queue.remove(idx).expect("cannot fail");

                        let _ = tx.send(false);

                        tracing::debug!(shard, "cancelled");
                    }
                    Some(Message::GetSettings(channel)) => {
                        let settings = Settings {
                            max_concurrency,
                            remaining,
                            reset_after: reset_at.deadline() - Instant::now(),
                            total
                        };

                        if channel.send(settings).is_err() {
                            tracing::warn!("failed to send settings");
                        }
                    }
                    Some(Message::Update(update)) => {
                        let (max_concurrency, reset_after);
                        Settings {
                            max_concurrency,
                            remaining,
                            reset_after,
                            total,
                        } = update;

                        if remaining != total {
                            reset_at.as_mut().reset(Instant::now() + reset_after);
                        }

                        if max_concurrency as usize != queues.len() {
                            let unbalanced = queues.into_vec().into_iter().flatten();
                            queues = iter::repeat_with(VecDeque::new)
                                .take(max_concurrency.into())
                                .collect();
                            for (shard, tx) in unbalanced {
                                let key = (shard % u32::from(max_concurrency)) as usize;
                                queues[key].push_back((shard, tx));
                            }
                        }
                    }
                    None => break,
                }
            }
            _ = &mut interval, if queues.iter().any(|queue| !queue.is_empty()) => {
                let now = Instant::now();
                let span = tracing::info_span!("bucket", moment = ?now);

                interval.as_mut().reset(now + IDENTIFY_DELAY);

                if remaining == total {
                    reset_at.as_mut().reset(now + LIMIT_PERIOD);
                }

                for (key, queue) in queues.iter_mut().enumerate() {
                    if remaining == 0 {
                        tracing::debug!(
                            refill_delay = ?reset_at.deadline().saturating_duration_since(now),
                            "exhausted available permits"
                        );
                        (&mut reset_at).await;
                        remaining = total;

                        break;
                    }

                    while let Some((shard, tx)) = queue.pop_front() {
                        if tx.send(true).is_err() {
                            continue;
                        }

                        tracing::debug!(parent: &span, key, shard);
                        remaining -= 1;

                        // Reschedule behind shard for ordering correctness.
                        yield_now().await;

                        break;
                    }
                }
            }
        }
    }
}

/// Memory based [`Queue`] implementation backed by an efficient background
/// task.
///
/// [`Queue::update`] allows for dynamically changing the queue's
/// settings.
///
/// Cloning the queue is cheap and just increments a reference counter.
///
/// **Note:** A `max_concurrency` of `0` processes all requests instantly,
/// effectively disabling the queue.
#[derive(Clone, Debug)]
pub struct Queue {
    /// Sender to communicate with the background [task runner].
    ///
    /// [task runner]: runner
    tx: mpsc::UnboundedSender<Message>
}

impl Queue {
    /// Creates a new `Queue` with custom settings.
    ///
    /// # Panics
    ///
    /// Panics if `total` < `remaining`.
    pub fn new(max_concurrency: u16, remaining: u32, reset_after: Duration, total: u32) -> Self {
        assert!(total >= remaining);
        let (tx, rx) = mpsc::unbounded_channel();

        tokio::spawn(runner(
            rx,
            Settings { max_concurrency, remaining, reset_after, total }
        ));

        Self { tx }
    }

    /// Update the queue with new info from the [Get Gateway Bot] endpoint.
    ///
    /// May be regularly called as the bot joins/leaves guilds.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use twilight_gateway_queue::Queue;
    /// # let rt = tokio::runtime::Builder::new_current_thread()
    /// #     .enable_time()
    /// #     .build()
    /// #     .unwrap();
    /// use std::time::Duration;
    ///
    /// use twilight_http::Client;
    ///
    /// # rt.block_on(async {
    /// # let queue = Queue::default();
    /// # let token = String::new();
    /// let client = Client::new(token);
    /// let session = client
    ///     .gateway()
    ///     .authed()
    ///     .await?
    ///     .model()
    ///     .await?
    ///     .session_start_limit;
    /// queue.update(
    ///     session.max_concurrency,
    ///     session.remaining,
    ///     Duration::from_millis(session.reset_after),
    ///     session.total
    /// );
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// ```
    ///
    /// # Panics
    ///
    /// Panics if `total` < `remaining`.
    ///
    /// [Get Gateway Bot]: https://discord.com/developers/docs/topics/gateway#get-gateway-bot
    pub fn update(&self, new: &SessionStartLimit) {
        assert!(new.total >= new.remaining);

        self.tx
            .send(Message::Update(Settings {
                max_concurrency: new.max_concurrency,
                remaining: new.remaining,
                reset_after: Duration::from_millis(new.reset_after),
                total: new.remaining
            }))
            .expect("receiver dropped after sender");
    }

    /// Get the current settings of the queue.
    pub async fn get_current(&self) -> SessionStartLimit {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Message::GetSettings(tx))
            .expect("receiver dropped after sender");

        let settings = rx.await.expect("failed to receive settings");
        SessionStartLimit {
            max_concurrency: settings.max_concurrency,
            remaining: settings.remaining,
            reset_after: settings.reset_after.as_millis() as u64,
            total: settings.total
        }
    }

    pub fn dequeue(&self, shard: u32) {
        self.tx
            .send(Message::Cancel(shard))
            .expect("receiver dropped after sender");
    }

    pub fn enqueue(&self, shard: u32) -> oneshot::Receiver<bool> {
        let (tx, rx) = oneshot::channel();

        self.tx
            .send(Message::Request { shard, tx })
            .expect("receiver dropped after sender");

        rx
    }
}

impl Default for Queue {
    /// Creates a new `Queue` with Discord's default settings.
    ///
    /// Currently these are:
    ///
    /// * `max_concurrency`: 1
    /// * `remaining`: 1000
    /// * `reset_after`: [`LIMIT_PERIOD`]
    /// * `total`: 1000.
    fn default() -> Self {
        Self::new(1, 1000, LIMIT_PERIOD, 1000)
    }
}
