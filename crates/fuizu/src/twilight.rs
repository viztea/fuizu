use twilight_gateway_queue::Queue;

use crate::Fuizu;

impl Queue for Fuizu {
    fn enqueue(&self, id: u32) -> tokio::sync::oneshot::Receiver<()> {
        self.waiter(id)
    }
}
