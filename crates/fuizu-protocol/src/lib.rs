use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
#[serde(tag = "type", rename_all = "snake_case", content = "data")]
pub enum Request {
    /// Request to retrieve the current gateway information
    RetrieveGateway,
    /// Request to identify a shard
    RequestIdentify {
        /// Shard ID to identify
        id: u32,
        /// Name of the host that is requesting identification
        host_name: String
    },
    /// Request to cancel identification of a shard
    CancelIdentify {
        /// Shard ID to cancel identification
        id: u32,
        /// Name of the host that is canceling identification
        host_name: String
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdentifyAllowance {
    /// ID of the Shard which is allowed to identify
    pub id: u32,
    /// Whether the identification was canceled
    pub canceled: bool
}
