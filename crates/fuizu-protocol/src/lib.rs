use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdentifyRequest {
    /// Shard ID to identify
    pub id: u32,
    /// Name of the host that is requesting identification
    pub host_name: String
}

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct IdentifyAllowance {
    /// ID of the Shard which is allowed to identify
    pub id: u32,
}
