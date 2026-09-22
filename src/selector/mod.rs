pub mod canary;
pub mod random;
pub mod round_robin;
pub mod weighted;

pub use canary::TagFilterSelector;
pub use random::RandomSelector;
pub use round_robin::RoundRobinSelector;
pub use weighted::WeightedRoundRobinSelector;

use crate::instance::ServiceInstance;
use std::collections::HashMap;

/// 负载均衡与路由上下文
#[derive(Default, Clone, Debug)]
pub struct SelectContext {
    pub service_name: String,
    pub client_ip: Option<String>,
    pub required_tag: Option<(String, String)>,
    pub metadata: HashMap<String, String>,
}

impl SelectContext {
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            client_ip: None,
            required_tag: None,
            metadata: HashMap::new(),
        }
    }

    pub fn with_tag(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.required_tag = Some((key.into(), val.into()));
        self
    }

    pub fn with_client_ip(mut self, ip: impl Into<String>) -> Self {
        self.client_ip = Some(ip.into());
        self
    }

    pub fn with_metadata(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), val.into());
        self
    }
}

/// 统一负载均衡选择器契约
pub trait Selector: Send + Sync {
    /// 从健康候选节点列表中挑选出一个最佳目标实例
    fn select<'a>(
        &self,
        ctx: &SelectContext,
        instances: &'a [ServiceInstance],
    ) -> Option<&'a ServiceInstance>;
}
