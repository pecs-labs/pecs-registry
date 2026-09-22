use super::{SelectContext, Selector};
use crate::instance::ServiceInstance;

/// 随机负载均衡器（Random）
#[derive(Default, Clone)]
pub struct RandomSelector;

impl RandomSelector {
    pub fn new() -> Self {
        Self
    }
}

impl Selector for RandomSelector {
    fn select<'a>(
        &self,
        _ctx: &SelectContext,
        instances: &'a [ServiceInstance],
    ) -> Option<&'a ServiceInstance> {
        let len = instances.len();
        if len == 0 {
            return None;
        }
        if len == 1 {
            return Some(&instances[0]);
        }

        let idx = (std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0))
            % len;
        Some(&instances[idx])
    }
}
