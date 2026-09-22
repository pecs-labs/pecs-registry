use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use super::{SelectContext, Selector};
use crate::instance::ServiceInstance;

/// 经典无锁轮询负载均衡器（Round-Robin）
///
/// 针对每个微服务独立维护无锁原子计数器，纳秒级无等待完成路由选择
#[derive(Default)]
pub struct RoundRobinSelector {
    counters: RwLock<HashMap<String, Arc<AtomicUsize>>>,
}

impl RoundRobinSelector {
    pub fn new() -> Self {
        Self {
            counters: RwLock::new(HashMap::new()),
        }
    }

    fn get_counter(&self, service_name: &str) -> Arc<AtomicUsize> {
        {
            let read = self.counters.read().expect("round robin lock poisoned");
            if let Some(counter) = read.get(service_name) {
                return counter.clone();
            }
        }
        let mut write = self.counters.write().expect("round robin lock poisoned");
        write
            .entry(service_name.to_string())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }
}

impl Selector for RoundRobinSelector {
    fn select<'a>(
        &self,
        ctx: &SelectContext,
        instances: &'a [ServiceInstance],
    ) -> Option<&'a ServiceInstance> {
        let len = instances.len();
        if len == 0 {
            return None;
        }
        if len == 1 {
            return Some(&instances[0]);
        }

        let counter = self.get_counter(&ctx.service_name);
        let idx = counter.fetch_add(1, Ordering::Relaxed) % len;
        Some(&instances[idx])
    }
}
