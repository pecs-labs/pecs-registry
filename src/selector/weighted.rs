use std::collections::HashMap;
use std::sync::RwLock;

use super::{SelectContext, Selector};
use crate::instance::ServiceInstance;

/// 平滑加权轮询负载均衡器（Smooth Weighted Round-Robin）
///
/// 借鉴 Nginx 平滑加权算法：
/// 每次选择具有最大 current_weight 的节点，命中后将该节点权重减去 total_weight，
/// 避免大权重节点在初期被连续集中冲击，实现请求在时间线上的均匀分布。
#[derive(Default)]
pub struct WeightedRoundRobinSelector {
    state: RwLock<HashMap<String, HashMap<String, isize>>>,
}

impl WeightedRoundRobinSelector {
    pub fn new() -> Self {
        Self {
            state: RwLock::new(HashMap::new()),
        }
    }
}

impl Selector for WeightedRoundRobinSelector {
    fn select<'a>(
        &self,
        ctx: &SelectContext,
        instances: &'a [ServiceInstance],
    ) -> Option<&'a ServiceInstance> {
        if instances.is_empty() {
            return None;
        }
        if instances.len() == 1 {
            return Some(&instances[0]);
        }

        let mut write = self.state.write().expect("weighted selector lock poisoned");
        let service_weights = write
            .entry(ctx.service_name.clone())
            .or_insert_with(HashMap::new);

        let mut total_weight: isize = 0;
        let mut best_idx: Option<usize> = None;
        let mut max_current_weight: isize = isize::MIN;

        for (i, inst) in instances.iter().enumerate() {
            let weight = (inst.weight.max(1.0) * 100.0) as isize;
            total_weight += weight;

            let cur = service_weights
                .entry(inst.service_id.clone())
                .or_insert(0);
            *cur += weight;

            if *cur > max_current_weight {
                max_current_weight = *cur;
                best_idx = Some(i);
            }
        }

        if let Some(idx) = best_idx {
            let chosen = &instances[idx];
            if let Some(cur) = service_weights.get_mut(&chosen.service_id) {
                *cur -= total_weight;
            }
            Some(chosen)
        } else {
            Some(&instances[0])
        }
    }
}
