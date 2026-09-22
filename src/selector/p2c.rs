use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use super::{SelectContext, Selector};
use crate::instance::ServiceInstance;

/// P2C 活跃请求生命周期守卫（RAII）
///
/// 当请求完成离开作用域时自动将目标节点的并发活跃计数减 1
pub struct P2CGuard {
    counter: Arc<AtomicUsize>,
}

impl Drop for P2CGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

/// P2C（Power of Two Choices）二选一动态负载均衡器
///
/// 算法思想（被 Finagle、Envoy、gRPC 等顶级工业级 RPC 框架广泛采纳）：
/// 1. 每次路由请求随机挑选两个不同的候选节点（`Node A` 与 `Node B`）；
/// 2. 评估两者的综合负载得分：`load_score = (active_inflight + 1) / weight`；
/// 3. 选择负载得分更低（更空闲或性能更好）的节点投递请求；
/// 4. 彻底解决纯轮询/随机在长耗时请求下的节点堆积颠簸（Herd Effect）与羊群效应。
pub struct P2CSelector {
    inflight: RwLock<HashMap<String, Arc<AtomicUsize>>>,
    seed_counter: AtomicUsize,
}

impl P2CSelector {
    pub fn new() -> Self {
        Self {
            inflight: RwLock::new(HashMap::new()),
            seed_counter: AtomicUsize::new(0),
        }
    }

    /// 获取或创建指定实例的 inflight 计数器
    pub fn get_inflight_counter(&self, service_id: &str) -> Arc<AtomicUsize> {
        {
            let read = self.inflight.read().expect("p2c lock poisoned");
            if let Some(counter) = read.get(service_id) {
                return counter.clone();
            }
        }

        let mut write = self.inflight.write().expect("p2c lock poisoned");
        write
            .entry(service_id.to_string())
            .or_insert_with(|| Arc::new(AtomicUsize::new(0)))
            .clone()
    }

    /// 手动占位目标实例并获取生命周期 Guard
    pub fn acquire(&self, service_id: &str) -> P2CGuard {
        let counter = self.get_inflight_counter(service_id);
        counter.fetch_add(1, Ordering::Relaxed);
        P2CGuard { counter }
    }

    /// 挑选节点并直接返回关联的并发计数 Guard（推荐在客户端中间件中使用）
    pub fn select_with_guard<'a>(
        &self,
        ctx: &SelectContext,
        instances: &'a [ServiceInstance],
    ) -> Option<(&'a ServiceInstance, P2CGuard)> {
        let inst = self.select(ctx, instances)?;
        let guard = self.acquire(&inst.service_id);
        Some((inst, guard))
    }

    /// 获取两节点的不重复随机索引
    fn pick_two(&self, len: usize) -> (usize, usize) {
        let now_nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos() as usize)
            .unwrap_or(0);
        let seq = self.seed_counter.fetch_add(1, Ordering::Relaxed);
        let seed = now_nanos.wrapping_add(seq);

        let idx1 = seed % len;
        let offset = 1 + (seed / len) % (len - 1);
        let idx2 = (idx1 + offset) % len;
        (idx1, idx2)
    }

    /// 计算节点的负载综合评分（得分越低代表越优）
    fn compute_load_score(&self, inst: &ServiceInstance) -> f64 {
        let inflight = {
            let read = self.inflight.read().expect("p2c lock poisoned");
            read.get(&inst.service_id)
                .map(|c| c.load(Ordering::Relaxed))
                .unwrap_or(0)
        };
        let weight = inst.weight.max(0.1);
        (inflight as f64 + 1.0) / (weight as f64)
    }
}

impl Default for P2CSelector {
    fn default() -> Self {
        Self::new()
    }
}

impl Selector for P2CSelector {
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
        if len == 2 {
            let score0 = self.compute_load_score(&instances[0]);
            let score1 = self.compute_load_score(&instances[1]);
            return if score0 <= score1 {
                Some(&instances[0])
            } else {
                Some(&instances[1])
            };
        }

        // P2C 二选一比较
        let (idx1, idx2) = self.pick_two(len);
        let inst1 = &instances[idx1];
        let inst2 = &instances[idx2];

        let score1 = self.compute_load_score(inst1);
        let score2 = self.compute_load_score(inst2);

        if score1 <= score2 {
            Some(inst1)
        } else {
            Some(inst2)
        }
    }
}
