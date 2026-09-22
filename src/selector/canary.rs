use super::{SelectContext, Selector};
use crate::instance::ServiceInstance;

/// 元数据与灰度/金丝雀标签过滤选择器（Canary / Tag Router）
///
/// 采用装饰器模式包裹底层选择器（如 RoundRobin 或 Weighted），
/// 优先在匹配标签的子集中挑选；若无标签要求或策略匹配无结果，可平滑回退至全局实例池。
pub struct TagFilterSelector<S: Selector> {
    inner: S,
    fallback_on_miss: bool,
}

impl<S: Selector> TagFilterSelector<S> {
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            fallback_on_miss: true,
        }
    }

    /// 设置当标签无匹配节点时，是否平滑回退至全部候选节点（默认为 true）
    pub fn with_fallback(mut self, fallback: bool) -> Self {
        self.fallback_on_miss = fallback;
        self
    }
}

impl<S: Selector> Selector for TagFilterSelector<S> {
    fn select<'a>(
        &self,
        ctx: &SelectContext,
        instances: &'a [ServiceInstance],
    ) -> Option<&'a ServiceInstance> {
        if let Some((ref tag_k, ref tag_v)) = ctx.required_tag {
            let filtered: Vec<ServiceInstance> = instances
                .iter()
                .filter(|inst| {
                    // 1. 优先比对 metadata 中的键值
                    if let Some(val) = inst.metadata.get(tag_k) {
                        if val == tag_v {
                            return true;
                        }
                    }
                    // 2. 兼容 group 与 namespace 维度的比对
                    if inst.group == *tag_v || inst.namespace == *tag_v {
                        return true;
                    }
                    // 3. 包含关系判断
                    inst.service_id.contains(tag_v)
                })
                .cloned()
                .collect();

            if !filtered.is_empty() {
                if let Some(target) = self.inner.select(ctx, &filtered) {
                    let target_id = target.service_id.clone();
                    return instances.iter().find(|i| i.service_id == target_id);
                }
            } else if !self.fallback_on_miss {
                return None;
            }
        }

        self.inner.select(ctx, instances)
    }
}
