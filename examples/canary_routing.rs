use pecs_registry::prelude::*;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // 1. 装配带金丝雀/灰度标签过滤的负载均衡选择器
    let canary_selector = Arc::new(TagFilterSelector::new(RoundRobinSelector::new()));
    let registry = RegistryBuilder::local()
        .with_selector(canary_selector)
        .build()?;

    // 2. 模拟注册稳定版与灰度版节点
    let v1 = ServiceInstance::simple("order-service", "192.168.1.10", 8080)
        .with_metadata("version", "v1.0.0");
    let v2 = ServiceInstance::simple("order-service", "192.168.1.20", 8080)
        .with_metadata("version", "v2.0.0");

    registry.register(&v1).await?;
    registry.register(&v2).await?;

    // 3. 普通流量请求（命中全局实例）
    let normal_ctx = SelectContext::new("order-service");
    let normal_target = registry.select_instance_with_context(&normal_ctx).await?.unwrap();
    println!("普通流量路由节点: {}", normal_target.service_id);

    // 4. 灰度流量请求（带 HTTP Header 或 Query 提取的 tag）
    let canary_ctx = SelectContext::new("order-service").with_tag("version", "v2.0.0");
    let canary_target = registry.select_instance_with_context(&canary_ctx).await?.unwrap();
    println!("灰度流量精准命中节点: {} (version={:?})", canary_target.service_id, canary_target.metadata.get("version"));

    Ok(())
}
