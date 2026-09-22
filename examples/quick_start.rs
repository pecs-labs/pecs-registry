use pecs_registry::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    // 1. 开箱即用：创建基于 Local Provider 的 RegistryService 门面
    let registry = RegistryBuilder::local()
        .with_selector(std::sync::Arc::new(RoundRobinSelector::new()))
        .build()?;

    // 2. 注册当前微服务实例
    let instance = ServiceInstance::simple("user-service", "127.0.0.1", 8080)
        .with_metadata("env", "prod")
        .with_metadata("region", "ap-southeast-1");

    registry.register(&instance).await?;
    println!("✅ 注册服务成功: {}", instance.name);

    // 3. 服务发现与实例选择
    if let Some(target) = registry.select_instance("user-service").await? {
        println!("🎯 负载均衡选中目标节点: {} ({:?})", target.service_id, target.http);
    }

    // 4. 优雅下线
    registry.deregister().await?;
    println!("👋 优雅注销完成");

    Ok(())
}
