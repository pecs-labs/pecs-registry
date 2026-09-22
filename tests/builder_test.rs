use pecs_registry::prelude::*;
use std::time::Duration;

#[tokio::test]
async fn test_builder_local_and_config() {
    // 1. 测试 Local Builder
    let service = RegistryBuilder::local()
        .with_reconcile_interval(Duration::from_secs(10))
        .with_selector(std::sync::Arc::new(WeightedRoundRobinSelector::new()))
        .build()
        .unwrap();

    let inst = ServiceInstance::simple("test-svc", "127.0.0.1", 8080);
    service.register(&inst).await.unwrap();

    let picked = service.select_instance("test-svc").await.unwrap().unwrap();
    assert_eq!(picked.name, "test-svc");

    // 2. 测试从 RegistryConfig 装配
    let mut config = RegistryConfig::default();
    config.kind = "local".to_string();

    let cfg_service = RegistryBuilder::from_config(&config).build().unwrap();
    cfg_service.register(&inst).await.unwrap();
    let res = cfg_service.discover("test-svc").await.unwrap();
    assert!(!res.is_empty());
}
