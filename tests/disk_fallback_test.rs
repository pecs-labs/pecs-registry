use async_trait::async_trait;
use pecs_registry::directory::ServiceDirectory;
use pecs_registry::error::{RegistryError, RegistryResult};
use pecs_registry::instance::{Endpoint, ServiceInstance};
use pecs_registry::provider::local::LocalRegistry;
use pecs_registry::traits::{EventStream, Registry};
use std::sync::Arc;
use std::time::Duration;

fn make_instance(service_name: &str, node_id: &str, port: u16) -> ServiceInstance {
    ServiceInstance::new_http(
        node_id,
        "test",
        "default",
        service_name,
        Endpoint::new("127.0.0.1", port),
    )
}

/// 模拟发生网络隔离或完全宕机的注册中心
struct BrokenRegistry;

#[async_trait]
impl Registry for BrokenRegistry {
    async fn register(&self, _instance: &ServiceInstance) -> RegistryResult<()> {
        Err(RegistryError::Connection("network unreachable".to_string()))
    }

    async fn deregister(&self) -> RegistryResult<()> {
        Err(RegistryError::Connection("network unreachable".to_string()))
    }

    async fn list_instances(&self, _service_name: &str) -> RegistryResult<Vec<ServiceInstance>> {
        Err(RegistryError::Connection("cluster completely down".to_string()))
    }

    async fn watch(&self, _service_name: &str) -> RegistryResult<EventStream> {
        Err(RegistryError::Connection("watch stream broken".to_string()))
    }
}

#[tokio::test]
async fn test_disk_snapshot_write_and_recovery() {
    let temp_dir = std::env::temp_dir().join(format!("pecs_reg_test_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&temp_dir);

    let local = Arc::new(LocalRegistry::new());
    let inst1 = make_instance("payment-svc", "pay-node-01", 8081);
    let inst2 = make_instance("payment-svc", "pay-node-02", 8082);
    local.register(&inst1).await.unwrap();
    local.register(&inst2).await.unwrap();

    // 1. 创建具备本地磁盘快照支持的目录
    let directory = ServiceDirectory::with_options(
        local.clone(),
        Duration::from_secs(60),
        Some(temp_dir.clone()),
    );

    let loaded = directory.load_or_watch("payment-svc").await.unwrap();
    assert_eq!(loaded.len(), 2);

    // 等待异步持久化完成
    tokio::time::sleep(Duration::from_millis(50)).await;

    let snapshot_file = temp_dir.join("payment-svc.json");
    assert!(snapshot_file.exists(), "快照文件必须成功生成在磁盘中");

    // 2. 模拟注册中心宕机，新启动的节点在冷启动时从磁盘快照容灾恢复
    let broken = Arc::new(BrokenRegistry);
    let recovery_directory = ServiceDirectory::with_options(
        broken,
        Duration::from_secs(60),
        Some(temp_dir.clone()),
    );

    // 此时虽然远端 BrokenRegistry 彻底抛出 Connection 错误，但目录能无损降级恢复
    let recovered = recovery_directory.load_or_watch("payment-svc").await.unwrap();
    assert_eq!(recovered.len(), 2);
    let ids: Vec<&str> = recovered.iter().map(|i| i.service_id.as_str()).collect();
    assert!(ids.contains(&"pay-node-01"));
    assert!(ids.contains(&"pay-node-02"));

    // 清理临时测试目录
    let _ = std::fs::remove_dir_all(&temp_dir);
}
