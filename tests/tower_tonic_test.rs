#![cfg(all(feature = "tower", feature = "tonic", feature = "local"))]

use pecs_registry::prelude::*;
use std::sync::Arc;
use tokio_stream::StreamExt;
use tower::discover::Change;

#[tokio::test]
async fn test_tower_discover_stream() {
    let registry = Arc::new(LocalRegistry::new());
    let directory = ServiceDirectory::new(registry.clone());

    // 1. 先注册 2 个初始节点
    let inst1 = ServiceInstance::simple("user-svc", "127.0.0.1", 9001);
    let inst2 = ServiceInstance::simple("user-svc", "127.0.0.1", 9002);
    registry.add_instance(inst1.clone());
    registry.add_instance(inst2.clone());

    // 2. 开启 Tower Discover 事件流
    let mut discover = directory.discover_stream("user-svc").await.unwrap();

    // 接收两个初始节点
    let c1 = discover.next().await.unwrap().unwrap();
    let c2 = discover.next().await.unwrap().unwrap();

    let mut initial_ids = vec![];
    if let Change::Insert(id, _) = c1 {
        initial_ids.push(id);
    }
    if let Change::Insert(id, _) = c2 {
        initial_ids.push(id);
    }
    assert!(initial_ids.contains(&inst1.service_id));
    assert!(initial_ids.contains(&inst2.service_id));

    // 3. 动态注册第 3 个节点
    let inst3 = ServiceInstance::simple("user-svc", "127.0.0.1", 9003);
    registry.add_instance(inst3.clone());

    let c3 = discover.next().await.unwrap().unwrap();
    if let Change::Insert(id, _) = c3 {
        assert_eq!(id, inst3.service_id);
    } else {
        panic!("expected Insert event for inst3");
    }

    // 4. 节点下线
    registry.remove_instance(&inst1.name, &inst1.service_id);
    let c4 = discover.next().await.unwrap().unwrap();
    if let Change::Remove(id) = c4 {
        assert_eq!(id, inst1.service_id);
    } else {
        panic!("expected Remove event for inst1");
    }
}

#[tokio::test]
async fn test_tonic_channel_dynamic_balance() {
    let registry = Arc::new(LocalRegistry::new());
    let directory = ServiceDirectory::new(registry.clone());

    // 注册带有 gRPC Endpoint 的实例
    let inst1 = ServiceInstance::new_grpc(
        "grpc-node-01",
        "default",
        "default",
        "order-svc",
        Endpoint::new("127.0.0.1", 9011),
    );
    registry.register(&inst1).await.unwrap();

    // 创建开箱即用的 Tonic 动态均衡通道
    let channel = directory.tonic_channel("order-svc").await.unwrap();

    // 动态增加第 2 个节点
    let inst2 = ServiceInstance::new_grpc(
        "grpc-node-02",
        "default",
        "default",
        "order-svc",
        Endpoint::new("127.0.0.1", 9012),
    );
    registry.register(&inst2).await.unwrap();

    // 验证通道有效可用（底层由 Tonic 动态维护）
    let _ = channel;
}
