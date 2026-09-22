use pecs_registry::prelude::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::sleep;

#[tokio::test]
async fn test_local_registry_crud_and_directory_watch() {
    let local = Arc::new(LocalRegistry::new());
    let service = Arc::new(RegistryService::new(local.clone()));

    // 1. 初始发现应为空
    let instances = service.discover("user-service").await.unwrap();
    assert!(instances.is_empty());

    // 2. 注册服务实例 1
    let inst1 = ServiceInstance::new_http(
        "inst-01",
        "default",
        "dev",
        "user-service",
        Endpoint::new("127.0.0.1", 8001),
    );
    service.register(&inst1).await.unwrap();

    // 等待事件通过 Watch 广播更新到 Directory 快照
    sleep(Duration::from_millis(50)).await;

    // 3. 校验服务发现与快照
    let discovered = service.discover("user-service").await.unwrap();
    assert_eq!(discovered.len(), 1);
    assert_eq!(discovered[0].service_id, "inst-01");
    assert_eq!(discovered[0].http.as_ref().unwrap().port, 8001);

    // 4. 注册服务实例 2
    let inst2 = ServiceInstance::new_http(
        "inst-02",
        "default",
        "dev",
        "user-service",
        Endpoint::new("127.0.0.1", 8002),
    );
    local.add_instance(inst2);

    sleep(Duration::from_millis(50)).await;

    let discovered2 = service.discover("user-service").await.unwrap();
    assert_eq!(discovered2.len(), 2);

    // 5. 负载均衡选择（Round-Robin 轮询）
    let sel1 = service.select_instance("user-service").await.unwrap().unwrap();
    let sel2 = service.select_instance("user-service").await.unwrap().unwrap();
    let sel3 = service.select_instance("user-service").await.unwrap().unwrap();
    assert_ne!(sel1.service_id, sel2.service_id);
    assert_eq!(sel1.service_id, sel3.service_id);

    // 6. 下线实例 1
    local.remove_instance("user-service", "inst-01");
    sleep(Duration::from_millis(50)).await;

    let discovered3 = service.discover("user-service").await.unwrap();
    assert_eq!(discovered3.len(), 1);
    assert_eq!(discovered3[0].service_id, "inst-02");

    // 7. 注销服务
    service.deregister().await.unwrap();
}

struct TestListener(Arc<std::sync::atomic::AtomicUsize>);

impl InstanceListener for TestListener {
    fn on_change(&self, _service_name: &str, instances: &[ServiceInstance]) {
        self.0.fetch_add(instances.len(), std::sync::atomic::Ordering::SeqCst);
    }
}

#[tokio::test]
async fn test_instance_listener_notification() {
    let local = Arc::new(LocalRegistry::new());
    let service = Arc::new(RegistryService::new(local.clone()));

    let notify_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    service.directory().add_listener(Arc::new(TestListener(notify_count.clone())));

    // 预热 watch
    let _ = service.discover("order-service").await.unwrap();

    let inst = ServiceInstance::simple("order-service", "127.0.0.1", 9000);
    service.register(&inst).await.unwrap();

    sleep(Duration::from_millis(50)).await;
    assert!(notify_count.load(std::sync::atomic::Ordering::SeqCst) > 0);
}
