#![cfg(feature = "etcd")]
use pecs_registry::*;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn connection() -> RegistryConfig {
    RegistryConfig {
        host: std::env::var("ETCD_TEST_ENDPOINT").expect("set ETCD_TEST_ENDPOINT"),
        ttl: 3,
        timeout: 2,
        ..Default::default()
    }
}

fn prefix() -> String {
    format!(
        "/pecs-test/{}",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )
}

#[tokio::test]
#[ignore = "requires isolated etcd; set ETCD_TEST_ENDPOINT"]
async fn registration_confirmed_renewed_and_deregistered() {
    use tokio_stream::StreamExt;
    let config = RegistryConfig {
        namespace: prefix().trim_matches('/').into(),
        ..connection()
    };
    let registry = EtcdRegistry::new(config);
    let instance = ServiceInstance::simple("test-service", "127.0.0.1", 19000);
    registry.register(&instance).await.unwrap();
    assert!(registry.is_registered());
    assert_eq!(
        registry.list_instances(&instance.name).await.unwrap().len(),
        1
    );
    let mut stream = registry.watch(&instance.name).await.unwrap();
    assert!(matches!(stream.next().await, Some(ServiceEvent::Reset(list)) if list.len() == 1));
    tokio::time::sleep(Duration::from_secs(7)).await;
    assert_eq!(
        registry.list_instances(&instance.name).await.unwrap().len(),
        1
    );
    registry.deregister().await.unwrap();
    assert!(!registry.is_registered());
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(registry
        .list_instances(&instance.name)
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap(),
        Some(ServiceEvent::Delete { .. })
    ));
}
