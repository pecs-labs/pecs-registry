#![cfg(feature = "etcd")]
use pecs_registry::{
    lock::{LockConfig, LockService},
    *,
};
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
fn service(prefix: String) -> LockService {
    LockService::new(
        connection(),
        LockConfig {
            enabled: true,
            prefix,
            lease_ttl_secs: 3,
            acquire_timeout_ms: 400,
            rpc_timeout_ms: 500,
        },
    )
    .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated etcd; set ETCD_TEST_ENDPOINT"]
async fn lock_contention_renewal_drop_cancel_and_fencing() {
    tokio::time::timeout(Duration::from_secs(25), async {
        let prefix = prefix();
        let locks = service(prefix.clone());
        let first = locks.acquire("order/42").await.unwrap();
        let token = first.fencing_token();
        assert!(service(prefix.clone()).acquire("order/42").await.is_err());
        assert!(matches!(
            locks
                .acquire_with_timeout("order/42", Duration::from_millis(25))
                .await,
            Err(RegistryError::Timeout(_))
        ));
        assert!(locks.acquire("order/42").await.is_err());
        // Keep ownership across multiple original TTL periods.
        tokio::time::sleep(Duration::from_secs(7)).await;
        assert!(!first.is_lost());
        assert!(locks.acquire("order/42").await.is_err());
        let pending = tokio::spawn({
            let locks = locks.clone();
            async move { locks.acquire("order/42").await }
        });
        tokio::time::sleep(Duration::from_millis(100)).await;
        pending.abort();
        let _ = pending.await;
        first.release().await.unwrap();
        let second = locks.acquire("order/42").await.unwrap();
        assert!(second.fencing_token() > token);
        drop(second);
        tokio::time::sleep(Duration::from_millis(200)).await;
        let third = locks.acquire("order/42").await.unwrap();
        third.release().await.unwrap();
        let mut client = etcd_client::Client::connect(connection().endpoints(), None)
            .await
            .unwrap();
        let keys = client
            .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
            .await
            .unwrap();
        assert!(
            keys.kvs().is_empty(),
            "cancelled waiter or guard leaked a key"
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires isolated etcd; set ETCD_TEST_ENDPOINT"]
async fn revoked_lock_is_lost_and_never_reacquired() {
    let prefix = prefix();
    let locks = service(prefix.clone());
    let guard = locks.acquire("resource").await.unwrap();
    let mut client = etcd_client::Client::connect(connection().endpoints(), None)
        .await
        .unwrap();
    let response = client
        .get(prefix, Some(etcd_client::GetOptions::new().with_prefix()))
        .await
        .unwrap();
    client
        .lease_revoke(response.kvs()[0].lease())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(4), guard.lost())
        .await
        .unwrap();
    let next = locks.acquire("resource").await.unwrap();
    assert!(next.fencing_token() > guard.fencing_token());
    drop(guard);
    next.release().await.unwrap();
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

#[tokio::test]
#[ignore = "requires isolated etcd; set ETCD_TEST_ENDPOINT"]
async fn occurrence_claim_is_atomic_independent_of_lock_and_expires() {
    let prefix = prefix();
    let first = service(prefix.clone());
    let second = service(prefix);
    let (a, b) = tokio::join!(
        first.claim_once("runs/task/1", 3),
        second.claim_once("runs/task/1", 3),
    );
    assert_ne!(a.unwrap(), b.unwrap(), "exactly one contender must claim");
    let guard = first.acquire("jobs/task").await.unwrap();
    guard.release().await.unwrap();
    assert!(!second.claim_once("runs/task/1", 3).await.unwrap());
    assert!(second.claim_once("runs/task/2", 3).await.unwrap());
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if first.claim_once("runs/task/1", 3).await.unwrap() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "claim did not expire"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}
