use pecs_registry::RegistryConfig;
#[test]
fn endpoints_accept_single_urls_and_clusters() {
    for (host, expected) in [
        ("localhost", vec!["http://localhost:2379"]),
        ("https://etcd:1234", vec!["https://etcd:1234"]),
        ("etcd:1234", vec!["http://etcd:1234"]),
        ("[::1]", vec!["http://[::1]:2379"]),
        (
            " a , https://b:1234 ",
            vec!["http://a:2379", "https://b:1234"],
        ),
    ] {
        let config = RegistryConfig {
            host: host.into(),
            ..Default::default()
        };
        assert_eq!(config.endpoints(), expected);
        assert_eq!(config.endpoint(), expected[0]);
    }
}
#[cfg(feature = "etcd")]
#[tokio::test]
async fn disabled_lock_fails_without_local_fallback() {
    let locks =
        pecs_registry::lock::LockService::new(Default::default(), Default::default()).unwrap();
    assert!(locks.acquire("order/42").await.is_err());
}

#[cfg(feature = "etcd")]
#[tokio::test]
async fn zero_wait_budget_is_rejected_before_connection() {
    let locks =
        pecs_registry::lock::LockService::new(Default::default(), Default::default()).unwrap();
    assert!(matches!(
        locks
            .acquire_with_timeout("order/42", std::time::Duration::ZERO)
            .await,
        Err(pecs_registry::RegistryError::Config(_))
    ));
}
