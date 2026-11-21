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

#[cfg(feature = "etcd")]
#[tokio::test]
async fn claims_reject_disabled_service_invalid_resources_and_retention() {
    use pecs_registry::{
        lock::{LockConfig, LockService},
        RegistryError,
    };
    let disabled = LockService::new(Default::default(), Default::default()).unwrap();
    assert!(matches!(
        disabled.claim_once("runs/1", 60).await,
        Err(RegistryError::Config(_))
    ));
    let enabled = LockService::new(
        Default::default(),
        LockConfig {
            enabled: true,
            ..Default::default()
        },
    )
    .unwrap();
    for resource in ["", "a//b", "../a", "a/./b"] {
        assert!(matches!(
            enabled.claim_once(resource, 60).await,
            Err(RegistryError::Config(_))
        ));
    }
    assert!(matches!(
        enabled.claim_once("runs/1", 0).await,
        Err(RegistryError::Config(_))
    ));
}

#[test]
fn has_auth_credentials_requires_non_empty_user_and_password() {
    let mut cfg = RegistryConfig::default();
    assert!(!cfg.has_auth_credentials());

    cfg.user = Some("root".into());
    cfg.password = Some(String::new());
    assert!(!cfg.has_auth_credentials());

    cfg.user = Some("   ".into());
    cfg.password = Some("secret".into());
    assert!(!cfg.has_auth_credentials());

    cfg.user = Some("root".into());
    cfg.password = Some("secret".into());
    assert!(cfg.has_auth_credentials());
}
