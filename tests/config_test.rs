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
