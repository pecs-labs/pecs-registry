use pecs_registry::prelude::*;

fn mock_instance(id: &str, weight: f64) -> ServiceInstance {
    ServiceInstance::new_http(
        id,
        "default",
        "dev",
        "demo-service",
        Endpoint::new("127.0.0.1", 8080),
    )
    .with_weight(weight)
}

#[test]
fn test_round_robin_selector() {
    let selector = RoundRobinSelector::new();
    let ctx = SelectContext::new("demo-service");
    let instances = vec![
        mock_instance("node-1", 1.0),
        mock_instance("node-2", 1.0),
        mock_instance("node-3", 1.0),
    ];

    let r1 = selector.select(&ctx, &instances).unwrap();
    let r2 = selector.select(&ctx, &instances).unwrap();
    let r3 = selector.select(&ctx, &instances).unwrap();
    let r4 = selector.select(&ctx, &instances).unwrap();

    assert_eq!(r1.service_id, "node-1");
    assert_eq!(r2.service_id, "node-2");
    assert_eq!(r3.service_id, "node-3");
    assert_eq!(r4.service_id, "node-1");
}

#[test]
fn test_weighted_round_robin_selector() {
    let selector = WeightedRoundRobinSelector::new();
    let ctx = SelectContext::new("demo-service");
    let instances = vec![
        mock_instance("node-A", 4.0),
        mock_instance("node-B", 2.0),
        mock_instance("node-C", 1.0),
    ];

    let mut counts = std::collections::HashMap::new();
    // 7 次选择应分别对应 4, 2, 1
    for _ in 0..7 {
        let chosen = selector.select(&ctx, &instances).unwrap();
        *counts.entry(chosen.service_id.clone()).or_insert(0) += 1;
    }

    assert_eq!(counts.get("node-A"), Some(&4));
    assert_eq!(counts.get("node-B"), Some(&2));
    assert_eq!(counts.get("node-C"), Some(&1));
}

#[test]
fn test_canary_tag_filter_selector() {
    let base_selector = RoundRobinSelector::new();
    let canary_selector = TagFilterSelector::new(base_selector);

    let mut inst1 = mock_instance("node-v1", 1.0);
    inst1.metadata.insert("version".to_string(), "v1".to_string());

    let mut inst2 = mock_instance("node-v2", 1.0);
    inst2.metadata.insert("version".to_string(), "v2".to_string());

    let instances = vec![inst1, inst2];

    // 1. 指定 version=v2 标签选择
    let ctx_v2 = SelectContext::new("demo-service").with_tag("version", "v2");
    let chosen_v2 = canary_selector.select(&ctx_v2, &instances).unwrap();
    assert_eq!(chosen_v2.service_id, "node-v2");

    // 2. 指定不存在的标签，默认回退
    let ctx_none = SelectContext::new("demo-service").with_tag("version", "v3");
    let chosen_fallback = canary_selector.select(&ctx_none, &instances).unwrap();
    assert!(chosen_fallback.service_id == "node-v1" || chosen_fallback.service_id == "node-v2");
}
