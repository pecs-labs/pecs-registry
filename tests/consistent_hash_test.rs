use pecs_registry::instance::{Endpoint, ServiceInstance};
use pecs_registry::selector::{ConsistentHashSelector, SelectContext, Selector};
use std::collections::HashMap;

fn make_instance(service_name: &str, node_id: &str, port: u16) -> ServiceInstance {
    ServiceInstance::new_http(
        node_id,
        "test",
        "default",
        service_name,
        Endpoint::new("127.0.0.1", port),
    )
}

#[test]
fn test_consistent_hash_determinism_and_affinity() {
    let selector = ConsistentHashSelector::with_virtual_nodes(120);
    let instances = vec![
        make_instance("user-svc", "node-1", 8001),
        make_instance("user-svc", "node-2", 8002),
        make_instance("user-svc", "node-3", 8003),
    ];

    let ctx1 = SelectContext::new("user-svc").with_key("user:account:1001");
    let ctx2 = SelectContext::new("user-svc").with_key("user:account:1002");
    let ctx3 = SelectContext::new("user-svc").with_key("user:account:1003");

    // 1. 同一个 key 多次选择必须 100% 确定性命中同一个节点
    let target1_a = selector.select(&ctx1, &instances).unwrap();
    let target1_b = selector.select(&ctx1, &instances).unwrap();
    assert_eq!(target1_a.service_id, target1_b.service_id);

    let target2_a = selector.select(&ctx2, &instances).unwrap();
    let target2_b = selector.select(&ctx2, &instances).unwrap();
    assert_eq!(target2_a.service_id, target2_b.service_id);

    let target3_a = selector.select(&ctx3, &instances).unwrap();
    let target3_b = selector.select(&ctx3, &instances).unwrap();
    assert_eq!(target3_a.service_id, target3_b.service_id);
}

#[test]
fn test_consistent_hash_distribution() {
    let selector = ConsistentHashSelector::with_virtual_nodes(150);
    let instances = vec![
        make_instance("order-svc", "order-1", 8001),
        make_instance("order-svc", "order-2", 8002),
        make_instance("order-svc", "order-3", 8003),
    ];

    let mut hit_counts: HashMap<String, usize> = HashMap::new();

    // 测试 1000 个不同 key 的离散分布情况
    for i in 0..1000 {
        let key = format!("order_id_{i}");
        let ctx = SelectContext::new("order-svc").with_key(key);
        let chosen = selector.select(&ctx, &instances).unwrap();
        *hit_counts.entry(chosen.service_id.clone()).or_insert(0) += 1;
    }

    // 每一个节点都应该被合理命中，且不应发生极端倾斜（每个节点命中数 > 150）
    assert_eq!(hit_counts.len(), 3);
    for (node, count) in hit_counts {
        println!("节点 {node} 命中次数: {count}");
        assert!(count > 150, "节点命中数过低，哈希环分布不均: {count}");
    }
}

#[test]
fn test_consistent_hash_node_scaling_stability() {
    let selector = ConsistentHashSelector::with_virtual_nodes(100);
    let initial_nodes = vec![
        make_instance("cache-svc", "cache-1", 7001),
        make_instance("cache-svc", "cache-2", 7002),
        make_instance("cache-svc", "cache-3", 7003),
    ];

    let mut initial_mappings = HashMap::new();
    for i in 0..500 {
        let key = format!("cache_key_{i}");
        let ctx = SelectContext::new("cache-svc").with_key(&key);
        let chosen = selector.select(&ctx, &initial_nodes).unwrap();
        initial_mappings.insert(key, chosen.service_id.clone());
    }

    // 扩容增加第四个节点 cache-4
    let mut expanded_nodes = initial_nodes.clone();
    expanded_nodes.push(make_instance("cache-svc", "cache-4", 7004));

    let mut unchanged_keys = 0;
    for (key, old_node) in &initial_mappings {
        let ctx = SelectContext::new("cache-svc").with_key(key);
        let new_chosen = selector.select(&ctx, &expanded_nodes).unwrap();
        if new_chosen.service_id == *old_node {
            unchanged_keys += 1;
        }
    }

    // 一致性哈希理论：从 3 节点增加到 4 节点，保留率理论约为 75%
    let retention_rate = unchanged_keys as f64 / 500.0;
    println!("扩容节点保留率: {:.2}%", retention_rate * 100.0);
    assert!(
        retention_rate >= 0.60 && retention_rate <= 0.90,
        "一致性哈希重映射比例异常: {retention_rate}"
    );
}
