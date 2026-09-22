use pecs_registry::instance::{Endpoint, ServiceInstance};
use pecs_registry::selector::{P2CSelector, SelectContext, Selector};

fn make_instance_with_weight(
    service_name: &str,
    node_id: &str,
    port: u16,
    weight: f64,
) -> ServiceInstance {
    ServiceInstance::new_http(
        node_id,
        "test",
        "default",
        service_name,
        Endpoint::new("127.0.0.1", port),
    )
    .with_weight(weight)
}

#[test]
fn test_p2c_load_avoidance() {
    let selector = P2CSelector::new();
    let instances = vec![
        make_instance_with_weight("rpc-svc", "node-busy", 9001, 1.0),
        make_instance_with_weight("rpc-svc", "node-free", 9002, 1.0),
    ];

    let ctx = SelectContext::new("rpc-svc");

    // 模拟 node-busy 正在处理 10 个高并发在途请求
    let busy_counter = selector.get_inflight_counter("node-busy");
    busy_counter.store(10, std::sync::atomic::Ordering::Relaxed);

    // 模拟 node-free 空闲（0 个在途请求）
    let free_counter = selector.get_inflight_counter("node-free");
    free_counter.store(0, std::sync::atomic::Ordering::Relaxed);

    // 在 2 节点场景下，P2C 必须持续选择负载更低的 node-free
    for _ in 0..20 {
        let chosen = selector.select(&ctx, &instances).unwrap();
        assert_eq!(chosen.service_id, "node-free");
    }
}

#[test]
fn test_p2c_guard_raii_inflight_tracking() {
    let selector = P2CSelector::new();
    let instances = vec![
        make_instance_with_weight("api-svc", "api-1", 8001, 1.0),
        make_instance_with_weight("api-svc", "api-2", 8002, 1.0),
    ];

    let ctx = SelectContext::new("api-svc");

    let counter1 = selector.get_inflight_counter("api-1");
    let counter2 = selector.get_inflight_counter("api-2");
    assert_eq!(counter1.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(counter2.load(std::sync::atomic::Ordering::Relaxed), 0);

    // 1. 获取 Guard
    {
        let (inst, guard) = selector.select_with_guard(&ctx, &instances).unwrap();
        let target_id = inst.service_id.clone();

        if target_id == "api-1" {
            assert_eq!(counter1.load(std::sync::atomic::Ordering::Relaxed), 1);
        } else {
            assert_eq!(counter2.load(std::sync::atomic::Ordering::Relaxed), 1);
        }

        // drop guard 前保持计数
        drop(guard);
    }

    // 2. Guard 离开作用域后，计数必须自动归 0
    assert_eq!(counter1.load(std::sync::atomic::Ordering::Relaxed), 0);
    assert_eq!(counter2.load(std::sync::atomic::Ordering::Relaxed), 0);
}

#[test]
fn test_p2c_weight_sensitivity() {
    let selector = P2CSelector::new();
    // 两个节点在途请求数相同（0），但 node-strong 权重是 node-weak 的 3 倍
    let instances = vec![
        make_instance_with_weight("calc-svc", "node-weak", 8001, 1.0),
        make_instance_with_weight("calc-svc", "node-strong", 8002, 3.0),
    ];

    let ctx = SelectContext::new("calc-svc");

    // node-strong 的得分更低: (0 + 1) / 3.0 = 0.33 < (0 + 1) / 1.0 = 1.0
    for _ in 0..10 {
        let chosen = selector.select(&ctx, &instances).unwrap();
        assert_eq!(chosen.service_id, "node-strong");
    }
}
