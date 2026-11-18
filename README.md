# pecs-registry

[![Crates.io](https://img.shields.io/badge/crates.io-pecs--registry-orange.svg)](https://crates.io/)
[![Documentation](https://docs.rs/pecs-registry/badge.svg)](https://docs.rs/pecs-registry)
[![License](https://img.shields.io/badge/license-MIT%2FApache--2.0-blue.svg)](#license)

**pecs-registry** 是一个面向云原生与微服务架构的**高性能、高可用、可扩展的统一服务注册与发现框架**。

采用工业级**三层解耦架构**：
1. **SPI Provider 抽象层**（统一契约，支持 etcd、Nacos、Local、Consul、Kubernetes 等）；
2. **事件驱动无锁快照目录层**（`< 20ns` 纯内存无锁读取，长连接 Watch 增量推送，周期对齐自愈）；
3. **独立负载均衡与路由策略层**（无锁原子轮询、平滑加权轮询、灰度金丝雀标签路由）。

---

## 🌟 核心特性

- 🚀 **极速无锁 RCU 快照读取**：基于 `arc-swap` 实现读写无锁分离，日常读操作延迟 `< 5ns`，零 CPU Cacheline 颠簸与协程阻塞。
- 📡 **事件驱动推送**：基于底层 Watch 前缀事件流长连接，毫秒级感知节点上下线；断线指数退避重连。
- 🛡️ **生产级自愈保活**：etcd 租约自动续签与自愈重连，双重静默周期校准兜底，彻底杜绝数据漂移与雪崩。
- 💾 **本地磁盘快照容灾降级（Disaster Recovery）**：支持异步将服务拓扑持久化至磁盘，即使注册中心集群全部宕机或突发断网，服务冷启动与调用仍能从本地磁盘快照无缝降级恢复。
- ⚡ **RPC / gRPC 原生支持**：
  - **Tower 生态集成**：原生实现 `tower::discover::Discover` 规范，无缝对接各类基于 Tower 的微服务中间件。
  - **Tonic 开箱即用**：提供 `registry.tonic_channel("service")`，自动转换为 Tonic 动态负载均衡连接池（`balance_channel`）。
- 🔌 **开箱即用多 Provider**：
  - `etcd`：生产级高可用集群注册中心（基于 `etcd-client`）。
  - `local`：本地直连 Provider，免外部中间件，极速单测与本地开发。
  - `nacos`：主流微服务治理协议接入。
- ⚖️ **丰富的工业级负载均衡算法**：
  - `P2CSelector`（Power of Two Choices）：结合活跃并发数（Inflight）与权重的最优二选一，有效规避慢节点羊群效应。
  - `ConsistentHashSelector`：带虚拟节点技术与拓扑指纹缓存的高性能一致性哈希，保障会话粘滞与分片亲和性。
  - `RoundRobinSelector`：基于微服务独立的原子计数器，零锁争用。
  - `WeightedRoundRobinSelector`：Nginx 经典平滑加权算法。
  - `RandomSelector`：高效随机。
  - `TagFilterSelector`：支持环境、版本、金丝雀/灰度流量染色过滤。
- 🧩 **高可扩展性**：支持自定义 SPI Provider，支持注册实例变更监听器（`InstanceListener`），支持动态 `unwatch` 资源释放。

---

## 📦 安装与配置

在 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
# 默认启用 local 与 etcd
pecs-registry = { version = "0.1.0", features = ["etcd", "local"] }
```

### Feature Flags

| Feature | 描述 | 默认开启 |
| :--- | :--- | :---: |
| `local` | 内置本地直连提供者（单测与本地极速开发） | ✅ |
| `etcd` | 生产级 etcd v3 驱动（支持租约保活与 Watch 事件流） | ✅ |
| `nacos` | Nacos HTTP OpenAPI 驱动 | ❌ |
| `tower` | Tower 服务发现 `Discover` 规范支持与流式变更监听 | ❌ |
| `tonic` | Tonic gRPC 原生无缝集成与动态负载均衡通道 (`balance_channel`) | ❌ |

---

## 🚀 快速上手 (Quick Start)

### 1. 极速单测与本地开发 (Local Provider)

无需启动任何外部中间件，直接享受全套服务发现与负载均衡能力：

```rust
use pecs_registry::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1. 链式构建门面
    let registry = RegistryBuilder::local()
        .with_selector(std::sync::Arc::new(RoundRobinSelector::new()))
        .build()?;

    // 2. 注册服务实例
    let instance = ServiceInstance::simple("user-service", "127.0.0.1", 8080)
        .with_metadata("env", "prod");
    registry.register(&instance).await?;

    // 3. 负载均衡选择实例
    if let Some(target) = registry.select_instance("user-service").await? {
        println!("选中的实例地址: {:?}", target.http);
    }

    // 4. 优雅下线
    registry.deregister().await?;
    Ok(())
}
```

### 2. 基于配置自动装配 (Production etcd)

```rust
use pecs_registry::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut config = RegistryConfig::default();
    config.kind = "etcd".to_string();
    config.host = "10.0.0.1:2379,10.0.0.2:2379".to_string();
    config.namespace = "my-cluster".to_string();
    config.group = "prod".to_string();

    let registry = RegistryBuilder::from_config(&config).build()?;

    let instance = ServiceInstance::new_http(
        "order-node-01",
        &config.namespace,
        &config.group,
        "order-service",
        Endpoint::new("10.0.0.10", 8001),
    );

    registry.register(&instance).await?;
    Ok(())
}
```

---

## 🧭 金丝雀/灰度路由 (Canary Tag Routing)

利用 `TagFilterSelector`，可基于请求头中的版本号或灰度标签进行流量染色精准分流：

```rust
use pecs_registry::prelude::*;
use std::sync::Arc;

let canary = Arc::new(TagFilterSelector::new(RoundRobinSelector::new()));
let registry = RegistryBuilder::local()
    .with_selector(canary)
    .build()?;

// 灰度流量路由 (匹配 metadata["version"] == "v2")
let ctx = SelectContext::new("user-service").with_tag("version", "v2");
let chosen = registry.select_instance_with_context(&ctx).await?;
```

---

## 🎯 一致性哈希分片路由 (Consistent Hashing)

适用于长连接有状态服务、会话粘滞（Sticky Session）及分布式缓存分片：

```rust
use pecs_registry::prelude::*;
use std::sync::Arc;

let selector = Arc::new(ConsistentHashSelector::with_virtual_nodes(150));
let registry = RegistryBuilder::local()
    .with_selector(selector)
    .build()?;

// 基于用户 ID 或订单 ID 等 Sharding Key 进行确定性路由
let ctx = SelectContext::new("chat-service").with_key("user_session_100234");
let chosen = registry.select_instance_with_context(&ctx).await?;
```

---

## ⚖️ P2C 动态负载均衡 (Power of Two Choices)

Envoy、Finagle 工业级标准：随机挑两节点，评估活跃在途请求数与节点权重，自动避开慢节点：

```rust
use pecs_registry::prelude::*;
use std::sync::Arc;

let p2c = Arc::new(P2CSelector::new());
let registry = RegistryBuilder::local()
    .with_selector(p2c.clone())
    .build()?;

// 配合生命周期 Guard 追踪在途并发（请求结束离开作用域自动归还）
let instances = registry.get_instances("user-service");
let ctx = SelectContext::new("user-service");
if let Some((inst, _guard)) = p2c.select_with_guard(&ctx, &instances) {
    // 处理 RPC / HTTP 请求...
}
```

---

## 🛡️ 本地磁盘快照容灾降级 (Disaster Recovery)

即使注册中心 etcd/Nacos 集群全面瘫痪或网络隔离，微服务在冷启动与运行期仍能无损从本地磁盘快照恢复拓扑：

```rust
use pecs_registry::prelude::*;

let registry = RegistryBuilder::local()
    .with_disk_cache_dir("./.registry_cache") // 开启本地磁盘快照持久化
    .build()?;
```

---

## ⚡ gRPC / Tonic 原生动态负载均衡

启用 `features = ["tonic"]` 后，可直接将服务发现与 Tonic 的 `balance_channel` 连接池融合：

```rust
use pecs_registry::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let registry = RegistryBuilder::local().build()?;

    // 1. 获取动态负载均衡的 Tonic gRPC Channel
    // 底层通过 Tower/Tonic Discover 流式同步节点新增、变更与下线
    let channel = registry.tonic_channel("user-service").await?;

    // 2. 直接传入 gRPC 客户端桩代码（如 UserServiceClient::new(channel)）
    // 通道内部会自动感知实例伸缩，实现动态无缝轮询与故障转移

    Ok(())
}
```

若需定制底层 gRPC 连接参数（超时、保活、并发等），可使用 `tonic_channel_with_config`：

```rust
let channel = registry.tonic_channel_with_config("user-service", |endpoint| {
    endpoint
        .timeout(std::time::Duration::from_secs(3))
        .concurrency_limit(256)
}).await?;
```

---

## 🏗️ 架构概览

```
  ┌──────────────────────────────────────────────────────────┐
  │         应用业务层 / 网关 / Tonic gRPC / Tower 中间件       │
  └───────────────┬──────────────────────────┬───────────────┘
                  │ (select_instance)        │ (tonic_channel / discover_stream)
                  ▼                          ▼
  ┌──────────────────────────────────────────────────────────┐
  │            RegistryService (统一门面与控制中心)            │
  └───────────────┬──────────────────────────┬───────────────┘
                  │                          │
                  ▼                          ▼
  ┌──────────────────────────────┐ ┌─────────────────────────┐
  │  ServiceDirectory (快照目录)   │ │  Selector (路由负载均衡) │
  ├──────────────────────────────┤ ├─────────────────────────┤
  │ • < 5ns 纯内存无锁 RCU 快照   │ │ • P2C (动态负载二选一)   │
  │ • 增量 Watch 长连接自愈监听    │ │ • ConsistentHash (环分片)│
  │ • 本地磁盘快照容灾降级恢复     │ │ • RoundRobin (原子无锁) │
  │ • Tower Discover 增量变更流   │ │ • WeightedRoundRobin    │
  │ • 周期性全量静默对齐兜底       │ │ • TagFilter / Canary    │
  └───────────────┬──────────────┘ └─────────────────────────┘
                  │
                  ▼
  ┌──────────────────────────────────────────────────────────┐
  │              Registry Provider SPI 核心契约              │
  ├──────────────────────────────────────────────────────────┤
  │    [Local Provider]  │  [Etcd Provider]  │  [Nacos Provider] │
  └──────────────────────────────────────────────────────────┘
```

---

## 📄 License

Licensed under either of:
- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.


## 分布式协调：锁与轮次认领

`pecs_registry::lock::{LockService, LockConfig, LockGuard}` 提供独立于 `Registry` trait 的 etcd 协调能力，需启用 Cargo feature `etcd`。锁不使用 Local/Nacos provider，不因注册发现关闭而关闭，也不会降级为本地锁。实现见 [src/lock.rs](src/lock.rs)。

### 初始化与配置

```rust
use pecs_registry::{RegistryConfig, RegistryResult, lock::{LockConfig, LockService}};

fn create_locks() -> RegistryResult<LockService> {
    LockService::new(
        RegistryConfig {
            host: "http://127.0.0.1:2379".into(),
            ..Default::default()
        },
        LockConfig {
            enabled: true,
            prefix: "/pecs/production/locks".into(),
            ..Default::default()
        },
    )
}
```

复用同一个 Service 或其 clone，共享连接；每次获取仍使用独立 lease。连接支持单 URL、逗号分隔端点、用户名密码，以及 `RegistryConfig.tls` 的 PEM 参数 `domain_name`、`ca_pem`、`client_cert_pem`、`client_key_pem`。初始化只校验配置，连接在首次调用时建立。

| `LockConfig` 字段 | 库默认值 | 含义 |
| --- | --- | --- |
| `enabled` | `false` | 禁用时获取锁和认领均返回配置错误 |
| `prefix` | `/pecs/dev/locks` | 协调资源前缀；同环境竞争者须一致，不同环境须隔离 |
| `lease_ttl_secs` | `30` | 执行锁租约 TTL，后台自动续租；不是业务超时 |
| `acquire_timeout_ms` | `3000` | 默认等待预算，覆盖 Lock 与所有权读取 |
| `rpc_timeout_ms` | `3000` | lease、所有权读取、认领事务及释放等 RPC 的超时预算 |

TTL 至少 3 秒，两个超时字段须大于 0，`rpc_timeout_ms` 必须小于 TTL 毫秒数的三分之一；前缀不能只有 `/`。资源名不能为空，不能包含空路径段、`.` 或 `..`。最终名称为 `{prefix}/{resource}`；`RegistryConfig.namespace/group` 不会自动拼进锁名，环境隔离需显式放进 prefix。

### 接口契约

| 接口 | 行为 |
| --- | --- |
| `acquire(resource)` | 使用配置中的等待预算获取执行锁，返回不可 Clone 的 Guard |
| `acquire_with_timeout(resource, Duration)` | 覆盖本次等待预算；必须大于 0，是有界排队，不是立即返回的 try-lock |
| `guard.lost().await` / `guard.is_lost()` | 感知续租失败或本地确认过期；失效 Guard 不可恢复使用 |
| `guard.fencing_token()` | 所有权 key 的 `create_revision`；下游须原子校验才能拒绝旧执行者写入 |
| `guard.release().await` | 消耗 Guard，等待 revoke 的服务端确认；失败返回错误 |
| Drop Guard | 通知后台停止续租并尽力 revoke；无法等待释放确认，失败时等待服务端 TTL 回收 |
| `claim_once(resource, retention_secs)` | 原子认领一个名称，返回是否本次成功；保留秒数必须大于 0 |

连接和 lease 申请另有超时，不包含在获取锁的等待预算里。Guard 必须存活至受保护业务结束；只写 `locks.acquire(...).await?;` 会立即丢弃它。每次获取的 lease 独立，锁不可重入，避免外层和业务层再次获取同一资源。

### `claim_once` 的保证与限制

认领通过 `version(key) == 0` 条件事务写入，记录使用独立的有限期 lease，不自动续租，也不依赖执行锁 lease。因此执行锁释放、业务取消或进程崩溃不会主动删除认领记录。

| 返回值/事件 | 调用方应如何处理 |
| --- | --- |
| `Ok(true)` | 当前调用认领成功，可以开始这次尝试 |
| `Ok(false)` | 已有记录，跳过本次执行 |
| `Err(...)` | 跳过执行；事务可能已提交，不能把错误当作“尚未认领” |
| 认领后失败或崩溃 | 保留记录，本轮不自动接管/重试 |
| 记录过期、被删除或历史数据丢失 | 名称可再次认领；不提供永久去重或历史重放保护 |

服务端明确拒绝事务时会尽力回收本次未使用的 lease；事务错误或超时结果不确定时保留 lease 等待过期，避免撤销可能已提交的记录。认领记录只是一次**尝试**，不记录业务完成结果，也不是幂等结果缓存。

锁解决执行互斥，认领解决保留期内同一名称重复尝试。只用认领无法阻止跨周期长任务重叠，只用锁无法阻止释放后的串行重跑。通用作业入口应组合使用，并以稳定任务名和计划周期组成认领名称。保留期应覆盖执行超时及预期延迟，不能把资源名、执行锁 lease 与认领 lease 混用。

### 组合使用示例

以下示例在获取锁后认领指定轮次，监听失锁，并以 60 秒限制认领及业务执行。调用方提供稳定的 `slot`，业务 Future 内的远程写入仍需幂等保护。

```rust
use std::{future::Future, time::Duration};
use pecs_registry::{RegistryError, RegistryResult, lock::LockService};

async fn run_tick(
    locks: &LockService,
    slot: i64,
    work: impl Future<Output = RegistryResult<()>>,
) -> RegistryResult<()> {
    let guard = locks.acquire_with_timeout(
        "jobs/catalog", Duration::from_millis(100),
    ).await?;
    let attempt = async {
        if !locks.claim_once(&format!("job-runs/catalog/{slot}"), 7200).await? {
            return Ok(());
        }
        work.await
    };
    let result = tokio::select! {
        biased;
        _ = guard.lost() => Err(RegistryError::driver("job lock lost")),
        result = tokio::time::timeout(Duration::from_secs(60), attempt) =>
            result.unwrap_or_else(|_| Err(RegistryError::timeout("job execution"))),
    };
    let released = guard.release().await;
    result?;
    released
}
```

丢弃 Future 无法撤回已经提交的 RPC，也无法停止业务自行 spawn 的工作。下游仍需幂等键、条件更新或原子 fencing；锁与认领不能承诺业务副作用严格执行一次。fencing token 依赖同一 etcd 历史的 revision 顺序，集群恢复或重建时需另行处理 epoch/revision 边界。

### 验证与排查

```sh
cargo test -p pecs-registry
# 仅对隔离的 etcd 运行；这些测试默认 ignored。
ETCD_TEST_ENDPOINT=http://127.0.0.1:12379 cargo test -p pecs-registry --all-features --test etcd_coordination_test -- --ignored
```

[协调测试](tests/etcd_coordination_test.rs)覆盖竞争、跨 TTL 续租、Drop/取消清理、fencing 顺序、撤销后失锁，以及认领竞争、独立租约和到期回收。普通测试通过不代表真实 etcd 集成测试已执行。连接不可用时检查端点、认证/TLS 和多数派可用性；重复执行时先核对所有竞争者的 prefix、resource、轮次算法及记录保留期。
