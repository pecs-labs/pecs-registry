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

- 🚀 **极速无锁读取**：日常服务发现基于本地不可变快照，读操作纳秒级直接返回（无需协程挂起与网络 I/O）。
- 📡 **事件驱动推送**：基于底层 Watch 前缀事件流长连接，毫秒级感知节点上下线；断线指数退避重连。
- 🛡️ **生产级自愈保活**：etcd 租约自动续签与自愈重连，双重静默周期校准兜底，彻底杜绝数据漂移与雪崩。
- 🔌 **开箱即用多 Provider**：
  - `etcd`：生产级高可用集群注册中心（基于 `etcd-client`）。
  - `local`：本地直连 Provider，免外部中间件，极速单测与本地开发。
  - `nacos`：主流微服务治理协议接入。
- ⚖️ **丰富的负载均衡算法**：
  - `RoundRobinSelector`：基于微服务独立的原子计数器，零锁争用。
  - `WeightedRoundRobinSelector`：Nginx 经典平滑加权算法。
  - `RandomSelector`：高效随机。
  - `TagFilterSelector`：支持环境、版本、金丝雀/灰度流量染色过滤。
- 🧩 **高可扩展性**：支持自定义 SPI Provider，支持注册实例变更监听器（`InstanceListener`）。

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

## 🏗️ 架构概览

```
  ┌──────────────────────────────────────────────────────────┐
  │         应用业务层 / 网关 / RPC 客户端 / Tonic / Axum         │
  └────────────────────────────┬─────────────────────────────┘
                               │
                               ▼
  ┌──────────────────────────────────────────────────────────┐
  │            RegistryService (统一门面与控制中心)            │
  └───────────────┬──────────────────────────┬───────────────┘
                  │                          │
                  ▼                          ▼
  ┌──────────────────────────────┐ ┌─────────────────────────┐
  │  ServiceDirectory (快照目录)   │ │  Selector (路由负载均衡) │
  ├──────────────────────────────┤ ├─────────────────────────┤
  │ • < 20ns 纯内存无锁读取快照    │ │ • RoundRobin (原子无锁) │
  │ • 增量 Watch 长连接自愈监听    │ │ • WeightedRoundRobin    │
  │ • 周期性全量静默对齐兜底       │ │ • TagFilter / Canary    │
  │ • 扩展变更监听器支持          │ │ • Random                │
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
