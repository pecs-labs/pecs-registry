use async_trait::async_trait;
use std::pin::Pin;
use tokio_stream::Stream;

use crate::error::RegistryResult;
use crate::instance::ServiceInstance;

/// 微服务实例变更事件（用于事件驱动的动态服务发现）
#[derive(Debug, Clone, PartialEq)]
pub enum ServiceEvent {
    /// 实例上线或元数据更新
    Upsert(ServiceInstance),
    /// 实例下线或租约到期注销
    Delete {
        service_name: String,
        service_id: String,
    },
    /// 全量实例重置（网络重连、全量同步对齐）
    Reset(Vec<ServiceInstance>),
}

/// 统一变更事件流类型定义
pub type EventStream = Pin<Box<dyn Stream<Item = ServiceEvent> + Send>>;

/// 统一注册中心底层契约（Service Provider Interface - SPI）
///
/// 任何注册中心实现驱动（Memory、etcd、Nacos、Consul、Kubernetes 等）
/// 均通过实现此 Trait 接入系统。
#[async_trait]
pub trait Registry: Send + Sync {
    /// 注册本地服务实例，并启动自动续约与自愈保活心跳
    async fn register(&self, instance: &ServiceInstance) -> RegistryResult<()>;

    /// 注销本地服务实例，撤销租约并优雅移除节点
    async fn deregister(&self) -> RegistryResult<()>;

    /// 查询指定微服务当前在注册中心的全量实例列表
    async fn list_instances(&self, service_name: &str) -> RegistryResult<Vec<ServiceInstance>>;

    /// 订阅指定微服务的动态变更事件长连接流
    async fn watch(&self, service_name: &str) -> RegistryResult<EventStream>;
}

/// 实例变更监听器接口（用于外部插件/监控/链路统计接入）
pub trait InstanceListener: Send + Sync {
    fn on_change(&self, service_name: &str, instances: &[ServiceInstance]);
}

impl<F> InstanceListener for F
where
    F: Fn(&str, &[ServiceInstance]) + Send + Sync,
{
    fn on_change(&self, service_name: &str, instances: &[ServiceInstance]) {
        self(service_name, instances);
    }
}
