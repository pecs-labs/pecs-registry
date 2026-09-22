pub mod builder;
pub mod config;
pub mod directory;
pub mod discover;
pub mod error;
pub mod instance;
pub mod provider;
pub mod selector;
pub mod traits;

pub use builder::RegistryBuilder;
pub use config::RegistryConfig;
pub use directory::ServiceDirectory;
#[cfg(feature = "tower")]
pub use discover::ServiceDiscover;
pub use error::{RegistryError, RegistryResult};
pub use instance::{Endpoint, ServiceInstance};
pub use provider::*;
pub use selector::{
    RandomSelector, RoundRobinSelector, SelectContext, Selector, TagFilterSelector,
    WeightedRoundRobinSelector,
};
pub use traits::{EventStream, InstanceListener, Registry, ServiceEvent};

use std::sync::Arc;

/// 注册中心底层实现提供者枚举
#[derive(Clone)]
pub enum RegistryProvider {
    #[cfg(feature = "etcd")]
    Etcd(Arc<EtcdRegistry>),
    Nacos(Arc<NacosRegistry>),
    Local(Arc<LocalRegistry>),
}

/// 统一注册中心与服务发现门面
///
/// 内部采用三层解耦工业级架构：
/// 1. SPI 提供者适配层 (`Arc<dyn Registry>`)；
/// 2. 本地事件驱动与无锁快照目录层 (`ServiceDirectory`)；
/// 3. 独立负载均衡与路由策略层 (`Arc<dyn Selector>`)。
#[derive(Clone)]
pub struct RegistryService {
    provider: Arc<dyn Registry>,
    directory: Arc<ServiceDirectory>,
    selector: Arc<dyn Selector>,
}

impl RegistryService {
    /// 构造标准注册中心服务门面（默认使用经典无锁 Round-Robin 轮询选择器）
    pub fn new(provider: Arc<dyn Registry>) -> Self {
        let directory = Arc::new(ServiceDirectory::new(provider.clone()));
        let selector = Arc::new(RoundRobinSelector::new());
        Self {
            provider,
            directory,
            selector,
        }
    }

    /// 自定义配置负载均衡策略（如权重选择器、灰度标签选择器）
    pub fn with_selector(provider: Arc<dyn Registry>, selector: Arc<dyn Selector>) -> Self {
        let directory = Arc::new(ServiceDirectory::new(provider.clone()));
        Self {
            provider,
            directory,
            selector,
        }
    }

    /// 从已构建好的三层组件中组装门面
    pub fn from_parts(
        provider: Arc<dyn Registry>,
        directory: Arc<ServiceDirectory>,
        selector: Arc<dyn Selector>,
    ) -> Self {
        Self {
            provider,
            directory,
            selector,
        }
    }

    /// 获取链式构建器
    pub fn builder() -> RegistryBuilder {
        RegistryBuilder::new()
    }

    /// 构造 Local 本地直连门面便捷方法
    pub fn new_local(local: Arc<LocalRegistry>) -> Self {
        Self::new(local)
    }

    #[cfg(feature = "etcd")]
    /// 构造 Etcd 门面便捷方法
    pub fn new_etcd(etcd: Arc<EtcdRegistry>) -> Self {
        Self::new(etcd)
    }

    /// 构造 Nacos 门面便捷方法
    pub fn new_nacos(nacos: Arc<NacosRegistry>) -> Self {
        Self::new(nacos)
    }

    /// 获取底层服务目录只读快照引用
    pub fn directory(&self) -> Arc<ServiceDirectory> {
        self.directory.clone()
    }

    /// 获取当前负载均衡选择器
    pub fn selector(&self) -> Arc<dyn Selector> {
        self.selector.clone()
    }

    /// 获取底层注册提供者 SPI 引用
    pub fn provider(&self) -> Arc<dyn Registry> {
        self.provider.clone()
    }

    /// 向注册中心登记当前服务节点（带自愈保活心跳与即时本地快照感知）
    pub async fn register(&self, instance: &ServiceInstance) -> RegistryResult<()> {
        self.provider.register(instance).await?;
        self.directory.apply_event(&instance.name, ServiceEvent::Upsert(instance.clone()));
        Ok(())
    }

    /// 从注册中心注销当前服务实例（撤销租约并优雅移除 key）
    pub async fn deregister(&self) -> RegistryResult<()> {
        self.directory.shutdown().await;
        self.provider.deregister().await
    }

    /// 服务发现：根据服务名查询当前所有可用实例（基于事件驱动只读快照，纳秒级无锁返回）
    pub async fn discover(&self, service_name: &str) -> RegistryResult<Vec<ServiceInstance>> {
        let instances = self.directory.load_or_watch(service_name).await?;
        Ok((*instances).clone())
    }

    /// 负载均衡选择：根据默认策略选择指定微服务的一个可用目标实例
    pub async fn select_instance(
        &self,
        service_name: &str,
    ) -> RegistryResult<Option<ServiceInstance>> {
        self.select_instance_with_context(&SelectContext::new(service_name))
            .await
    }

    /// 带上下文的负载均衡选择（支持灰度、请求头标签匹配等高级路由策略）
    pub async fn select_instance_with_context(
        &self,
        ctx: &SelectContext,
    ) -> RegistryResult<Option<ServiceInstance>> {
        let instances = self.directory.load_or_watch(&ctx.service_name).await?;
        if instances.is_empty() {
            return Ok(None);
        }

        let chosen = self.selector.select(ctx, &instances);
        Ok(chosen.cloned())
    }

    #[cfg(feature = "tower")]
    /// 创建针对指定微服务的 Tower Discover 事件流
    pub async fn discover_stream(&self, service_name: &str) -> RegistryResult<ServiceDiscover> {
        self.directory.discover_stream(service_name).await
    }

    #[cfg(feature = "tonic")]
    /// 创建开箱即用的 Tonic 动态负载均衡 Channel
    pub async fn tonic_channel(
        &self,
        service_name: &str,
    ) -> RegistryResult<tonic::transport::Channel> {
        self.directory.tonic_channel(service_name).await
    }

    #[cfg(feature = "tonic")]
    /// 创建带自定义配置的 Tonic 动态负载均衡 Channel
    pub async fn tonic_channel_with_config<F>(
        &self,
        service_name: &str,
        configure: F,
    ) -> RegistryResult<tonic::transport::Channel>
    where
        F: Fn(tonic::transport::Endpoint) -> tonic::transport::Endpoint + Send + Sync + 'static,
    {
        self.directory
            .tonic_channel_with_config(service_name, configure)
            .await
    }
}

/// 常用类型便捷导出
pub mod prelude {
    pub use crate::builder::RegistryBuilder;
    pub use crate::config::RegistryConfig;
    pub use crate::directory::ServiceDirectory;
    #[cfg(feature = "tower")]
    pub use crate::discover::ServiceDiscover;
    pub use crate::error::{RegistryError, RegistryResult};
    pub use crate::instance::{Endpoint, ServiceInstance};
    pub use crate::provider::local::LocalRegistry;
    pub use crate::selector::{
        RandomSelector, RoundRobinSelector, SelectContext, Selector, TagFilterSelector,
        WeightedRoundRobinSelector,
    };
    pub use crate::traits::{EventStream, InstanceListener, Registry, ServiceEvent};
    pub use crate::RegistryService;

    #[cfg(feature = "etcd")]
    pub use crate::provider::etcd::EtcdRegistry;
    pub use crate::provider::nacos::NacosRegistry;
}
