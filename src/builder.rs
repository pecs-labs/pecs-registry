use std::sync::Arc;
use std::time::Duration;

use crate::config::RegistryConfig;
use crate::directory::ServiceDirectory;
use crate::error::RegistryResult;
use crate::provider::local::LocalRegistry;
use crate::selector::{RoundRobinSelector, Selector};
use crate::traits::Registry;
use crate::RegistryService;

/// 链式构造器，用于开箱即用快速装配注册中心门面
pub struct RegistryBuilder {
    provider: Option<Arc<dyn Registry>>,
    selector: Option<Arc<dyn Selector>>,
    reconcile_interval: Duration,
}

impl RegistryBuilder {
    pub fn new() -> Self {
        Self {
            provider: None,
            selector: None,
            reconcile_interval: Duration::from_secs(60),
        }
    }

    /// 使用本地直连提供者构造（Local Provider，无需外部中间件）
    pub fn local() -> Self {
        let mut builder = Self::new();
        builder.provider = Some(Arc::new(LocalRegistry::new()));
        builder
    }

    /// 使用指定底层提供者（Provider）
    pub fn with_provider(mut self, provider: Arc<dyn Registry>) -> Self {
        self.provider = Some(provider);
        self
    }

    /// 使用指定负载均衡选择器（如 RoundRobin, Weighted, Canary 等）
    pub fn with_selector(mut self, selector: Arc<dyn Selector>) -> Self {
        self.selector = Some(selector);
        self
    }

    /// 配置静默校准同步周期
    pub fn with_reconcile_interval(mut self, interval: Duration) -> Self {
        self.reconcile_interval = interval;
        self
    }

    /// 根据统一配置自动装配底层注册中心驱动
    pub fn from_config(config: &RegistryConfig) -> Self {
        let mut builder = Self::new();
        let kind = config.kind.to_lowercase();

        match kind.as_str() {
            #[cfg(feature = "etcd")]
            "etcd" => {
                let etcd = Arc::new(crate::provider::etcd::EtcdRegistry::new(config.clone()));
                builder.provider = Some(etcd);
            }
            "nacos" => {
                let nacos_cfg = crate::provider::nacos::NacosConfig {
                    server_addr: format!("{}:{}", config.host, config.port),
                    app_name: "pecs-app".to_string(),
                    namespace: config.namespace.clone(),
                    enable_config: true,
                    enable_naming: config.enable,
                    username: config.user.clone(),
                    password: config.password.clone(),
                    registration: crate::provider::nacos::NacosRegistrationConfig {
                        service_name: "pecs-app".to_string(),
                        group: config.group.clone(),
                        cluster_name: "DEFAULT".to_string(),
                        weight: 1.0,
                        ephemeral: true,
                        metadata: std::collections::HashMap::new(),
                    },
                    bootstrap: vec![],
                };
                let nacos = Arc::new(crate::provider::nacos::NacosRegistry::new(nacos_cfg));
                builder.provider = Some(nacos);
            }
            _ => {
                builder.provider = Some(Arc::new(LocalRegistry::new()));
            }
        }

        builder
    }

    /// 完成装配并返回 `RegistryService` 统一门面
    pub fn build(self) -> RegistryResult<Arc<RegistryService>> {
        let provider = self
            .provider
            .unwrap_or_else(|| Arc::new(LocalRegistry::new()));
        let selector = self
            .selector
            .unwrap_or_else(|| Arc::new(RoundRobinSelector::new()));

        let directory = Arc::new(ServiceDirectory::with_reconcile_interval(
            provider.clone(),
            self.reconcile_interval,
        ));

        Ok(Arc::new(RegistryService::from_parts(
            provider, directory, selector,
        )))
    }
}

impl Default for RegistryBuilder {
    fn default() -> Self {
        Self::new()
    }
}
