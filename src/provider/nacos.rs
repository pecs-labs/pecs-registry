use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::error::RegistryResult;
use crate::instance::{Endpoint, ServiceInstance};
use crate::traits::{EventStream, Registry};

/// Nacos 服务端与连接配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NacosConfig {
    /// Nacos 服务端地址，格式：`"127.0.0.1:8848"` 或带 http 前缀
    pub server_addr: String,
    /// 应用名称
    #[serde(default = "default_app_name")]
    pub app_name: String,
    /// 命名空间 ID (Tenant/Namespace)，默认 "public"
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// 是否开启服务注册与发现 (Naming)
    #[serde(default = "default_true")]
    pub enable_naming: bool,
    /// 鉴权用户名（可选）
    pub username: Option<String>,
    /// 鉴权密码（可选）
    pub password: Option<String>,
    /// 服务实例注册元数据配置
    #[serde(default)]
    pub registration: NacosRegistrationConfig,
}

fn default_app_name() -> String {
    "pecs-app".to_string()
}

fn default_namespace() -> String {
    "public".to_string()
}

fn default_true() -> bool {
    true
}

/// Nacos 实例注册元信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NacosRegistrationConfig {
    #[serde(default = "default_app_name")]
    pub service_name: String,
    #[serde(default = "default_group")]
    pub group: String,
    #[serde(default = "default_cluster")]
    pub cluster_name: String,
    #[serde(default = "default_weight")]
    pub weight: f64,
    #[serde(default = "default_true")]
    pub ephemeral: bool,
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

fn default_group() -> String {
    "DEFAULT_GROUP".to_string()
}

fn default_cluster() -> String {
    "DEFAULT".to_string()
}

fn default_weight() -> f64 {
    1.0
}

impl Default for NacosRegistrationConfig {
    fn default() -> Self {
        Self {
            service_name: default_app_name(),
            group: default_group(),
            cluster_name: default_cluster(),
            weight: default_weight(),
            ephemeral: true,
            metadata: HashMap::new(),
        }
    }
}

/// Nacos 注册中心适配器
#[derive(Clone)]
pub struct NacosRegistry {
    config: NacosConfig,
    registered_instance: Arc<Mutex<Option<ServiceInstance>>>,
    _cancel_token: Arc<Mutex<Option<CancellationToken>>>,
}

impl NacosRegistry {
    pub fn new(config: NacosConfig) -> Self {
        Self {
            config,
            registered_instance: Arc::new(Mutex::new(None)),
            _cancel_token: Arc::new(Mutex::new(None)),
        }
    }

    pub fn server_url(&self) -> String {
        let addr = self.config.server_addr.trim();
        if addr.starts_with("http://") || addr.starts_with("https://") {
            addr.to_string()
        } else {
            format!("http://{addr}")
        }
    }
}

#[async_trait]
impl Registry for NacosRegistry {
    async fn register(&self, instance: &ServiceInstance) -> RegistryResult<()> {
        if !self.config.enable_naming || self.config.server_addr.trim().is_empty() {
            tracing::info!("Nacos 服务注册已禁用或 server_addr 为空，跳过注册");
            return Ok(());
        }

        *self.registered_instance.lock().await = Some(instance.clone());

        #[cfg(feature = "nacos")]
        {
            let base_url = self.server_url();
            let endpoint = instance.http.as_ref().or(instance.grpc.as_ref());
            let (ip, port) = if let Some(ep) = endpoint {
                (ep.address.clone(), ep.port)
            } else {
                ("127.0.0.1".to_string(), 8080)
            };

            let client = reqwest::Client::new();
            let register_url = format!("{base_url}/nacos/v1/ns/instance");
            let mut params = vec![
                ("serviceName", instance.name.as_str()),
                ("ip", ip.as_str()),
                ("groupName", self.config.registration.group.as_str()),
                ("namespaceId", self.config.namespace.as_str()),
                ("clusterName", self.config.registration.cluster_name.as_str()),
            ];
            let port_str = port.to_string();
            params.push(("port", port_str.as_str()));

            let weight_str = instance.weight.to_string();
            params.push(("weight", weight_str.as_str()));

            match client.post(&register_url).form(&params).send().await {
                Ok(resp) => {
                    if resp.status().is_success() {
                        tracing::info!(
                            "✅ 成功向 Nacos ({}) 注册服务: service={}, ip={ip}, port={port}",
                            self.config.server_addr,
                            instance.name
                        );
                    } else {
                        tracing::warn!(
                            "⚠️ 向 Nacos 注册返回非成功状态: {}",
                            resp.status()
                        );
                    }
                }
                Err(err) => {
                    tracing::warn!("⚠️ 向 Nacos 注册请求失败: {err}");
                }
            }
        }

        #[cfg(not(feature = "nacos"))]
        {
            tracing::info!(
                "🚀 [Nacos Mock] 模拟注册服务: service={}, group={}, ep={:?}",
                instance.name,
                self.config.registration.group,
                instance.http
            );
        }

        Ok(())
    }

    async fn deregister(&self) -> RegistryResult<()> {
        if !self.config.enable_naming || self.config.server_addr.trim().is_empty() {
            return Ok(());
        }

        let instance = self.registered_instance.lock().await.take();

        #[cfg(feature = "nacos")]
        if let Some(inst) = instance {
            let base_url = self.server_url();
            let endpoint = inst.http.as_ref().or(inst.grpc.as_ref());
            let (ip, port) = if let Some(ep) = endpoint {
                (ep.address.clone(), ep.port)
            } else {
                ("127.0.0.1".to_string(), 8080)
            };

            let client = reqwest::Client::new();
            let deregister_url = format!("{base_url}/nacos/v1/ns/instance");
            let port_str = port.to_string();
            let params = vec![
                ("serviceName", inst.name.as_str()),
                ("ip", ip.as_str()),
                ("port", port_str.as_str()),
                ("groupName", self.config.registration.group.as_str()),
                ("namespaceId", self.config.namespace.as_str()),
            ];

            let _ = client.delete(&deregister_url).form(&params).send().await;
            tracing::info!("👋 已向 Nacos 发送注销请求: service={}", inst.name);
        }

        #[cfg(not(feature = "nacos"))]
        {
            tracing::info!(
                "👋 [Nacos Mock] 注销服务: service={}, group={}",
                self.config.registration.service_name,
                self.config.registration.group
            );
        }

        Ok(())
    }

    async fn list_instances(&self, service_name: &str) -> RegistryResult<Vec<ServiceInstance>> {
        if !self.config.enable_naming || self.config.server_addr.trim().is_empty() {
            return Ok(Vec::new());
        }

        #[cfg(feature = "nacos")]
        {
            let base_url = self.server_url();
            let client = reqwest::Client::new();
            let list_url = format!("{base_url}/nacos/v1/ns/instance/list");
            let query = vec![
                ("serviceName", service_name),
                ("groupName", self.config.registration.group.as_str()),
                ("namespaceId", self.config.namespace.as_str()),
                ("healthyOnly", "true"),
            ];

            #[derive(Deserialize)]
            struct NacosInstanceItem {
                ip: String,
                port: u16,
                #[serde(default)]
                weight: f64,
                #[serde(default)]
                healthy: bool,
                #[serde(default)]
                enabled: bool,
                #[serde(default)]
                metadata: HashMap<String, String>,
            }

            #[derive(Deserialize)]
            struct NacosListResponse {
                #[serde(default)]
                hosts: Vec<NacosInstanceItem>,
            }

            if let Ok(resp) = client.get(&list_url).query(&query).send().await {
                if let Ok(data) = resp.json::<NacosListResponse>().await {
                    let mut instances = Vec::new();
                    for h in data.hosts {
                        let id = format!("{service_name}-{}:{}", h.ip, h.port);
                        let endpoint = Endpoint::new(h.ip.clone(), h.port);
                        let mut inst = ServiceInstance::new_http(
                            id,
                            self.config.namespace.clone(),
                            self.config.registration.group.clone(),
                            service_name,
                            endpoint,
                        )
                        .with_weight(h.weight);
                        inst.healthy = h.healthy;
                        inst.enabled = h.enabled;
                        inst.metadata = h.metadata;
                        instances.push(inst);
                    }
                    return Ok(instances);
                }
            }
        }

        Ok(Vec::new())
    }

    async fn watch(&self, _service_name: &str) -> RegistryResult<EventStream> {
        let (_tx, rx) = tokio::sync::mpsc::channel(1);
        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
