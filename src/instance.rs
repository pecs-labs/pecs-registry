use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// 注册中心服务终端节点（Host + Port + Scheme）
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Endpoint {
    pub address: String,
    pub port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scheme: Option<String>,
}

impl Endpoint {
    pub fn new(address: impl Into<String>, port: u16) -> Self {
        Self {
            address: address.into(),
            port,
            scheme: None,
        }
    }

    pub fn with_scheme(mut self, scheme: impl Into<String>) -> Self {
        self.scheme = Some(scheme.into());
        self
    }

    pub fn to_url(&self, default_scheme: &str) -> String {
        let scheme = self.scheme.as_deref().unwrap_or(default_scheme);
        format!("{scheme}://{}:{}", self.address, self.port)
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.address, self.port)
    }
}

/// 注册中心微服务实例元数据定义
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ServiceInstance {
    pub service_id: String,
    pub namespace: String,
    pub group: String,
    pub name: String,

    /// HTTP 协议接入点（兼容老版本及直接访问）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http: Option<Endpoint>,

    /// gRPC 协议接入点（兼容老版本及直接访问）
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub grpc: Option<Endpoint>,

    /// 扩展协议与多端口接入点映射 (如 "metrics", "websocket" 等)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub endpoints: HashMap<String, Endpoint>,

    /// 扩展元数据 (如版本号、环境、金丝雀标签、机房等)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, String>,

    /// 负载均衡权重 (默认为 1.0)
    #[serde(default = "default_weight")]
    pub weight: f64,

    /// 节点启用状态 (默认 true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// 健康状态标记 (默认 true)
    #[serde(default = "default_true")]
    pub healthy: bool,
}

fn default_weight() -> f64 {
    1.0
}

fn default_true() -> bool {
    true
}

impl ServiceInstance {
    /// 构造包含全量核心字段的实例
    pub fn new(
        service_id: impl Into<String>,
        namespace: impl Into<String>,
        group: impl Into<String>,
        name: impl Into<String>,
        http: Option<Endpoint>,
        grpc: Option<Endpoint>,
    ) -> Self {
        let mut endpoints = HashMap::new();
        if let Some(ref h) = http {
            endpoints.insert("http".to_string(), h.clone());
        }
        if let Some(ref g) = grpc {
            endpoints.insert("grpc".to_string(), g.clone());
        }

        Self {
            service_id: service_id.into(),
            namespace: namespace.into(),
            group: group.into(),
            name: name.into(),
            http,
            grpc,
            endpoints,
            metadata: HashMap::new(),
            weight: 1.0,
            enabled: true,
            healthy: true,
        }
    }

    /// 便捷构造：单一 HTTP 实例
    pub fn new_http(
        service_id: impl Into<String>,
        namespace: impl Into<String>,
        group: impl Into<String>,
        name: impl Into<String>,
        http: Endpoint,
    ) -> Self {
        Self::new(service_id, namespace, group, name, Some(http), None)
    }

    /// 便捷构造：单一 gRPC 实例
    pub fn new_grpc(
        service_id: impl Into<String>,
        namespace: impl Into<String>,
        group: impl Into<String>,
        name: impl Into<String>,
        grpc: Endpoint,
    ) -> Self {
        Self::new(service_id, namespace, group, name, None, Some(grpc))
    }

    /// 极简便捷构造：用于测试或单机快速注册
    pub fn simple(
        service_name: impl Into<String>,
        host: impl Into<String>,
        port: u16,
    ) -> Self {
        let name = service_name.into();
        let host_str = host.into();
        let endpoint = Endpoint::new(host_str.clone(), port);
        let id = format!("{name}-{host_str}-{port}");
        Self::new_http(id, "default", "default", name, endpoint)
    }

    /// 设置服务名别名访问
    pub fn service_name(&self) -> &str {
        &self.name
    }

    /// 链式添加/设置元数据
    pub fn with_metadata(mut self, key: impl Into<String>, val: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), val.into());
        self
    }

    /// 链式设置权重
    pub fn with_weight(mut self, weight: f64) -> Self {
        self.weight = weight.max(0.0);
        self
    }

    /// 链式添加扩展端口
    pub fn with_endpoint(mut self, name: impl Into<String>, endpoint: Endpoint) -> Self {
        let key = name.into();
        if key.eq_ignore_ascii_case("http") {
            self.http = Some(endpoint.clone());
        } else if key.eq_ignore_ascii_case("grpc") {
            self.grpc = Some(endpoint.clone());
        }
        self.endpoints.insert(key, endpoint);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_instance_serialization_roundtrip() {
        let mut inst = ServiceInstance::new(
            "inst-01",
            "pecs",
            "dev",
            "pecs-bff",
            Some(Endpoint::new("127.0.0.1", 8001)),
            Some(Endpoint::new("127.0.0.1", 9001)),
        );
        inst = inst.with_metadata("version", "v1.0.0").with_weight(2.5);

        let json = serde_json::to_string(&inst).expect("serialize");
        let decoded: ServiceInstance = serde_json::from_str(&json).expect("deserialize");

        assert_eq!(decoded.service_id, "inst-01");
        assert_eq!(decoded.name, "pecs-bff");
        assert_eq!(decoded.http.as_ref().unwrap().port, 8001);
        assert_eq!(decoded.grpc.as_ref().unwrap().port, 9001);
        assert_eq!(decoded.metadata.get("version").unwrap(), "v1.0.0");
        assert_eq!(decoded.weight, 2.5);
    }
}
