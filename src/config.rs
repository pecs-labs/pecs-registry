use serde::{Deserialize, Serialize};

/// 统一注册中心与服务发现配置
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct RegistryConfig {
    #[serde(default = "default_true")]
    pub enable: bool,

    /// 注册中心提供者类型: "etcd", "nacos", "local"
    #[serde(default = "default_kind")]
    pub kind: String,

    /// 单机主机或逗号分隔的多节点主机列表（例如 "10.0.0.1:2379,10.0.0.2:2379"）
    #[serde(default = "default_host")]
    pub host: String,

    #[serde(default = "default_port")]
    pub port: u16,

    #[serde(default = "default_namespace")]
    pub namespace: String,

    #[serde(default = "default_group")]
    pub group: String,

    pub user: Option<String>,
    pub password: Option<String>,

    /// 单次操作超时时间（秒）
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// 租约存活时间 TTL（秒）
    #[serde(default = "default_ttl")]
    pub ttl: u64,

    /// 本地快照缓存重拉取间隔（秒）
    #[serde(default = "default_cache_ttl")]
    pub cache_ttl: u64,

    /// 本地磁盘快照持久化容灾目录（可选）
    /// 当注册中心完全宕机或不可用时，应用可从本地历史快照恢复节点，保障极端情况可用性
    #[serde(default)]
    pub disk_cache_dir: Option<String>,

    /// 是否开启本地磁盘快照容灾机制
    #[serde(default)]
    pub disk_cache_enabled: bool,
}

fn default_true() -> bool {
    true
}

fn default_kind() -> String {
    "etcd".to_string()
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    2379
}

fn default_namespace() -> String {
    "pecs".to_string()
}

fn default_group() -> String {
    "dev".to_string()
}

fn default_timeout() -> u64 {
    10
}

fn default_ttl() -> u64 {
    30
}

fn default_cache_ttl() -> u64 {
    5
}

impl Default for RegistryConfig {
    fn default() -> Self {
        Self {
            enable: true,
            kind: default_kind(),
            host: default_host(),
            port: default_port(),
            namespace: default_namespace(),
            group: default_group(),
            user: None,
            password: None,
            timeout: default_timeout(),
            ttl: default_ttl(),
            cache_ttl: default_cache_ttl(),
            disk_cache_dir: None,
            disk_cache_enabled: false,
        }
    }
}

impl RegistryConfig {
    /// 获取单点地址 URL
    pub fn endpoint(&self) -> String {
        format!("http://{}:{}", self.host, self.port)
    }

    /// 解析并返回注册中心集群终端地址列表
    /// 支持逗号分隔的集群配置（如 host: "10.0.0.1:2379,10.0.0.2:2379"）或单节点 host + port
    pub fn endpoints(&self) -> Vec<String> {
        if self.host.contains(',') {
            self.host
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|addr| {
                    if addr.starts_with("http://") || addr.starts_with("https://") {
                        addr.to_string()
                    } else if addr.contains(':') {
                        format!("http://{addr}")
                    } else {
                        format!("http://{addr}:{}", self.port)
                    }
                })
                .collect()
        } else {
            vec![self.endpoint()]
        }
    }
}
