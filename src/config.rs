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

    #[serde(default)]
    pub tls: Option<EtcdTlsConfig>,
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
            tls: None,
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
    /// 是否配置了有效的用户名与密码认证凭据（任一为空则视为不启用密码认证）
    pub fn has_auth_credentials(&self) -> bool {
        self.user.as_ref().is_some_and(|u| !u.trim().is_empty())
            && self.password.as_ref().is_some_and(|p| !p.is_empty())
    }

    /// 获取单点地址 URL
    pub fn endpoint(&self) -> String {
        self.endpoints().into_iter().next().unwrap_or_default()
    }

    /// 解析并返回注册中心集群终端地址列表
    /// 支持逗号分隔的集群配置（如 host: "10.0.0.1:2379,10.0.0.2:2379"）或单节点 host + port
    pub fn endpoints(&self) -> Vec<String> {
        self.host
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(|addr| {
                if addr.starts_with("http://") || addr.starts_with("https://") {
                    addr.to_string()
                } else if addr.starts_with('[') && addr.ends_with(']') {
                    format!("http://{addr}:{}", self.port)
                } else if addr.contains(':') {
                    format!("http://{addr}")
                } else {
                    format!("http://{addr}:{}", self.port)
                }
            })
            .collect()
    }
}

#[cfg(feature = "etcd")]
pub(crate) async fn connect_etcd(
    config: &RegistryConfig,
) -> crate::RegistryResult<etcd_client::Client> {
    let timeout = std::time::Duration::from_secs(config.timeout.max(1));
    let mut options = etcd_client::ConnectOptions::new().with_connect_timeout(timeout);
    if let Some(tls) = &config.tls {
        let mut options_tls = etcd_client::TlsOptions::new().with_native_roots();
        if let Some(domain) = &tls.domain_name {
            options_tls = options_tls.domain_name(domain);
        }
        if let Some(ca) = &tls.ca_pem {
            options_tls = options_tls.ca_certificate(etcd_client::Certificate::from_pem(ca));
        }
        match (&tls.client_cert_pem, &tls.client_key_pem) {
            (Some(cert), Some(key)) => {
                options_tls = options_tls.identity(etcd_client::Identity::from_pem(cert, key))
            }
            (None, None) => {}
            _ => {
                return Err(crate::RegistryError::config(
                    "TLS client certificate and key must be supplied together",
                ))
            }
        }
        options = options.with_tls(options_tls);
    }
    if config.has_auth_credentials() {
        if let (Some(user), Some(password)) = (&config.user, &config.password) {
            options = options.with_user(user, password);
        }
    }
    tokio::time::timeout(
        timeout,
        etcd_client::Client::connect(config.endpoints(), Some(options)),
    )
    .await
    .map_err(|_| crate::RegistryError::timeout("etcd connect"))?
    .map_err(|e| crate::RegistryError::connection(e.to_string()))
}

/// PEM credentials. Keep private keys out of committed configuration.
#[derive(Clone, Deserialize, Serialize, Debug, Default)]
pub struct EtcdTlsConfig {
    pub domain_name: Option<String>,
    pub ca_pem: Option<String>,
    pub client_cert_pem: Option<String>,
    pub client_key_pem: Option<String>,
}
