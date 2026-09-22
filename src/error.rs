use thiserror::Error;

/// 注册中心统一错误定义
#[derive(Error, Debug)]
pub enum RegistryError {
    #[error("连接注册中心失败: {0}")]
    Connection(String),

    #[error("未找到微服务 '{0}' 的可用实例")]
    ServiceNotFound(String),

    #[error("未找到指定的实例: {0}")]
    InstanceNotFound(String),

    #[error("操作超时: {0}")]
    Timeout(String),

    #[error("数据序列化/反序列化错误: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("注册中心驱动底层错误: {0}")]
    Driver(String),

    #[error("配置错误: {0}")]
    Config(String),

    #[error("未知/内部错误: {0}")]
    Other(String),
}

impl RegistryError {
    pub fn driver(msg: impl Into<String>) -> Self {
        Self::Driver(msg.into())
    }

    pub fn connection(msg: impl Into<String>) -> Self {
        Self::Connection(msg.into())
    }

    pub fn timeout(msg: impl Into<String>) -> Self {
        Self::Timeout(msg.into())
    }

    pub fn config(msg: impl Into<String>) -> Self {
        Self::Config(msg.into())
    }

    pub fn other(msg: impl Into<String>) -> Self {
        Self::Other(msg.into())
    }
}

pub type RegistryResult<T> = Result<T, RegistryError>;
