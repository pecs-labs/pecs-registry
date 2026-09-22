pub mod local;
pub mod nacos;

#[cfg(feature = "etcd")]
pub mod etcd;

pub use local::LocalRegistry;
pub use nacos::{NacosBootstrapConfig, NacosConfig, NacosRegistrationConfig, NacosRegistry};

#[cfg(feature = "etcd")]
pub use etcd::EtcdRegistry;
