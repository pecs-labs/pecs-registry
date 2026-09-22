use async_trait::async_trait;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tokio::sync::{broadcast, Mutex};
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::error::RegistryResult;
use crate::instance::ServiceInstance;
use crate::traits::{EventStream, Registry, ServiceEvent};

/// 本地注册中心实现（Local Provider）
///
/// 专为本地开发联调、单元测试及无外部中间件环境设计。
/// 具备完整的事件总线广播、实例 Upsert/Delete/Reset 与实时订阅功能。
#[derive(Clone)]
pub struct LocalRegistry {
    instances: Arc<RwLock<HashMap<String, HashMap<String, ServiceInstance>>>>,
    tx: broadcast::Sender<ServiceEvent>,
    registered_key: Arc<Mutex<Option<(String, String)>>>,
}

impl LocalRegistry {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(128);
        Self {
            instances: Arc::new(RwLock::new(HashMap::new())),
            tx,
            registered_key: Arc::new(Mutex::new(None)),
        }
    }

    /// 便捷测试/调试方法：向本地注册中心直接添加一个实例
    pub fn add_instance(&self, instance: ServiceInstance) {
        let mut write = self.instances.write().expect("local registry lock poisoned");
        let svc_map = write
            .entry(instance.name.clone())
            .or_insert_with(HashMap::new);
        svc_map.insert(instance.service_id.clone(), instance.clone());
        let _ = self.tx.send(ServiceEvent::Upsert(instance));
    }

    /// 便捷测试/调试方法：直接删除一个实例
    pub fn remove_instance(&self, service_name: &str, service_id: &str) {
        let mut write = self.instances.write().expect("local registry lock poisoned");
        if let Some(svc_map) = write.get_mut(service_name) {
            svc_map.remove(service_id);
        }
        let _ = self.tx.send(ServiceEvent::Delete {
            service_name: service_name.to_string(),
            service_id: service_id.to_string(),
        });
    }

    /// 清空所有实例
    pub fn clear(&self) {
        let mut write = self.instances.write().expect("local registry lock poisoned");
        write.clear();
    }
}

impl Default for LocalRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl Registry for LocalRegistry {
    async fn register(&self, instance: &ServiceInstance) -> RegistryResult<()> {
        {
            let mut write = self.instances.write().expect("local registry lock poisoned");
            let svc_map = write
                .entry(instance.name.clone())
                .or_insert_with(HashMap::new);
            svc_map.insert(instance.service_id.clone(), instance.clone());
        }

        *self.registered_key.lock().await = Some((instance.name.clone(), instance.service_id.clone()));

        let _ = self.tx.send(ServiceEvent::Upsert(instance.clone()));
        tracing::info!("✅ [LocalRegistry] 注册实例成功: {}:{}", instance.name, instance.service_id);
        Ok(())
    }

    async fn deregister(&self) -> RegistryResult<()> {
        let key = self.registered_key.lock().await.take();
        if let Some((svc_name, svc_id)) = key {
            {
                let mut write = self.instances.write().expect("local registry lock poisoned");
                if let Some(svc_map) = write.get_mut(&svc_name) {
                    svc_map.remove(&svc_id);
                }
            }
            let _ = self.tx.send(ServiceEvent::Delete {
                service_name: svc_name.clone(),
                service_id: svc_id.clone(),
            });
            tracing::info!("👋 [LocalRegistry] 注销实例成功: {svc_name}:{svc_id}");
        }
        Ok(())
    }

    async fn list_instances(&self, service_name: &str) -> RegistryResult<Vec<ServiceInstance>> {
        let read = self.instances.read().expect("local registry lock poisoned");
        let list = read
            .get(service_name)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        Ok(list)
    }

    async fn watch(&self, service_name: &str) -> RegistryResult<EventStream> {
        let rx = self.tx.subscribe();
        let svc_name = service_name.to_string();

        let stream = BroadcastStream::new(rx).filter_map(move |item| {
            if let Ok(event) = item {
                match &event {
                    ServiceEvent::Upsert(inst) if inst.name == svc_name => Some(event),
                    ServiceEvent::Delete { service_name, .. } if service_name == &svc_name => {
                        Some(event)
                    }
                    ServiceEvent::Reset(list)
                        if list.first().map(|i| &i.name) == Some(&svc_name) =>
                    {
                        Some(event)
                    }
                    _ => None,
                }
            } else {
                None
            }
        });

        Ok(Box::pin(stream))
    }
}
