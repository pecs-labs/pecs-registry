use async_trait::async_trait;
use etcd_client::{Client, ConnectOptions, EventType, GetOptions, PutOptions, WatchOptions};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::sync::CancellationToken;

use crate::config::RegistryConfig;
use crate::error::{RegistryError, RegistryResult};
use crate::instance::ServiceInstance;
use crate::traits::{EventStream, Registry, ServiceEvent};

/// 基于 etcd-client 的微服务注册中心实现（支持租约心跳自愈与动态事件流 Watch）
#[derive(Clone)]
pub struct EtcdRegistry {
    config: RegistryConfig,
    client: Arc<Mutex<Option<Client>>>,
    registered_key: Arc<Mutex<Option<String>>>,
    lease_id: Arc<Mutex<Option<i64>>>,
    cancel_token: Arc<Mutex<Option<CancellationToken>>>,
}

impl EtcdRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            config,
            client: Arc::new(Mutex::new(None)),
            registered_key: Arc::new(Mutex::new(None)),
            lease_id: Arc::new(Mutex::new(None)),
            cancel_token: Arc::new(Mutex::new(None)),
        }
    }

    /// 获取统一单次操作超时时间
    fn op_timeout(&self) -> Duration {
        Duration::from_secs(self.config.timeout.max(1))
    }

    /// 清空失效的 client 缓存，以便下一次调用重新创建底层连接
    pub async fn invalidate_client(&self) {
        let mut guard = self.client.lock().await;
        *guard = None;
    }

    /// 获取或创建 etcd Client（支持集群多节点与超时控制）
    async fn get_or_connect_client(&self) -> RegistryResult<Client> {
        let mut guard = self.client.lock().await;
        if let Some(client) = guard.as_ref() {
            return Ok(client.clone());
        }

        let endpoints = self.config.endpoints();
        let endpoint_strs: Vec<&str> = endpoints.iter().map(|s| s.as_str()).collect();

        let mut options = ConnectOptions::new().with_timeout(self.op_timeout());

        if let (Some(u), Some(p)) = (&self.config.user, &self.config.password) {
            if !u.is_empty() {
                options = options.with_user(u, p);
            }
        }

        let connect_fut = Client::connect(&endpoint_strs[..], Some(options));
        let client = tokio::time::timeout(self.op_timeout(), connect_fut)
            .await
            .map_err(|_| RegistryError::timeout(format!("连接 etcd 集群 ({endpoints:?}) 超时")))?
            .map_err(|err| RegistryError::connection(format!("连接 etcd ({endpoints:?}) 失败: {err}")))?;

        *guard = Some(client.clone());
        Ok(client)
    }

    /// 构造实例存储键: `/{namespace}/{group}/{service_name}/{service_id}`
    fn make_instance_key(&self, service_name: &str, service_id: &str) -> String {
        format!(
            "/{}/{}/{}/{}",
            self.config.namespace.trim_matches('/'),
            self.config.group.trim_matches('/'),
            service_name.trim_matches('/'),
            service_id.trim_matches('/')
        )
    }

    /// 构造服务发现前缀: `/{namespace}/{group}/{service_name}/`
    fn make_service_prefix(&self, service_name: &str) -> String {
        format!(
            "/{}/{}/{}/",
            self.config.namespace.trim_matches('/'),
            self.config.group.trim_matches('/'),
            service_name.trim_matches('/')
        )
    }

    /// 执行一次实际的租约申请与键值写入（带超时保护）
    async fn do_register_once(
        &self,
        client: &mut Client,
        instance: &ServiceInstance,
    ) -> RegistryResult<(i64, String)> {
        let ttl = if self.config.ttl > 0 {
            self.config.ttl as i64
        } else {
            10
        };

        // 1. 申请租约（带操作超时）
        let grant_fut = client.lease_grant(ttl, None);
        let lease = tokio::time::timeout(self.op_timeout(), grant_fut)
            .await
            .map_err(|_| RegistryError::timeout("etcd lease_grant 超时"))?
            .map_err(|err| RegistryError::driver(format!("etcd lease_grant 失败: {err}")))?;

        let lease_id = lease.id();
        let key = self.make_instance_key(&instance.name, &instance.service_id);
        let val = serde_json::to_string(instance)
            .map_err(|err| RegistryError::Serialization(err))?;

        // 2. 写入键值并绑定租约（带操作超时）
        let put_opts = PutOptions::new().with_lease(lease_id);
        let put_fut = client.put(key.as_str(), val, Some(put_opts));
        tokio::time::timeout(self.op_timeout(), put_fut)
            .await
            .map_err(|_| RegistryError::timeout("etcd put 注册键超时"))?
            .map_err(|err| RegistryError::driver(format!("向 etcd 写入注册键失败: {err}")))?;

        *self.registered_key.lock().await = Some(key.clone());
        *self.lease_id.lock().await = Some(lease_id);

        Ok((lease_id, key))
    }
}

#[async_trait]
impl Registry for EtcdRegistry {
    /// 向 etcd 注册当前服务实例，带租约心跳续期与断线自动重试/重新注册
    async fn register(&self, instance: &ServiceInstance) -> RegistryResult<()> {
        if !self.config.enable || self.config.host.trim().is_empty() {
            tracing::info!("etcd 注册中心未启用或 host 为空，跳过服务实例注册");
            return Ok(());
        }

        // 取消上一轮心跳任务（若存在）
        if let Some(token) = self.cancel_token.lock().await.take() {
            token.cancel();
        }

        let cancel_token = CancellationToken::new();
        *self.cancel_token.lock().await = Some(cancel_token.clone());

        let ttl = if self.config.ttl > 0 {
            self.config.ttl as i64
        } else {
            10
        };
        let heartbeat_interval = Duration::from_secs((ttl / 3).max(1) as u64);

        let this = self.clone();
        let inst = instance.clone();
        let token = cancel_token.clone();

        // 启动后台自愈式心跳与重连注册任务
        tokio::spawn(async move {
            let mut is_first_registration = true;

            loop {
                if token.is_cancelled() {
                    break;
                }

                // 1. 获取客户端连接（带重试）
                let mut client = match this.get_or_connect_client().await {
                    Ok(c) => c,
                    Err(err) => {
                        tracing::warn!("⚠️ 连接 etcd 注册中心失败，3秒后重试: {err}");
                        this.invalidate_client().await;
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(3)) => continue,
                        }
                    }
                };

                // 2. 申请租约并注册实例元数据
                let (lease_id, key) = match this.do_register_once(&mut client, &inst).await {
                    Ok(res) => res,
                    Err(err) => {
                        tracing::warn!("⚠️ 向 etcd 写入注册键失败，3秒后重试: {err}");
                        this.invalidate_client().await;
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(3)) => continue,
                        }
                    }
                };

                if is_first_registration {
                    tracing::info!(
                        "✅ 成功向 etcd ({:?}) 注册服务实例: key={key} (lease: {lease_id})",
                        this.config.endpoints(),
                    );
                    is_first_registration = false;
                } else {
                    tracing::info!("🔄 etcd 租约自愈重连成功，重新注册实例: key={key} (lease: {lease_id})");
                }

                // 3. 建立心跳保活流（带超时保护）
                let keep_alive_fut = client.lease_keep_alive(lease_id);
                let (mut keeper, mut stream) = match tokio::time::timeout(this.op_timeout(), keep_alive_fut).await {
                    Ok(Ok(pair)) => pair,
                    Ok(Err(err)) => {
                        tracing::warn!("⚠️ 建立 etcd lease_keep_alive 失败: {err}，2秒后尝试重新建连");
                        this.invalidate_client().await;
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                        }
                    }
                    Err(_) => {
                        tracing::warn!("⚠️ 建立 etcd lease_keep_alive 超时，2秒后尝试重新建连");
                        this.invalidate_client().await;
                        tokio::select! {
                            _ = token.cancelled() => break,
                            _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                        }
                    }
                };

                // 4. 心跳保活与异常监听循环
                let mut connection_healthy = true;
                while connection_healthy {
                    tokio::select! {
                        _ = token.cancelled() => {
                            tracing::info!("收到注销信号，停止 etcd 租约心跳保活 (lease: {lease_id})");
                            return;
                        }
                        _ = tokio::time::sleep(heartbeat_interval) => {
                            if let Err(err) = keeper.keep_alive().await {
                                tracing::warn!("⚠️ etcd 发送心跳失败: {err}");
                                connection_healthy = false;
                            }
                        }
                        msg = stream.message() => {
                            match msg {
                                Ok(Some(_)) => {
                                    // 租约续签正常
                                }
                                Ok(None) => {
                                    tracing::warn!("⚠️ etcd 心跳流断开，触发重新注册与租约自愈机制");
                                    connection_healthy = false;
                                }
                                Err(err) => {
                                    tracing::warn!("⚠️ etcd 心跳回复异常: {err}，触发重新注册与租约自愈机制");
                                    connection_healthy = false;
                                }
                            }
                        }
                    }
                }

                // 心跳中断，清空旧 client 缓存并在短暂退避后重新注册
                this.invalidate_client().await;
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                }
            }
        });

        Ok(())
    }

    /// 从 etcd 注销服务实例并释放租约（带操作超时保护）
    async fn deregister(&self) -> RegistryResult<()> {
        if let Some(token) = self.cancel_token.lock().await.take() {
            token.cancel();
        }

        let key = self.registered_key.lock().await.take();
        let lease_id = self.lease_id.lock().await.take();

        if let Ok(mut client) = self.get_or_connect_client().await {
            if let Some(k) = key {
                let del_fut = client.delete(k.as_str(), None);
                let _ = tokio::time::timeout(self.op_timeout(), del_fut).await;
                tracing::info!(
                    "👋 已从 etcd ({:?}) 注销实例节点: key={k}",
                    self.config.endpoints()
                );
            }
            if let Some(id) = lease_id {
                let revoke_fut = client.lease_revoke(id);
                let _ = tokio::time::timeout(self.op_timeout(), revoke_fut).await;
                tracing::info!("🗑️ 已撤销 etcd 租约: {id}");
            }
        }

        Ok(())
    }

    /// 服务发现：根据服务名称查询所有健康实例（带单次操作超时保护）
    async fn list_instances(&self, service_name: &str) -> RegistryResult<Vec<ServiceInstance>> {
        if !self.config.enable || self.config.host.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut client = self.get_or_connect_client().await?;
        let prefix = self.make_service_prefix(service_name);
        let opts = GetOptions::new().with_prefix();

        let get_fut = client.get(prefix.as_str(), Some(opts));
        let resp = tokio::time::timeout(self.op_timeout(), get_fut)
            .await
            .map_err(|_| RegistryError::timeout(format!("etcd 查询服务 '{service_name}' 超时")))?
            .map_err(|err| RegistryError::driver(format!("etcd 获取服务列表失败: {err}")))?;

        let mut instances = Vec::new();
        for kv in resp.kvs() {
            if let Ok(val_str) = kv.value_str() {
                if let Ok(inst) = serde_json::from_str::<ServiceInstance>(val_str) {
                    instances.push(inst);
                }
            }
        }

        Ok(instances)
    }

    /// 订阅指定微服务的变更事件流（基于 etcd watch 前缀长流）
    async fn watch(&self, service_name: &str) -> RegistryResult<EventStream> {
        let (tx, rx) = mpsc::channel(64);
        if !self.config.enable || self.config.host.trim().is_empty() {
            return Ok(Box::pin(ReceiverStream::new(rx)));
        }

        let mut client = self.get_or_connect_client().await?;
        let prefix = self.make_service_prefix(service_name);
        let opts = WatchOptions::new().with_prefix();

        let watch_fut = client.watch(prefix.as_str(), Some(opts));
        let mut stream = tokio::time::timeout(self.op_timeout(), watch_fut)
            .await
            .map_err(|_| RegistryError::timeout(format!("建立 etcd watch ('{service_name}') 超时")))?
            .map_err(|e| RegistryError::driver(format!("建立 etcd watch ('{service_name}') 失败: {e}")))?;

        let svc_name = service_name.to_string();

        tokio::spawn(async move {
            loop {
                match stream.message().await {
                    Ok(Some(resp)) => {
                        for event in resp.events() {
                            match event.event_type() {
                                EventType::Put => {
                                    if let Some(kv) = event.kv() {
                                        if let Ok(val_str) = kv.value_str() {
                                            if let Ok(inst) = serde_json::from_str::<ServiceInstance>(val_str) {
                                                let _ = tx.send(ServiceEvent::Upsert(inst)).await;
                                            }
                                        }
                                    }
                                }
                                EventType::Delete => {
                                    if let Some(kv) = event.kv() {
                                        if let Ok(key_str) = kv.key_str() {
                                            let service_id = key_str
                                                .rsplit('/')
                                                .next()
                                                .unwrap_or_default()
                                                .to_string();
                                            let _ = tx
                                                .send(ServiceEvent::Delete {
                                                    service_name: svc_name.clone(),
                                                    service_id,
                                                })
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    Ok(None) => {
                        tracing::warn!("⚠️ etcd watch 流断开: {svc_name}");
                        break;
                    }
                    Err(err) => {
                        tracing::warn!("⚠️ etcd watch 异常: {err} (service: {svc_name})");
                        break;
                    }
                }
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
