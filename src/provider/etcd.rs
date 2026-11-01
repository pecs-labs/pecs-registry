use async_trait::async_trait;
use etcd_client::{Client, EventType, GetOptions, PutOptions, WatchOptions};
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
    ready: Arc<std::sync::atomic::AtomicBool>,
    auth_epoch: Arc<std::sync::atomic::AtomicU64>,
    auth_refresh: Arc<Mutex<()>>,
    config: RegistryConfig,
    client: Arc<Mutex<Option<Client>>>,
    registered_key: Arc<Mutex<Option<String>>>,
    lease_id: Arc<Mutex<Option<i64>>>,
    cancel_token: Arc<Mutex<Option<CancellationToken>>>,
    lifecycle: Arc<Mutex<()>>,
    task: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl EtcdRegistry {
    pub fn new(config: RegistryConfig) -> Self {
        Self {
            ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            auth_epoch: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            auth_refresh: Arc::new(Mutex::new(())),
            config,
            client: Arc::new(Mutex::new(None)),
            registered_key: Arc::new(Mutex::new(None)),
            lease_id: Arc::new(Mutex::new(None)),
            cancel_token: Arc::new(Mutex::new(None)),
            lifecycle: Arc::new(Mutex::new(())),
            task: Arc::new(Mutex::new(None)),
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

        let client = crate::config::connect_etcd(&self.config).await?;

        *guard = Some(client.clone());
        Ok(client)
    }

    fn is_auth_expired(error: &etcd_client::Error) -> bool {
        // gRPC UNAUTHENTICATED = 16. PermissionDenied is not a token expiry.
        matches!(error, etcd_client::Error::GRpcStatus(status) if status.code() as i32 == 16)
    }

    async fn refresh_auth(&self, failed_epoch: u64) -> RegistryResult<()> {
        let _refresh = self.auth_refresh.lock().await;
        if self.auth_epoch.load(std::sync::atomic::Ordering::Acquire) != failed_epoch {
            return Ok(());
        }
        let client = self.get_or_connect_client().await?;
        // refresh_token updates the token shared by all clones, including leases.
        tokio::time::timeout(self.op_timeout(), client.refresh_token())
            .await
            .map_err(|_| RegistryError::timeout("etcd authentication refresh timeout"))?
            .map_err(|error| {
                RegistryError::connection(format!("etcd authentication refresh failed: {error}"))
            })?;
        self.auth_epoch
            .fetch_add(1, std::sync::atomic::Ordering::Release);
        tracing::info!("etcd authentication refreshed");
        Ok(())
    }

    /// Retry only an explicitly rejected authentication attempt, once. Callers use
    /// this for discovery and stream creation, not arbitrary replay of writes.
    async fn authenticated<T, F, Fut>(&self, mut operation: F) -> RegistryResult<T>
    where
        F: FnMut(Client) -> Fut,
        Fut: std::future::Future<Output = Result<T, etcd_client::Error>>,
    {
        let epoch = self.auth_epoch.load(std::sync::atomic::Ordering::Acquire);
        for attempt in 0..2 {
            let client = self.get_or_connect_client().await?;
            match tokio::time::timeout(self.op_timeout(), operation(client)).await {
                Ok(Ok(response)) => return Ok(response),
                Ok(Err(error))
                    if attempt == 0
                        && Self::is_auth_expired(&error)
                        && self
                            .config
                            .user
                            .as_ref()
                            .is_some_and(|user| !user.is_empty())
                        && self.config.password.is_some() =>
                {
                    self.refresh_auth(epoch).await?;
                }
                Ok(Err(error)) => return Err(RegistryError::driver(error.to_string())),
                Err(_) => return Err(RegistryError::timeout("etcd operation timeout")),
            }
        }
        unreachable!("the second attempt always returns")
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
        let val =
            serde_json::to_string(instance).map_err(|err| RegistryError::Serialization(err))?;

        // 2. 写入键值并绑定租约（带操作超时）
        let put_opts = PutOptions::new().with_lease(lease_id);
        let put_fut = client.put(key.as_str(), val, Some(put_opts));
        let written = tokio::time::timeout(self.op_timeout(), put_fut)
            .await
            .map_err(|_| RegistryError::timeout("etcd registration put timeout"))
            .and_then(|result| result.map_err(|error| RegistryError::driver(error.to_string())));
        if let Err(error) = written {
            let _ = tokio::time::timeout(self.op_timeout(), client.lease_revoke(lease_id)).await;
            return Err(error);
        }

        self.ready.store(true, std::sync::atomic::Ordering::Release);
        *self.registered_key.lock().await = Some(key.clone());
        *self.lease_id.lock().await = Some(lease_id);

        Ok((lease_id, key))
    }
}

#[async_trait]
impl Registry for EtcdRegistry {
    fn is_registered(&self) -> bool {
        self.ready.load(std::sync::atomic::Ordering::Acquire)
    }
    fn watch_includes_snapshot(&self) -> bool {
        true
    }

    /// 向 etcd 注册当前服务实例，带租约心跳续期与断线自动重试/重新注册
    async fn register(&self, instance: &ServiceInstance) -> RegistryResult<()> {
        if !self.config.enable || self.config.host.trim().is_empty() {
            tracing::info!("etcd 注册中心未启用或 host 为空，跳过服务实例注册");
            return Ok(());
        }

        let _lifecycle = self.lifecycle.lock().await;
        self.ready
            .store(false, std::sync::atomic::Ordering::Release);
        // 取消上一轮心跳任务（若存在）
        if let Some(token) = self.cancel_token.lock().await.take() {
            token.cancel();
        }

        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        // 首次注册必须得到服务端确认，调用失败由启动方决定如何处理。
        let initial = async {
            let mut client = self.get_or_connect_client().await?;
            self.do_register_once(&mut client, instance).await
        }
        .await;
        let (initial, initial_error) = match initial {
            Ok(value) => (Some(value), None),
            Err(error) => (None, Some(error)),
        };
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
        let task = tokio::spawn(async move {
            let mut first = initial;
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
                let (lease_id, key) = match if let Some(initial) = first.take() {
                    Ok(initial)
                } else {
                    this.do_register_once(&mut client, &inst).await
                } {
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
                    tracing::info!(
                        "🔄 etcd 租约自愈重连成功，重新注册实例: key={key} (lease: {lease_id})"
                    );
                }

                // 3. 建立心跳保活流（带超时保护）
                let keep_alive_fut = client.lease_keep_alive(lease_id);
                let (mut keeper, mut stream) =
                    match tokio::time::timeout(this.op_timeout(), keep_alive_fut).await {
                        Ok(Ok(pair)) => pair,
                        Ok(Err(err)) => {
                            tracing::warn!(
                                "⚠️ 建立 etcd lease_keep_alive 失败: {err}，2秒后尝试重新建连"
                            );
                            this.ready
                                .store(false, std::sync::atomic::Ordering::Release);
                            this.invalidate_client().await;
                            tokio::select! {
                                _ = token.cancelled() => break,
                                _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                            }
                        }
                        Err(_) => {
                            tracing::warn!("⚠️ 建立 etcd lease_keep_alive 超时，2秒后尝试重新建连");
                            this.ready
                                .store(false, std::sync::atomic::Ordering::Release);
                            this.invalidate_client().await;
                            tokio::select! {
                                _ = token.cancelled() => break,
                                _ = tokio::time::sleep(Duration::from_secs(2)) => continue,
                            }
                        }
                    };

                // 4. 心跳保活与异常监听循环
                let mut heartbeat = tokio::time::interval(heartbeat_interval);
                let mut last_ack = tokio::time::Instant::now();
                let mut connection_healthy = true;
                while connection_healthy {
                    tokio::select! {
                        _ = token.cancelled() => {
                            tracing::info!("收到注销信号，停止 etcd 租约心跳保活 (lease: {lease_id})");
                            return;
                        }
                        _ = heartbeat.tick() => {
                            if !matches!(tokio::time::timeout(this.op_timeout(), keeper.keep_alive()).await, Ok(Ok(()))) {
                                connection_healthy = false;
                            }
                        }
                        _ = tokio::time::sleep_until(last_ack + Duration::from_secs(ttl as u64)) => {
                            connection_healthy = false;
                        }
                        msg = stream.message() => {
                            match msg {
                                Ok(Some(reply)) => {
                                    if reply.ttl() <= 0 || reply.id() != lease_id {
                                        connection_healthy = false;
                                    } else { last_ack = tokio::time::Instant::now(); }
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
                this.ready
                    .store(false, std::sync::atomic::Ordering::Release);
                this.invalidate_client().await;
                tokio::select! {
                    _ = token.cancelled() => break,
                    _ = tokio::time::sleep(Duration::from_secs(2)) => {},
                }
            }
        });
        *self.task.lock().await = Some(task);

        // Preserve background recovery even when the caller sees an initial failure.
        match initial_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    /// 从 etcd 注销服务实例并释放租约（带操作超时保护）
    async fn deregister(&self) -> RegistryResult<()> {
        let _lifecycle = self.lifecycle.lock().await;
        self.ready
            .store(false, std::sync::atomic::Ordering::Release);
        if let Some(token) = self.cancel_token.lock().await.take() {
            token.cancel();
        }
        if let Some(task) = self.task.lock().await.take() {
            task.abort();
            let _ = task.await;
        }
        let lease = self.lease_id.lock().await.take();
        self.registered_key.lock().await.take();
        if let Some(id) = lease {
            self.authenticated(|mut client| async move { client.lease_revoke(id).await })
                .await?;
        }
        Ok(())
    }

    /// 服务发现：根据服务名称查询所有健康实例（带单次操作超时保护）
    async fn list_instances(&self, service_name: &str) -> RegistryResult<Vec<ServiceInstance>> {
        if !self.config.enable || self.config.host.trim().is_empty() {
            return Ok(Vec::new());
        }

        let prefix = self.make_service_prefix(service_name);
        let resp = self
            .authenticated(|mut client| {
                let prefix = prefix.clone();
                async move {
                    client
                        .get(prefix, Some(GetOptions::new().with_prefix()))
                        .await
                }
            })
            .await?;

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

        let prefix = self.make_service_prefix(service_name);
        let snapshot = self
            .authenticated(|mut client| {
                let prefix = prefix.clone();
                async move {
                    client
                        .get(prefix, Some(GetOptions::new().with_prefix()))
                        .await
                }
            })
            .await?;
        let revision = snapshot
            .header()
            .ok_or_else(|| RegistryError::driver("missing revision"))?
            .revision();
        let instances = snapshot
            .kvs()
            .iter()
            .filter_map(|kv| serde_json::from_slice(kv.value()).ok())
            .collect();
        let mut stream = self
            .authenticated(|mut client| {
                let prefix = prefix.clone();
                async move {
                    client
                        .watch(
                            prefix,
                            Some(
                                WatchOptions::new()
                                    .with_prefix()
                                    .with_start_revision(revision + 1),
                            ),
                        )
                        .await
                }
            })
            .await?;
        let watch_epoch = self.auth_epoch.load(std::sync::atomic::Ordering::Acquire);
        let this = self.clone();

        let svc_name = service_name.to_string();

        tokio::spawn(async move {
            if tx.send(ServiceEvent::Reset(instances)).await.is_err() {
                return;
            }
            loop {
                let message = tokio::select! {
                    _ = tx.closed() => break,
                    result = stream.message() => result,
                };
                match message {
                    Ok(Some(resp)) => {
                        if resp.canceled() || resp.compact_revision() > 0 {
                            break;
                        }
                        for event in resp.events() {
                            match event.event_type() {
                                EventType::Put => {
                                    if let Some(kv) = event.kv() {
                                        if let Ok(val_str) = kv.value_str() {
                                            if let Ok(inst) =
                                                serde_json::from_str::<ServiceInstance>(val_str)
                                            {
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
                        if Self::is_auth_expired(&err)
                            && this.config.user.is_some()
                            && this.config.password.is_some()
                        {
                            if let Err(error) = this.refresh_auth(watch_epoch).await {
                                tracing::warn!(%error, "etcd watch authentication refresh failed");
                            }
                        }
                        break;
                    }
                }
            }
        });

        Ok(Box::pin(ReceiverStream::new(rx)))
    }
}
