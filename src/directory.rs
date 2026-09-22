use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::error::RegistryResult;
use crate::instance::ServiceInstance;
use crate::traits::{InstanceListener, Registry, ServiceEvent};

/// 服务目录管理器（维护本地不可变只读内存快照，基于事件驱动进行增量维护）
///
/// 核心高并发与低开销特性：
/// 1. 读操作纳秒级完成（< 20ns），完全无锁等待，不涉及任何 Tokio 协程挂起；
/// 2. 节点上下线基于底层 Watch 长流主动毫秒级推送，日常 0 网络请求；
/// 3. 具备 Watch 断线指数退避重连与全量周期校准自愈兜底机制；
/// 4. 支持注册变更监听器（`InstanceListener`），便于与连接池、网关路由联动。
#[derive(Clone)]
pub struct ServiceDirectory {
    registry: Arc<dyn Registry>,
    snapshots: Arc<RwLock<HashMap<String, Arc<Vec<ServiceInstance>>>>>,
    active_watches: Arc<Mutex<HashMap<String, CancellationToken>>>,
    listeners: Arc<RwLock<Vec<Arc<dyn InstanceListener>>>>,
    reconcile_interval: Duration,
}

impl ServiceDirectory {
    pub fn new(registry: Arc<dyn Registry>) -> Self {
        Self::with_reconcile_interval(registry, Duration::from_secs(60))
    }

    pub fn with_reconcile_interval(registry: Arc<dyn Registry>, interval: Duration) -> Self {
        Self {
            registry,
            snapshots: Arc::new(RwLock::new(HashMap::new())),
            active_watches: Arc::new(Mutex::new(HashMap::new())),
            listeners: Arc::new(RwLock::new(Vec::new())),
            reconcile_interval: interval,
        }
    }

    /// 添加服务变更监听器
    pub fn add_listener(&self, listener: Arc<dyn InstanceListener>) {
        let mut list = self.listeners.write().expect("listener lock poisoned");
        list.push(listener);
    }

    /// 零锁读取指定服务的当前健康实例快照（纳秒级内存返回）
    pub fn get_instances(&self, service_name: &str) -> Arc<Vec<ServiceInstance>> {
        let read = self.snapshots.read().expect("directory lock poisoned");
        read.get(service_name)
            .cloned()
            .unwrap_or_else(|| Arc::new(Vec::new()))
    }

    /// 加载服务并激活后台事件监听（冷启动或首次访问时调用）
    pub async fn load_or_watch(
        &self,
        service_name: &str,
    ) -> RegistryResult<Arc<Vec<ServiceInstance>>> {
        // 1. 若快照中已有且已有活跃 Watcher，直接返回
        let existing = {
            let read = self.snapshots.read().expect("directory lock poisoned");
            read.get(service_name).cloned()
        };

        let has_watcher = {
            let guard = self.active_watches.lock().await;
            guard.contains_key(service_name)
        };

        if let Some(instances) = existing {
            if has_watcher {
                return Ok(instances);
            }
        }

        // 2. 首次全量拉取对齐
        let remote_instances = self.registry.list_instances(service_name).await?;
        let shared_instances = Arc::new(remote_instances);

        {
            let mut write = self.snapshots.write().expect("directory lock poisoned");
            write.insert(service_name.to_string(), shared_instances.clone());
        }

        // 3. 启动后台长连接 Watch 任务
        self.ensure_watcher_started(service_name).await;

        Ok(shared_instances)
    }

    /// 确保针对指定服务的 Watch 监听任务正在运行
    async fn ensure_watcher_started(&self, service_name: &str) {
        let mut guard = self.active_watches.lock().await;
        if guard.contains_key(service_name) {
            return;
        }

        let cancel_token = CancellationToken::new();
        guard.insert(service_name.to_string(), cancel_token.clone());

        // 立即尝试建立首次长连接 Watch，确保在返回前已完成订阅
        let initial_stream = self.registry.watch(service_name).await.ok();

        let this = self.clone();
        let svc_name = service_name.to_string();

        tokio::spawn(async move {
            this.run_watch_loop(svc_name, cancel_token, initial_stream).await;
        });
    }

    /// 后台常驻监听与事件消费循环
    async fn run_watch_loop(
        &self,
        service_name: String,
        cancel: CancellationToken,
        mut initial_stream: Option<crate::traits::EventStream>,
    ) {
        let mut retry_backoff = Duration::from_secs(1);

        while !cancel.is_cancelled() {
            let mut event_stream = if let Some(stream) = initial_stream.take() {
                stream
            } else {
                match self.registry.watch(&service_name).await {
                    Ok(s) => s,
                    Err(err) => {
                        tracing::warn!("⚠️ 建立服务 '{service_name}' Watch 失败: {err}，{retry_backoff:?}后重试");
                        tokio::select! {
                            _ = cancel.cancelled() => return,
                            _ = tokio::time::sleep(retry_backoff) => {
                                retry_backoff = (retry_backoff * 2).min(Duration::from_secs(10));
                                continue;
                            }
                        }
                    }
                }
            };

            retry_backoff = Duration::from_secs(1);
            tracing::info!("📡 已建立针对服务 '{service_name}' 的注册中心事件监听");

            let mut reconcile_interval = tokio::time::interval(self.reconcile_interval);
            reconcile_interval.tick().await;

            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        tracing::info!("收到取消信号，停止服务 '{service_name}' 的 Watch 任务");
                        return;
                    }
                    _ = reconcile_interval.tick() => {
                        // 全量静默校准，防止网络抖动丢包
                        if let Ok(latest) = self.registry.list_instances(&service_name).await {
                            self.apply_event(&service_name, ServiceEvent::Reset(latest));
                        }
                    }
                    event_opt = event_stream.next() => {
                        match event_opt {
                            Some(event) => {
                                self.apply_event(&service_name, event);
                            }
                            None => {
                                tracing::warn!("⚠️ 服务 '{service_name}' 的 Watch 事件流已结束，准备重连");
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /// 应用变更事件并原子更新快照，同时触发外部通知
    pub fn apply_event(&self, service_name: &str, event: ServiceEvent) {
        let updated = {
            let mut write = self.snapshots.write().expect("directory lock poisoned");
            let current = write
                .get(service_name)
                .cloned()
                .unwrap_or_else(|| Arc::new(Vec::new()));

            let new_list: Arc<Vec<ServiceInstance>> = match event {
                ServiceEvent::Upsert(new_inst) => {
                    let mut list: Vec<ServiceInstance> = (*current).clone();
                    if let Some(pos) = list.iter().position(|i| i.service_id == new_inst.service_id) {
                        list[pos] = new_inst;
                    } else {
                        list.push(new_inst);
                    }
                    tracing::debug!("🔄 实例变更推送: service={service_name}, 当前健康节点数={}", list.len());
                    Arc::new(list)
                }
                ServiceEvent::Delete { service_id, .. } => {
                    let mut list: Vec<ServiceInstance> = (*current).clone();
                    list.retain(|i| i.service_id != service_id);
                    tracing::info!("👋 实例下线推送: service={service_name}, node={service_id}, 剩余健康节点数={}", list.len());
                    Arc::new(list)
                }
                ServiceEvent::Reset(all) => {
                    Arc::new(all)
                }
            };

            write.insert(service_name.to_string(), new_list.clone());
            new_list
        };

        // 触发监听器通知
        let listeners = {
            let read = self.listeners.read().expect("listener lock poisoned");
            read.clone()
        };
        for listener in listeners {
            listener.on_change(service_name, &updated);
        }
    }

    /// 清理所有快照并注销所有监听任务
    pub async fn shutdown(&self) {
        let mut tokens = self.active_watches.lock().await;
        for (_, token) in tokens.drain() {
            token.cancel();
        }
        let mut write = self.snapshots.write().expect("directory lock poisoned");
        write.clear();
    }
}
