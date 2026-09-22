//! Tower & Tonic 服务发现抽象集成模块
//!
//! 提供：
//! - `ServiceDiscover`：实现 `futures_core::Stream`，由 Tower 自动适配为 `tower::discover::Discover`，与 Tower 负载均衡生态无缝集成；
//! - 与 `tonic::transport::Channel::balance_channel` 深度集成，实现开箱即用的 gRPC 客户端动态服务发现与自愈连接池。

#[cfg(feature = "tower")]
use std::collections::HashSet;
#[cfg(feature = "tower")]
use std::convert::Infallible;
#[cfg(feature = "tower")]
use std::pin::Pin;
#[cfg(feature = "tower")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "tower")]
use std::task::{Context, Poll};
#[cfg(feature = "tower")]
use tokio::sync::mpsc::Receiver;

#[cfg(feature = "tower")]
use crate::directory::ServiceDirectory;
#[cfg(feature = "tower")]
use crate::error::RegistryResult;
#[cfg(feature = "tower")]
use crate::instance::ServiceInstance;

/// 实现 `futures_core::Stream` 的服务发现事件流
///
/// Tower 为所有 `TryStream<Ok = tower::discover::Change<K, S>>` 提供了 `Discover` 特质的盲插实现。
#[cfg(feature = "tower")]
pub struct ServiceDiscover {
    rx: Receiver<tower::discover::Change<String, ServiceInstance>>,
}

#[cfg(feature = "tower")]
impl futures_core::Stream for ServiceDiscover {
    type Item = Result<tower::discover::Change<String, ServiceInstance>, Infallible>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match self.rx.poll_recv(cx) {
            Poll::Ready(Some(change)) => Poll::Ready(Some(Ok(change))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(feature = "tower")]
impl ServiceDirectory {
    /// 创建针对指定微服务的 Tower Discover 事件流
    pub async fn discover_stream(&self, service_name: &str) -> RegistryResult<ServiceDiscover> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);

        // 1. 确保已拉取初始快照并激活 Watch
        let initial_instances = self.load_or_watch(service_name).await?;

        // 2. 发送初始存量实例的 Insert 事件
        for inst in initial_instances.iter() {
            let _ = tx
                .send(tower::discover::Change::Insert(
                    inst.service_id.clone(),
                    inst.clone(),
                ))
                .await;
        }

        // 3. 注册监听器监听后续增量事件
        let svc_name = service_name.to_string();
        let known_ids = Arc::new(Mutex::new(
            initial_instances
                .iter()
                .map(|i| i.service_id.clone())
                .collect::<HashSet<_>>(),
        ));

        self.on_change(move |name, instances| {
            if name != svc_name {
                return;
            }
            let mut known = known_ids.lock().unwrap();
            let new_set: HashSet<String> =
                instances.iter().map(|i| i.service_id.clone()).collect();

            // 检查被移除的实例
            for old_id in known.iter() {
                if !new_set.contains(old_id) {
                    let _ = tx.try_send(tower::discover::Change::Remove(old_id.clone()));
                }
            }

            // 检查新增的实例
            for inst in instances {
                if !known.contains(&inst.service_id) {
                    let _ = tx.try_send(tower::discover::Change::Insert(
                        inst.service_id.clone(),
                        inst.clone(),
                    ));
                }
            }

            *known = new_set;
        });

        Ok(ServiceDiscover { rx })
    }
}

#[cfg(feature = "tonic")]
impl ServiceDirectory {
    /// 创建开箱即用的 Tonic 动态负载均衡 Channel
    ///
    /// 核心能力：
    /// 1. 自动连接注册中心并监听节点上下线；
    /// 2. 节点上线时自动向 Tonic 提交 `Change::Insert`；
    /// 3. 节点下线时自动向 Tonic 提交 `Change::Remove`；
    /// 4. Tonic 底层自动执行动态连接池维护、长连接保活与请求级负载均衡。
    pub async fn tonic_channel(
        &self,
        service_name: &str,
    ) -> RegistryResult<tonic::transport::Channel> {
        self.tonic_channel_with_config(service_name, |ep| ep).await
    }

    /// 创建带自定义 Endpoint 配置（如超时、重试、并发限制等）的 Tonic 动态负载均衡 Channel
    pub async fn tonic_channel_with_config<F>(
        &self,
        service_name: &str,
        configure: F,
    ) -> RegistryResult<tonic::transport::Channel>
    where
        F: Fn(tonic::transport::Endpoint) -> tonic::transport::Endpoint + Send + Sync + 'static,
    {
        let (channel, tx) = tonic::transport::Channel::balance_channel(64);

        // 1. 确保已拉取初始快照并激活 Watch
        let initial_instances = self.load_or_watch(service_name).await?;

        // 2. 转换并注入初始存量 gRPC 实例
        let known_ids = Arc::new(Mutex::new(HashSet::new()));

        for inst in initial_instances.iter() {
            if let Some(ref grpc) = inst.grpc {
                let url = grpc.to_url("http");
                if let Ok(ep) = tonic::transport::Endpoint::from_shared(url) {
                    let ep = configure(ep);
                    let _ = tx
                        .send(tonic::transport::channel::Change::Insert(
                            inst.service_id.clone(),
                            ep,
                        ))
                        .await;
                    known_ids.lock().unwrap().insert(inst.service_id.clone());
                }
            }
        }

        // 3. 注册监听器动态联动后续变更
        let svc_name = service_name.to_string();
        let configure = Arc::new(configure);

        self.on_change(move |name, instances| {
            if name != svc_name {
                return;
            }
            let mut known = known_ids.lock().unwrap();
            let mut new_set = HashSet::new();

            for inst in instances {
                if let Some(ref grpc) = inst.grpc {
                    new_set.insert(inst.service_id.clone());
                    if !known.contains(&inst.service_id) {
                        let url = grpc.to_url("http");
                        if let Ok(ep) = tonic::transport::Endpoint::from_shared(url) {
                            let ep = configure(ep);
                            let _ = tx.try_send(tonic::transport::channel::Change::Insert(
                                inst.service_id.clone(),
                                ep,
                            ));
                        }
                    }
                }
            }

            for old_id in known.iter() {
                if !new_set.contains(old_id) {
                    let _ = tx.try_send(tonic::transport::channel::Change::Remove(old_id.clone()));
                }
            }

            *known = new_set;
        });

        Ok(channel)
    }
}
