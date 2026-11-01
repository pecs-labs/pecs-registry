#![cfg(feature = "tower")]
//! Tower & Tonic 服务发现抽象集成模块
//!
//! 提供：
//! - `ServiceDiscover`：实现 `futures_core::Stream`，由 Tower 自动适配为 `tower::discover::Discover`，与 Tower 负载均衡生态无缝集成；
//! - 与 `tonic::transport::Channel::balance_channel` 深度集成，实现开箱即用的 gRPC 客户端动态服务发现与自愈连接池。

use crate::directory::ServiceDirectory;
use crate::error::RegistryResult;
use crate::instance::ServiceInstance;
use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::sync::mpsc::Receiver;
use tokio_stream::StreamExt;

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

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
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
        // Subscribe before loading: changes during initialization are retained.
        let mut updates = self.subscribe_updates();
        self.load_or_watch(service_name).await?;
        let directory = self.clone();
        let name = service_name.to_owned();
        tokio::spawn(async move {
            let mut known = std::collections::HashMap::<String, ServiceInstance>::new();
            loop {
                let current = directory.get_instances(&name);
                let next: std::collections::HashMap<_, _> = current
                    .iter()
                    .map(|i| (i.service_id.clone(), i.clone()))
                    .collect();
                let mut changes = Vec::new();
                for id in known.keys() {
                    if !next.contains_key(id) {
                        changes.push(tower::discover::Change::Remove(id.clone()));
                    }
                }
                for (id, instance) in &next {
                    if known.get(id) != Some(instance) {
                        changes.push(tower::discover::Change::Insert(
                            id.clone(),
                            instance.clone(),
                        ));
                    }
                }
                for change in changes {
                    if tx.send(change).await.is_err() {
                        return;
                    }
                }
                known = next;
                tokio::select! {
                    _ = tx.closed() => return,
                    result = updates.changed() => if result.is_err() { return; },
                }
            }
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
        let mut stream = self.discover_stream(service_name).await?;
        tokio::spawn(async move {
            while let Some(Ok(change)) = tokio::select! {
                _ = tx.closed() => return,
                change = stream.next() => change,
            } {
                let change = match change {
                    tower::discover::Change::Remove(id) => {
                        tonic::transport::channel::Change::Remove(id)
                    }
                    tower::discover::Change::Insert(id, instance) => {
                        match instance.grpc.and_then(|grpc| {
                            tonic::transport::Endpoint::from_shared(grpc.to_url("http")).ok()
                        }) {
                            Some(endpoint) => {
                                tonic::transport::channel::Change::Insert(id, configure(endpoint))
                            }
                            None => tonic::transport::channel::Change::Remove(id),
                        }
                    }
                };
                if tx.send(change).await.is_err() {
                    return;
                }
            }
        });
        Ok(channel)
    }
}
