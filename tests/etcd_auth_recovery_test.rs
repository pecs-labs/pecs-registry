#![cfg(feature = "etcd")]
//! Local official-protocol mock; no credentials or writes to a real etcd server.
use etcd_client::proto::*;
use pecs_registry::{
    provider::etcd::EtcdRegistry, Endpoint, Registry, RegistryConfig, ServiceEvent, ServiceInstance,
};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::sync::{mpsc, Mutex};
use tokio_stream::{
    wrappers::{ReceiverStream, TcpListenerStream},
    StreamExt,
};
use tonic::{Request, Response, Status};

#[derive(Default)]
struct State {
    auth_calls: AtomicUsize,
    deleted: AtomicBool,
    fail_delete: AtomicBool,
    revoke_calls: AtomicUsize,
    range_calls: AtomicUsize,
    minimum_token: AtomicUsize,
    deny: AtomicBool,
    always_expired: AtomicBool,
    reject_auth: AtomicBool,
    expire_on_watch: AtomicBool,
    watchers: Mutex<Vec<mpsc::Sender<Result<PbWatchResponse, Status>>>>,
}
#[derive(Clone)]
struct Mock(Arc<State>);
impl Mock {
    fn authorize<T>(&self, request: &Request<T>) -> Result<(), Status> {
        if self.0.deny.load(Ordering::SeqCst) {
            return Err(Status::permission_denied("not allowed"));
        }
        let version = request
            .metadata()
            .get("token")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.strip_prefix("token-"))
            .and_then(|v| v.parse::<usize>().ok())
            .unwrap_or(0);
        if self.0.always_expired.load(Ordering::SeqCst)
            || version < self.0.minimum_token.load(Ordering::SeqCst)
        {
            return Err(Status::unauthenticated("etcdserver: invalid auth token"));
        }
        Ok(())
    }
}
#[tonic::async_trait]
impl PbAuthService for Mock {
    async fn authenticate(
        &self,
        request: Request<PbAuthenticateRequest>,
    ) -> Result<Response<PbAuthenticateResponse>, Status> {
        let version = self.0.auth_calls.fetch_add(1, Ordering::SeqCst) + 1;
        tokio::time::sleep(std::time::Duration::from_millis(30)).await;
        if self.0.reject_auth.load(Ordering::SeqCst)
            || request.get_ref().name != "test-user"
            || request.get_ref().password != "test-password"
        {
            return Err(Status::unauthenticated("authentication failed"));
        }
        Ok(Response::new(PbAuthenticateResponse {
            token: format!("token-{version}"),
            ..Default::default()
        }))
    }
    async fn auth_enable(
        &self,
        _: Request<PbAuthEnableRequest>,
    ) -> Result<Response<PbAuthEnableResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn auth_disable(
        &self,
        _: Request<PbAuthDisableRequest>,
    ) -> Result<Response<PbAuthDisableResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_add(
        &self,
        _: Request<PbAuthUserAddRequest>,
    ) -> Result<Response<PbAuthUserAddResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_get(
        &self,
        _: Request<PbAuthUserGetRequest>,
    ) -> Result<Response<PbAuthUserGetResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_list(
        &self,
        _: Request<PbAuthUserListRequest>,
    ) -> Result<Response<PbAuthUserListResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_delete(
        &self,
        _: Request<PbAuthUserDeleteRequest>,
    ) -> Result<Response<PbAuthUserDeleteResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_change_password(
        &self,
        _: Request<PbAuthUserChangePasswordRequest>,
    ) -> Result<Response<PbAuthUserChangePasswordResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_grant_role(
        &self,
        _: Request<PbAuthUserGrantRoleRequest>,
    ) -> Result<Response<PbAuthUserGrantRoleResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn user_revoke_role(
        &self,
        _: Request<PbAuthUserRevokeRoleRequest>,
    ) -> Result<Response<PbAuthUserRevokeRoleResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn role_add(
        &self,
        _: Request<PbAuthRoleAddRequest>,
    ) -> Result<Response<PbAuthRoleAddResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn role_get(
        &self,
        _: Request<PbAuthRoleGetRequest>,
    ) -> Result<Response<PbAuthRoleGetResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn role_list(
        &self,
        _: Request<PbAuthRoleListRequest>,
    ) -> Result<Response<PbAuthRoleListResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn role_delete(
        &self,
        _: Request<PbAuthRoleDeleteRequest>,
    ) -> Result<Response<PbAuthRoleDeleteResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn role_grant_permission(
        &self,
        _: Request<PbAuthRoleGrantPermissionRequest>,
    ) -> Result<Response<PbAuthRoleGrantPermissionResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn role_revoke_permission(
        &self,
        _: Request<PbAuthRoleRevokePermissionRequest>,
    ) -> Result<Response<PbAuthRoleRevokePermissionResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
}
#[tonic::async_trait]
impl PbKvService for Mock {
    async fn range(
        &self,
        request: Request<PbRangeRequest>,
    ) -> Result<Response<PbRangeResponse>, Status> {
        self.0.range_calls.fetch_add(1, Ordering::SeqCst);
        self.authorize(&request)?;
        if self.0.deleted.load(Ordering::SeqCst) {
            return Ok(Response::new(PbRangeResponse {
                header: Some(PbResponseHeader {
                    revision: 2,
                    ..Default::default()
                }),
                ..Default::default()
            }));
        }
        let instance = ServiceInstance::new_http(
            "node-1",
            "test",
            "test",
            "service",
            Endpoint::new("127.0.0.1", 8001),
        );
        Ok(Response::new(PbRangeResponse {
            header: Some(PbResponseHeader {
                revision: 1,
                ..Default::default()
            }),
            kvs: vec![PbKeyValue {
                key: b"/test/test/service/node-1".to_vec(),
                value: serde_json::to_vec(&instance).unwrap(),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }
    async fn put(&self, request: Request<PbPutRequest>) -> Result<Response<PbPutResponse>, Status> {
        self.authorize(&request)?;
        self.0.deleted.store(false, Ordering::SeqCst);
        Ok(Response::new(PbPutResponse::default()))
    }
    async fn delete_range(
        &self,
        request: Request<PbDeleteRequest>,
    ) -> Result<Response<PbDeleteResponse>, Status> {
        self.authorize(&request)?;
        if self.0.fail_delete.load(Ordering::SeqCst) {
            return Err(Status::unavailable("mock delete failure"));
        }
        assert_eq!(request.get_ref().key, b"/test/test/service/node-1");
        assert!(
            request.get_ref().range_end.is_empty(),
            "must only delete the owned key"
        );
        self.0.deleted.store(true, Ordering::SeqCst);
        Ok(Response::new(PbDeleteResponse {
            deleted: 1,
            ..Default::default()
        }))
    }
    async fn txn(&self, _: Request<PbTxnRequest>) -> Result<Response<PbTxnResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn compact(
        &self,
        _: Request<PbCompactionRequest>,
    ) -> Result<Response<PbCompactionResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
}
#[tonic::async_trait]
impl PbWatchService for Mock {
    type WatchStream = ReceiverStream<Result<PbWatchResponse, Status>>;
    async fn watch(
        &self,
        request: Request<tonic::Streaming<PbWatchRequest>>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        self.authorize(&request)?;
        if self.0.expire_on_watch.swap(false, Ordering::SeqCst) {
            self.0.minimum_token.fetch_add(1, Ordering::SeqCst);
            return Err(Status::unauthenticated("etcdserver: invalid auth token"));
        }
        let mut incoming = request.into_inner();
        let create = incoming
            .message()
            .await?
            .ok_or_else(|| Status::invalid_argument("missing create"))?;
        assert!(matches!(
            create.request_union,
            Some(PbWatchRequestUnion::CreateRequest(_))
        ));
        let (tx, rx) = mpsc::channel(8);
        tx.send(Ok(PbWatchResponse {
            watch_id: 1,
            created: true,
            header: Some(PbResponseHeader {
                revision: 1,
                ..Default::default()
            }),
            ..Default::default()
        }))
        .await
        .unwrap();
        self.0.watchers.lock().await.push(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }
}

#[tonic::async_trait]
impl PbLeaseService for Mock {
    type LeaseKeepAliveStream = ReceiverStream<Result<PbLeaseKeepAliveResponse, Status>>;
    async fn lease_grant(
        &self,
        request: Request<PbLeaseGrantRequest>,
    ) -> Result<Response<PbLeaseGrantResponse>, Status> {
        Ok(Response::new(PbLeaseGrantResponse {
            id: 42,
            ttl: request.get_ref().ttl,
            ..Default::default()
        }))
    }
    async fn lease_revoke(
        &self,
        _: Request<PbLeaseRevokeRequest>,
    ) -> Result<Response<PbLeaseRevokeResponse>, Status> {
        assert!(
            self.0.deleted.load(Ordering::SeqCst),
            "must delete the registration key first"
        );
        self.0.revoke_calls.fetch_add(1, Ordering::SeqCst);
        Err(Status::invalid_argument("etcdserver: user name is empty"))
    }
    async fn lease_keep_alive(
        &self,
        _: Request<tonic::Streaming<PbLeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn lease_time_to_live(
        &self,
        _: Request<PbLeaseTimeToLiveRequest>,
    ) -> Result<Response<PbLeaseTimeToLiveResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
    async fn lease_leases(
        &self,
        _: Request<PbLeaseLeasesRequest>,
    ) -> Result<Response<PbLeaseLeasesResponse>, Status> {
        Err(Status::unimplemented("unused mock RPC"))
    }
}

async fn start() -> (EtcdRegistry, Arc<State>, tokio::task::JoinHandle<()>) {
    let state = Arc::new(State::default());
    state.minimum_token.store(1, Ordering::SeqCst);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let mock = Mock(state.clone());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(PbAuthServer::new(mock.clone()))
            .add_service(PbKvServer::new(mock.clone()))
            .add_service(PbWatchServer::new(mock.clone()))
            .add_service(PbLeaseServer::new(mock))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    let registry = EtcdRegistry::new(RegistryConfig {
        host: "127.0.0.1".into(),
        port,
        namespace: "test".into(),
        group: "test".into(),
        timeout: 2,
        user: Some("test-user".into()),
        password: Some("test-password".into()),
        ..Default::default()
    });
    (registry, state, task)
}

#[tokio::test]
async fn concurrent_queries_refresh_once_and_do_not_retry_permission_errors() {
    let (registry, state, task) = start().await;
    assert_eq!(
        registry.list_instances("service").await.unwrap()[0].service_id,
        "node-1"
    );
    state.minimum_token.store(2, Ordering::SeqCst);
    let mut calls = Vec::new();
    let barrier = Arc::new(tokio::sync::Barrier::new(12));
    for _ in 0..12 {
        let registry = registry.clone();
        let barrier = barrier.clone();
        calls.push(tokio::spawn(async move {
            barrier.wait().await;
            registry.list_instances("service").await
        }));
    }
    for call in calls {
        assert_eq!(call.await.unwrap().unwrap()[0].service_id, "node-1");
    }
    assert_eq!(state.auth_calls.load(Ordering::SeqCst), 2);
    state.deny.store(true, Ordering::SeqCst);
    assert!(registry.list_instances("service").await.is_err());
    assert_eq!(state.auth_calls.load(Ordering::SeqCst), 2);
    state.deny.store(false, Ordering::SeqCst);
    state.always_expired.store(true, Ordering::SeqCst);
    assert!(registry.list_instances("service").await.is_err());
    assert_eq!(
        state.auth_calls.load(Ordering::SeqCst),
        3,
        "retries must be bounded"
    );
    task.abort();
}

#[tokio::test]
async fn watch_snapshot_creation_and_stream_expiry_recover_authentication() {
    let (registry, state, task) = start().await;
    registry.list_instances("service").await.unwrap();
    state.minimum_token.store(2, Ordering::SeqCst);
    state.expire_on_watch.store(true, Ordering::SeqCst);
    let mut stream = registry.watch("service").await.unwrap();
    assert!(
        matches!(stream.next().await, Some(ServiceEvent::Reset(instances)) if instances[0].service_id == "node-1")
    );
    assert_eq!(state.auth_calls.load(Ordering::SeqCst), 3);
    state.minimum_token.store(4, Ordering::SeqCst);
    state
        .watchers
        .lock()
        .await
        .last()
        .unwrap()
        .send(Err(Status::unauthenticated(
            "etcdserver: invalid auth token",
        )))
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(std::time::Duration::from_secs(3), stream.next())
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(state.auth_calls.load(Ordering::SeqCst), 4);
    let mut replacement = registry.watch("service").await.unwrap();
    assert!(matches!(
        replacement.next().await,
        Some(ServiceEvent::Reset(_))
    ));
    assert_eq!(state.auth_calls.load(Ordering::SeqCst), 4);
    task.abort();
}

#[tokio::test]
async fn failed_authentication_is_returned_without_replaying_query() {
    let (registry, state, task) = start().await;
    registry.list_instances("service").await.unwrap();
    state.minimum_token.store(2, Ordering::SeqCst);
    state.reject_auth.store(true, Ordering::SeqCst);
    assert!(registry.list_instances("service").await.is_err());
    assert_eq!(state.auth_calls.load(Ordering::SeqCst), 2);
    assert_eq!(state.range_calls.load(Ordering::SeqCst), 2);
    task.abort();
}

#[tokio::test]
async fn deregistration_removes_address_even_if_lease_revoke_rejects_auth() {
    let (registry, state, task) = start().await;
    let instance = ServiceInstance::new_http(
        "node-1",
        "test",
        "test",
        "service",
        Endpoint::new("127.0.0.1", 8001),
    );
    registry.register(&instance).await.unwrap();
    state.fail_delete.store(true, Ordering::SeqCst);
    assert!(registry.deregister().await.is_err());
    assert!(!state.deleted.load(Ordering::SeqCst));
    state.fail_delete.store(false, Ordering::SeqCst);
    registry.deregister().await.unwrap();
    assert!(registry.list_instances("service").await.unwrap().is_empty());
    assert_eq!(state.revoke_calls.load(Ordering::SeqCst), 1);
    registry.deregister().await.unwrap();
    assert_eq!(state.revoke_calls.load(Ordering::SeqCst), 1);
    task.abort();
}
