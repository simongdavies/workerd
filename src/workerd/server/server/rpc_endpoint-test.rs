use std::cell::Cell;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;

use kj_rs::KjMaybe;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::io::ReadBuf;
use workerd_capnp::config;

use super::*;

type ConnectionRequests = Rc<RefCell<Vec<(Designator, Option<String>)>>>;

struct Refused;

impl OwnedWorkerConnector for Refused {
    fn connect<'a>(
        &'a self,
        _target: &'a Designator,
        _metadata: &'a RequestMetadata,
    ) -> LocalBoxFuture<'a, Result<KjOwn<AsyncIoStream>>> {
        async { Err(kj::failed!("fixture admission refused")) }.boxed_local()
    }
}

struct CountedRefusal(Rc<Cell<usize>>);

impl OwnedWorkerConnector for CountedRefusal {
    fn connect<'a>(
        &'a self,
        _target: &'a Designator,
        _metadata: &'a RequestMetadata,
    ) -> LocalBoxFuture<'a, Result<KjOwn<AsyncIoStream>>> {
        async move {
            self.0.set(self.0.get() + 1);
            Err(kj::failed!("fixture admission refused"))
        }
        .boxed_local()
    }
}

fn target() -> Designator {
    Designator {
        service: "remote".to_owned(),
        entrypoint: Some("Admin".to_owned()),
        props_json: Some("{\"role\":\"reader\"}".to_owned()),
        error_context: "unscoped RPC fixture".to_owned(),
    }
}

#[test]
fn endpoint_refuses_unbound_and_retired_resolution_without_connecting() {
    let endpoint = UnscopedRpcWorkerEndpoint::new(Rc::new(Refused));
    assert!(endpoint.resolve(&target()).is_err());
    endpoint.unlink().now_or_never().unwrap().unwrap();
    assert!(endpoint.resolve(&target()).is_err());
}

struct Recorded {
    events: Rc<RefCell<Vec<String>>>,
    dropped: Rc<Cell<usize>>,
}

impl Drop for Recorded {
    fn drop(&mut self) {
        self.dropped.set(self.dropped.get() + 1);
    }
}

#[async_trait::async_trait(?Send)]
impl Service for Recorded {
    async fn request<'a>(
        &'a mut self,
        _method: Method,
        _url: &'a [u8],
        headers: HeadersRef<'a>,
        mut body: Pin<&'a mut AsyncInputStream>,
        response: ServiceResponse<'a>,
    ) -> Result<()> {
        if headers.get(kj::http::HeaderId::UPGRADE).is_some() {
            let socket = response.accept_websocket(headers)?;
            self.events.borrow_mut().push("websocket".to_owned());
            std::future::pending::<()>().await;
            drop(socket);
            return Ok(());
        }
        self.events.borrow_mut().push("http".to_owned());
        let headers = headers.clone_shallow();
        let length = body.as_mut().try_get_length();
        let mut output = response.send(200, "OK", &headers, length)?;
        let mut buffer = [0; 257];
        loop {
            let amount = body.as_mut().try_read(&mut buffer, 1).await?;
            if amount == 0 {
                return Ok(());
            }
            output.write(&buffer[..amount]).await?;
        }
    }

    async fn connect<'a>(
        &'a mut self,
        _host: &'a [u8],
        headers: HeadersRef<'a>,
        connection: Pin<&'a mut AsyncIoStream>,
        response: ConnectResponse<'a>,
        _settings: ConnectSettings<'a>,
    ) -> Result<()> {
        response.accept(200, "OK", headers)?;
        let (echo, transport) = tokio::io::duplex(128);
        let echoing = async move {
            let (mut read, mut write) = tokio::io::split(echo);
            tokio::io::copy(&mut read, &mut write).await.unwrap();
            write.shutdown().await.unwrap();
        };
        let pumping = kj_hyper::ffi::pump_tunnel(connection, kj_hyper::into_kj_stream(transport));
        let (result, ()) = futures::join!(pumping, echoing);
        Ok(result?)
    }
}

#[async_trait::async_trait(?Send)]
impl Interface for Recorded {
    async fn prewarm(&mut self, url: &str) -> Result<()> {
        self.events.borrow_mut().push(format!("prewarm:{url}"));
        Ok(())
    }

    async fn run_scheduled(&mut self, time: &SystemTime, cron: &str) -> Result<ScheduledResult> {
        assert_eq!(*time, SystemTime::UNIX_EPOCH + Duration::from_secs(123));
        self.events.borrow_mut().push(format!("scheduled:{cron}"));
        Ok(ScheduledResult {
            retry: true,
            outcome: outcome_capnp::EventOutcome::Exception,
        })
    }

    async fn run_alarm(&mut self, time: &SystemTime, retry: u32) -> Result<AlarmResult> {
        assert_eq!(
            *time,
            SystemTime::UNIX_EPOCH + Duration::from_millis(123_456)
        );
        self.events.borrow_mut().push(format!("alarm:{retry}"));
        Ok(AlarmResult {
            retry: true,
            retry_counts_against_limit: false,
            outcome: outcome_capnp::EventOutcome::Exception,
            error_description: Some("fixture alarm detail".to_owned()),
        })
    }

    async fn abandon_alarm(&mut self, _time: &SystemTime) -> Result<Option<SystemTime>> {
        self.events.borrow_mut().push("abandon".to_owned());
        Ok(Some(SystemTime::UNIX_EPOCH + Duration::from_secs(987)))
    }

    async fn test(&mut self) -> Result<bool> {
        self.events
            .borrow_mut()
            .push("local-wrapper-test".to_owned());
        Ok(true)
    }
}

fn recorded_request(
    endpoint: &UnscopedRpcWorkerEndpoint,
    events: Rc<RefCell<Vec<String>>>,
    dropped: Rc<Cell<usize>>,
) -> RpcRequest {
    let target = Recorded { events, dropped }.into_kj();
    let request = Rc::new(Request {
        target: Mutex::new(Some(PromisedInterface::new(async { Ok(target) }))),
        start: RefCell::new(None),
        producer_finished: Rc::new(Cell::new(true)),
    });
    endpoint
        .0
        .requests
        .borrow_mut()
        .push(Rc::downgrade(&request));
    RpcRequest {
        endpoint: Rc::clone(&endpoint.0),
        request,
    }
}

struct TrackedIo {
    io: tokio::io::DuplexStream,
    drops: Rc<Cell<usize>>,
    clearing: Rc<Cell<bool>>,
    on_drop: Option<Rc<Notify>>,
}

impl Drop for TrackedIo {
    fn drop(&mut self) {
        assert!(
            !self.clearing.get(),
            "owned client closed inside TaskSet.clear"
        );
        self.drops.set(self.drops.get() + 1);
        if let Some(notify) = &self.on_drop {
            notify.notify_one();
        }
    }
}

impl AsyncRead for TrackedIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(context, buffer)
    }
}

impl AsyncWrite for TrackedIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(context, bytes)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(context)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(context)
    }
}

struct Peer {
    events: Rc<RefCell<Vec<String>>>,
    dropped: Rc<Cell<usize>>,
    blobs: Rc<RefCell<Vec<Option<String>>>>,
}

impl Channel for Peer {
    fn start_request(&self, metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        self.blobs
            .borrow_mut()
            .push(ffi::request_metadata_cf_blob_json(&metadata).into());
        Ok(Recorded {
            events: Rc::clone(&self.events),
            dropped: Rc::clone(&self.dropped),
        }
        .into_kj())
    }

    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(kj::failed!("fixture peer is not transferable"))
    }
}

struct Connected {
    factory: Weak<Factory>,
    target: Rc<dyn Channel>,
    requests: ConnectionRequests,
    drops: Rc<Cell<usize>>,
    clearing: Rc<Cell<bool>>,
}

struct Delayed {
    gate: RefCell<Option<tokio::sync::oneshot::Receiver<()>>>,
    calls: Rc<Cell<usize>>,
    drops: Rc<Cell<usize>>,
    clearing: Rc<Cell<bool>>,
    started: Rc<Notify>,
    closed: Rc<Notify>,
}

impl OwnedWorkerConnector for Delayed {
    fn connect<'a>(
        &'a self,
        _target: &'a Designator,
        _metadata: &'a RequestMetadata,
    ) -> LocalBoxFuture<'a, Result<KjOwn<AsyncIoStream>>> {
        async move {
            self.calls.set(self.calls.get() + 1);
            self.started.notify_one();
            let gate = self.gate.borrow_mut().take().unwrap();
            gate.await
                .map_err(|_| kj::failed!("fixture gate was lost"))?;
            let (io, _peer) = tokio::io::duplex(128);
            Ok(kj_hyper::into_kj_stream(TrackedIo {
                io,
                drops: Rc::clone(&self.drops),
                clearing: Rc::clone(&self.clearing),
                on_drop: Some(Rc::clone(&self.closed)),
            }))
        }
        .boxed_local()
    }
}

async fn late_admission_is_closed_before_rpc_publication(factory: &Rc<Factory>) {
    let (release, gate) = tokio::sync::oneshot::channel();
    let calls = Rc::new(Cell::new(0));
    let drops = Rc::new(Cell::new(0));
    let started = Rc::new(Notify::new());
    let closed = Rc::new(Notify::new());
    let endpoint = Rc::new(UnscopedRpcWorkerEndpoint::new(Rc::new(Delayed {
        gate: RefCell::new(Some(gate)),
        calls: Rc::clone(&calls),
        drops: Rc::clone(&drops),
        clearing: Rc::new(Cell::new(false)),
        started: Rc::clone(&started),
        closed: Rc::clone(&closed),
    })));
    endpoint.bind_factory(factory).unwrap();
    factory.owned_endpoints.borrow_mut().push(endpoint.clone());
    let channel = endpoint.resolve(&target()).unwrap();
    let mut worker = worker::CxxWorkerInterface::new(
        channel
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    assert_eq!(calls.get(), 0, "connector ran during start_request");
    factory.sleep(Duration::ZERO).await;
    assert_eq!(calls.get(), 0, "unused interface started admission");
    let mut operation = Box::pin(worker.prewarm("/not-published"));
    assert!(futures::poll!(operation.as_mut()).is_pending());
    started.notified().await;
    assert_eq!(calls.get(), 1);
    let finishing = endpoint.unlink();
    release.send(()).unwrap();
    closed.notified().await;
    assert_eq!(drops.get(), 1, "late admitted stream escaped the fence");
    let (finished, result) = futures::join!(finishing, operation);
    finished.unwrap();
    assert!(result.unwrap_err().description().contains("retired"));
    drop(worker);
    factory.settle_tasks().await.unwrap();
    assert_eq!(drops.get(), 1);
}

impl OwnedWorkerConnector for Connected {
    fn connect<'a>(
        &'a self,
        target: &'a Designator,
        metadata: &'a RequestMetadata,
    ) -> LocalBoxFuture<'a, Result<KjOwn<AsyncIoStream>>> {
        async move {
            self.requests.borrow_mut().push((
                target.clone(),
                ffi::request_metadata_cf_blob_json(metadata).into(),
            ));
            let factory = self.factory.upgrade().unwrap();
            let (client, peer) = tokio::io::duplex(128);
            let stream = kj_hyper::into_kj_stream(TrackedIo {
                io: client,
                drops: Rc::clone(&self.drops),
                clearing: Rc::clone(&self.clearing),
                on_drop: None,
            });
            let peer = kj_hyper::into_kj_stream(peer);
            let target = crate::channels::SubrequestChannel::new(Rc::clone(&self.target));
            let server_factory = Rc::clone(&factory);
            factory.spawn_detached(async move {
                if let Err(error) =
                    ffi::factory_accept_bootstrap(server_factory.raw(), peer, target).await
                {
                    eprintln!("fixture RPC peer ended: {error:?}");
                }
            });
            Ok(stream)
        }
        .boxed_local()
    }
}

struct HttpHandler {
    channel: Rc<dyn Channel>,
    blob: &'static str,
}

#[async_trait::async_trait(?Send)]
impl kj_hyper::server::Handler for HttpHandler {
    async fn request<'a>(
        &'a self,
        method: Method,
        url: &'a [u8],
        headers: HeadersRef<'a>,
        body: Pin<&'a mut AsyncInputStream>,
        response: ServiceResponse<'a>,
    ) -> Result<()> {
        let metadata = ffi::new_request_metadata(KjMaybe::Some(self.blob), KjMaybe::None);
        let mut worker = worker::CxxWorkerInterface::new(self.channel.start_request(metadata)?);
        worker.request(method, url, headers, body, response).await
    }

    async fn connect<'a>(
        &'a self,
        host: &'a [u8],
        headers: HeadersRef<'a>,
        connect: kj_hyper::server::Connect,
    ) -> Result<()> {
        let metadata = ffi::new_request_metadata(KjMaybe::None, KjMaybe::None);
        let mut worker = worker::CxxWorkerInterface::new(self.channel.start_request(metadata)?);
        let (mut stream, mut response) = connect.into_kj();
        worker
            .connect(
                host,
                headers,
                stream.as_mut(),
                ConnectResponse::from(response.as_mut()),
                ConnectSettings {
                    use_tls: false,
                    tls_starter: KjMaybe::None,
                },
            )
            .await
    }
}

fn front(
    factory: &Rc<Factory>,
    channel: Rc<dyn Channel>,
) -> (
    tokio::io::DuplexStream,
    futures::future::RemoteHandle<Result<()>>,
) {
    let (ours, client) = tokio::io::duplex(128);
    let owned = Rc::clone(factory);
    let task = factory.spawn(async move {
        let shutdown = kj_hyper::server::Shutdown::new();
        kj_hyper::server::serve_connection(
            ours,
            Box::pin(std::future::pending()),
            crate::services::header_table(&owned),
            Rc::new(kj_hyper::server::ServerSettings::default()),
            Rc::new(HttpHandler {
                channel,
                blob: "{\"fixture\":\"opaque-cf-not-authority\"}",
            }),
            &shutdown,
        )
        .await
    });
    (client, task)
}

async fn head(io: &mut tokio::io::DuplexStream) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        let mut byte = [0];
        io.read_exact(&mut byte).await.unwrap();
        bytes.push(byte[0]);
    }
    bytes
}

async fn rpc_http_exchange(factory: &Factory, channel: Rc<dyn Channel>, blob: &'static str) {
    let (ours, mut client) = tokio::io::duplex(128);
    let shutdown = kj_hyper::server::Shutdown::new();
    let serving = kj_hyper::server::serve_connection(
        ours,
        Box::pin(std::future::pending()),
        crate::services::header_table(factory),
        Rc::new(kj_hyper::server::ServerSettings::default()),
        Rc::new(HttpHandler {
            channel: Rc::clone(&channel),
            blob,
        }),
        &shutdown,
    );
    let request = async {
        let payload = vec![b'x'; 40_000];
        let (mut read, mut write) = tokio::io::split(&mut client);
        let send = async {
            write
                    .write_all(
                        b"POST https://peer.test HTTP/1.1\r\nHost: peer.test\r\nConnection: close\r\nContent-Length: 40000\r\n\r\n",
                    )
                    .await
                    .unwrap();
            write.write_all(&payload).await.unwrap();
        };
        let receive = async {
            let mut response = Vec::new();
            read.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200 "));
            assert!(response.ends_with(&payload));
        };
        futures::join!(send, receive);
    };
    let (served, ()) = futures::join!(serving, request);
    served.unwrap();
}

async fn real_bootstrap_matrix(factory: &Rc<Factory>) {
    const FIRST: &str = "{\"fixture\":\"first-not-authority\"}";
    const SECOND: &str = "{\"fixture\":\"second-not-authority\"}";
    let connects = Rc::new(RefCell::new(Vec::new()));
    let drops = Rc::new(Cell::new(0));
    let clearing = Rc::new(Cell::new(false));
    let blobs = Rc::new(RefCell::new(Vec::new()));
    let peer: Rc<dyn Channel> = Rc::new(Peer {
        events: Rc::new(RefCell::new(Vec::new())),
        dropped: Rc::new(Cell::new(0)),
        blobs: Rc::clone(&blobs),
    });
    let endpoint = Rc::new(UnscopedRpcWorkerEndpoint::new(Rc::new(Connected {
        factory: Rc::downgrade(factory),
        target: Rc::clone(&peer),
        requests: Rc::clone(&connects),
        drops: Rc::clone(&drops),
        clearing: Rc::clone(&clearing),
    })));
    endpoint.bind_factory(factory).unwrap();
    factory.owned_endpoints.borrow_mut().push(endpoint.clone());
    let channel = endpoint.resolve(&target()).unwrap();
    futures::join!(
        rpc_http_exchange(factory, Rc::clone(&channel), FIRST),
        rpc_http_exchange(factory, Rc::clone(&channel), SECOND),
    );
    assert_eq!(connects.borrow().len(), 2, "connection was pooled");
    for (designator, _) in connects.borrow().iter() {
        assert_eq!(*designator, target());
    }
    let requested = connects
        .borrow()
        .iter()
        .map(|(_, blob)| blob.clone().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        requested,
        std::collections::HashSet::from([FIRST.to_owned(), SECOND.to_owned()])
    );
    assert_eq!(blobs.borrow().len(), 2);
    let received = blobs
        .borrow()
        .iter()
        .map(|blob| blob.clone().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(received, requested);
    refused_published_events(Rc::clone(&channel), &connects).await;
    retire_bootstrap_transports(
        factory, endpoint, channel, peer, &connects, &drops, &clearing,
    )
    .await;
}

// The ordinary server refuses these events; preserve its refusal rather than successful defaults.
async fn refused_published_events(channel: Rc<dyn Channel>, connects: &ConnectionRequests) {
    let mut prewarm = worker::CxxWorkerInterface::new(
        channel
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    assert!(prewarm.prewarm("/warm").await.is_err());
    let mut scheduled = worker::CxxWorkerInterface::new(
        channel
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    assert!(
        scheduled
            .run_scheduled(&SystemTime::UNIX_EPOCH, "* * * * *")
            .await
            .is_err()
    );
    let mut alarm = worker::CxxWorkerInterface::new(
        channel
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    assert!(alarm.run_alarm(&SystemTime::UNIX_EPOCH, 0).await.is_err());
    let mut abandoned = worker::CxxWorkerInterface::new(
        channel
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    assert!(
        abandoned
            .abandon_alarm(&SystemTime::UNIX_EPOCH)
            .await
            .is_err()
    );
    let mut remote_test = worker::CxxWorkerInterface::new(
        channel
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    let before_test = connects.borrow().len();
    let error = remote_test.test().await.unwrap_err();
    assert_eq!(
        error.description(),
        "standard Workerd RPC has no remote test operation"
    );
    assert_eq!(
        connects.borrow().len(),
        before_test,
        "unpublished test operation opened a connection"
    );
    drop((prewarm, scheduled, alarm, abandoned, remote_test));
}

async fn retire_bootstrap_transports(
    factory: &Rc<Factory>,
    endpoint: Rc<UnscopedRpcWorkerEndpoint>,
    channel: Rc<dyn Channel>,
    peer: Rc<dyn Channel>,
    connects: &ConnectionRequests,
    drops: &Rc<Cell<usize>>,
    clearing: &Rc<Cell<bool>>,
) {
    let sibling = Rc::new(UnscopedRpcWorkerEndpoint::new(Rc::new(Connected {
        factory: Rc::downgrade(factory),
        target: peer,
        requests: Rc::clone(connects),
        drops: Rc::clone(drops),
        clearing: Rc::clone(clearing),
    })));
    sibling.bind_factory(factory).unwrap();
    factory.owned_endpoints.borrow_mut().push(sibling.clone());
    let sibling_channel = sibling.resolve(&target()).unwrap();
    let (mut websocket, websocket_task) = front(factory, Rc::clone(&channel));
    websocket
        .write_all(
            b"GET /ws HTTP/1.1\r\nHost: peer.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n",
        )
        .await
        .unwrap();
    assert!(head(&mut websocket).await.starts_with(b"HTTP/1.1 101 "));
    let (mut tunnel, tunnel_task) = front(factory, Rc::clone(&channel));
    tunnel
        .write_all(b"CONNECT peer.test:80 HTTP/1.1\r\nHost: peer.test\r\n\r\n")
        .await
        .unwrap();
    assert!(head(&mut tunnel).await.starts_with(b"HTTP/1.1 200 "));
    tunnel.write_all(b"echo").await.unwrap();
    let mut echo = [0; 4];
    tunnel.read_exact(&mut echo).await.unwrap();
    assert_eq!(&echo, b"echo");
    let retire = endpoint.unlink();
    let observed_close = async {
        let mut rest = Vec::new();
        websocket.read_to_end(&mut rest).await.unwrap();
        tunnel.read_to_end(&mut rest).await.unwrap();
        if let Err(error) = websocket_task.await {
            assert!(error.description().contains("retired"), "{error:?}");
        }
        if let Err(error) = tunnel_task.await {
            assert!(error.description().contains("retired"), "{error:?}");
        }
    };
    let (retired, ()) = futures::join!(retire, observed_close);
    retired.unwrap();
    assert!(endpoint.resolve(&target()).is_err());
    let (mut surviving, surviving_task) = front(factory, sibling_channel);
    surviving
        .write_all(
            b"POST https://peer.test HTTP/1.1\r\nHost: peer.test\r\nConnection: close\r\nContent-Length: 7\r\n\r\nsibling",
        )
        .await
        .unwrap();
    let mut response = Vec::new();
    surviving.read_to_end(&mut response).await.unwrap();
    surviving_task.await.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 "));
    assert!(response.ends_with(b"sibling"));
    clearing.set(true);
    ffi::factory_clear_tasks(factory.raw());
    clearing.set(false);
    factory.settle_tasks().await.unwrap();
    assert_eq!(drops.get(), connects.borrow().len());
    assert!(endpoint.resolve(&target()).is_err());
    drop(channel);
    drop(endpoint);
    drop(sibling);
}

fn child() -> bool {
    const CHILD: &str = "WORKERD_OWNED_RPC_TEST_CHILD";
    if std::env::var_os(CHILD).is_some() {
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "rpc_endpoint::tests::all_published_wrappers_and_retirement_survive_task_clear",
            "--nocapture",
        ])
        .env(CHILD, "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    print!("{stdout}");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success());
    assert!(stdout.contains("test result: ok. 1 passed;"));
    false
}

fn rpc_fixture_config() -> Vec<u64> {
    let mut message = capnp::message::Builder::new_default();
    {
        let root = message.init_root::<config::Builder>();
        let mut services = root.init_services(2);
        for (index, name) in ["local", "remote"].iter().enumerate() {
            let mut service = services.reborrow().get(index as u32);
            service.set_name(*name);
            let mut worker = service.init_worker();
            worker.set_compatibility_date("2026-10-19");
            let mut module = worker.init_modules(1).get(0);
            module.set_name("main.js");
            module.set_es_module(if index == 0 {
                "export default { fetch() { return new Response('local'); } };"
            } else {
                "throw new Error('RPC remote must not initialize locally');"
            });
        }
    }
    let bytes = capnp::serialize::write_message_to_words(&message);
    bytes
        .chunks_exact(8)
        .map(|word| u64::from_ne_bytes(word.try_into().unwrap()))
        .collect()
}

#[test]
fn all_published_wrappers_and_retirement_survive_task_clear() {
    if !child() {
        return;
    }
    let words = rpc_fixture_config();
    let options = ffi::ServeOrTestOptions {
        perfetto_trace_path: KjMaybe::None,
        perfetto_trace_categories: KjMaybe::None,
        async_trace_path: KjMaybe::None,
        async_trace_stacks: KjMaybe::None,
        async_trace_promises: false,
        experimental: false,
        pyodide_package_disk_cache_dir: KjMaybe::None,
        pyodide_bundle_disk_cache_dir: KjMaybe::None,
        python_save_snapshot: false,
        python_save_baseline_snapshot: false,
        python_load_snapshot: KjMaybe::None,
        python_snapshot_dir: KjMaybe::None,
    };
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    let raw = ffi::bootstrap(
        runtime.context(),
        words,
        &options,
        KjMaybe::None,
        KjMaybe::None,
    )
    .unwrap();
    runtime
        .block_on(async move {
            tokio::time::timeout(Duration::from_secs(10), async move {
                let factory = Rc::new(Factory::new(raw));
                configured_endpoint_is_retired(&factory).await;
                recorded_events_survive_task_clear(&factory).await;
                refused_admission_is_not_replayed(&factory).await;
                late_admission_is_closed_before_rpc_publication(&factory).await;
                real_bootstrap_matrix(&factory).await;
                crate::entry::shutdown(factory).await.unwrap();
            })
            .await
            .unwrap();
        })
        .unwrap();
}

async fn configured_endpoint_is_retired(factory: &Rc<Factory>) {
    let configured = Rc::new(UnscopedRpcWorkerEndpoint::new(Rc::new(Refused)));
    let remote: Rc<dyn WorkerEndpoint> = configured.clone();
    let message = factory.config().unwrap();
    let partition = crate::config::WorkerPartition::new(
        message.get_root().unwrap(),
        0,
        std::collections::HashMap::from([("remote".to_owned(), remote)]),
    )
    .unwrap();
    let errors = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&errors);
    let server = crate::config::Server::start_with_worker_partition(
        Rc::clone(factory),
        &crate::config::Overrides::default(),
        crate::config::Reporter::new(
            Box::new(move |error| sink.borrow_mut().push(error)),
            Box::new(|warning| eprintln!("{warning}")),
            false,
        ),
        std::collections::HashMap::new(),
        std::collections::HashMap::new(),
        partition,
    )
    .await
    .unwrap();
    assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
    assert!(Weak::ptr_eq(
        configured.0.factory.get().unwrap(),
        &Rc::downgrade(factory),
    ));
    assert_eq!(
        server
            .services()
            .filter_map(|(name, service)| {
                matches!(service, crate::config::Service::Worker(_)).then_some(name)
            })
            .collect::<Vec<_>>(),
        ["local"]
    );
    drop(server);
    assert!(configured.resolve(&target()).is_err());
    factory.settle_tasks().await.unwrap();
    drop(configured);
}

async fn recorded_events_survive_task_clear(factory: &Rc<Factory>) {
    let endpoint = Rc::new(UnscopedRpcWorkerEndpoint::new(Rc::new(Refused)));
    endpoint.bind_factory(factory).unwrap();
    endpoint.bind_factory(factory).unwrap();
    factory.owned_endpoints.borrow_mut().push(endpoint.clone());
    let events = Rc::new(RefCell::new(Vec::new()));
    let dropped = Rc::new(Cell::new(0));
    let mut request = recorded_request(&endpoint, Rc::clone(&events), Rc::clone(&dropped));
    request.prewarm("/warm").await.unwrap();
    assert_eq!(
        request
            .run_scheduled(
                &(SystemTime::UNIX_EPOCH + Duration::from_secs(123)),
                "0 * * * *",
            )
            .await
            .unwrap(),
        ScheduledResult {
            retry: true,
            outcome: outcome_capnp::EventOutcome::Exception,
        }
    );
    assert_eq!(
        request
            .run_alarm(
                &(SystemTime::UNIX_EPOCH + Duration::from_millis(123_456)),
                7,
            )
            .await
            .unwrap(),
        AlarmResult {
            retry: true,
            retry_counts_against_limit: false,
            outcome: outcome_capnp::EventOutcome::Exception,
            error_description: Some("fixture alarm detail".to_owned()),
        }
    );
    assert_eq!(
        request
            .abandon_alarm(&SystemTime::UNIX_EPOCH)
            .await
            .unwrap(),
        Some(SystemTime::UNIX_EPOCH + Duration::from_secs(987))
    );
    let before_test = events.borrow().len();
    let error = request.test().await.unwrap_err();
    assert_eq!(
        error.description(),
        "standard Workerd RPC has no remote test operation"
    );
    assert_eq!(
        events.borrow().len(),
        before_test,
        "test reached the target"
    );
    assert_eq!(
        &*events.borrow(),
        &["prewarm:/warm", "scheduled:0 * * * *", "alarm:7", "abandon",]
    );
    drop(request);
    assert_eq!(dropped.get(), 0, "drop ran inside the request's drop");
    ffi::factory_clear_tasks(factory.raw());
    assert_eq!(dropped.get(), 0, "task clear destroyed the queued resource");
    factory.settle_tasks().await.unwrap();
    assert_eq!(dropped.get(), 1);
    assert!(endpoint.resolve(&target()).is_err());
    drop(endpoint);
}

async fn refused_admission_is_not_replayed(factory: &Rc<Factory>) {
    let refused_calls = Rc::new(Cell::new(0));
    let refused = Rc::new(UnscopedRpcWorkerEndpoint::new(Rc::new(CountedRefusal(
        Rc::clone(&refused_calls),
    ))));
    refused.bind_factory(factory).unwrap();
    factory.owned_endpoints.borrow_mut().push(refused.clone());
    let mut failed = worker::CxxWorkerInterface::new(
        refused
            .resolve(&target())
            .unwrap()
            .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
            .unwrap(),
    );
    for _ in 0..2 {
        let error = failed.prewarm("/refused").await.unwrap_err();
        assert_eq!(error.description(), "fixture admission refused");
    }
    assert_eq!(refused_calls.get(), 1, "failed admission was replayed");
    drop(failed);
    factory.settle_tasks().await.unwrap();
    drop(refused);
}
