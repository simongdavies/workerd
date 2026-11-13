use std::cell::Cell;
use std::collections::VecDeque;
use std::pin::Pin;
use std::time::Duration;
use std::time::SystemTime;

use kj::http::ConnectResponse;
use kj::http::ConnectSettings;
use kj::http::HeadersRef;
use kj::http::Method;
use kj::http::ServiceResponse;
use kj::io::AsyncInputStream;
use kj::io::AsyncIoStream;
use kj_rs::KjOwn;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio::net::UnixStream;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use worker::Interface;
use workerd_server::channels::PendingToken;
use workerd_server::channels::RequestMetadata;
use workerd_server::channels::TokenUsage;
use workerd_server::channels::WorkerInterface;

use super::*;

type Incoming = Rc<Mutex<mpsc::UnboundedReceiver<io::Result<Connection<UnixStream>>>>>;

async fn tcp_pair() -> (TcpStream, TcpStream) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (connected, accepted) = futures::join!(TcpStream::connect(address), listener.accept());
    (connected.unwrap(), accepted.unwrap().0)
}

#[test]
fn async_admission_owns_stream_and_does_not_publish_context_before_completion() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (_peer, stream) = tcp_pair().await;
        let (release, gate) = oneshot::channel();
        let gate = Rc::new(RefCell::new(Some(gate)));
        let published = Rc::new(Cell::new(false));
        let observed = Rc::clone(&published);
        let provider = ConnectionAdmission::Stream(Rc::new(move |stream, config, worker| {
            assert_eq!(config, [7; 32]);
            assert_eq!(worker, 3);
            let gate = gate.borrow_mut().take().unwrap();
            let published = Rc::clone(&observed);
            async move {
                gate.await?;
                published.set(true);
                Ok(AdmittedConnection::new(
                    stream,
                    std::sync::Arc::new("test-only-context"),
                ))
            }
            .boxed_local()
        }));
        let mut admission = Box::pin(provider.admit(stream, [7; 32], 3));
        assert!(futures::poll!(admission.as_mut()).is_pending());
        assert!(!published.get());
        release.send(()).unwrap();
        let admitted = admission.await.unwrap();
        assert!(published.get());
        assert_eq!(
            admitted.context.downcast_ref::<&str>(),
            Some(&"test-only-context")
        );
    });
}

#[test]
fn async_admission_refusal_closes_its_owned_stream_without_context_fallback() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (mut peer, stream) = tcp_pair().await;
        let provider = ConnectionAdmission::Stream(Rc::new(|stream, _, _| {
            async move {
                drop(stream);
                anyhow::bail!("test-owned admission refused");
            }
            .boxed_local()
        }));
        let error = provider.admit(stream, [7; 32], 3).await.err().unwrap();
        assert_eq!(error.to_string(), "test-owned admission refused");
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    });
}

#[test]
fn cancelling_pending_admission_drops_only_its_original_owned_stream() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let (mut peer, stream) = tcp_pair().await;
        let (mut sibling_peer, mut sibling) = tcp_pair().await;
        let provider = ConnectionAdmission::Stream(Rc::new(|stream, _, _| {
            async move {
                let result = std::future::pending::<Result<AdmittedConnection>>().await;
                drop(stream);
                result
            }
            .boxed_local()
        }));
        let mut admission = Box::pin(provider.admit(stream, [7; 32], 3));
        assert!(futures::poll!(admission.as_mut()).is_pending());
        drop(admission);
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
        sibling.write_all(b"x").await.unwrap();
        sibling_peer.read_exact(&mut byte).await.unwrap();
        assert_eq!(byte, [b'x']);
    });
}

#[test]
fn admitted_targets_preserve_entrypoint_props_and_refuse_invalid_routes_before_http() {
    if !child(
        "ingress::tests::admitted_targets_preserve_entrypoint_props_and_refuse_invalid_routes_before_http",
    ) {
        return;
    }
    let bytes = fixture_source(
        false,
        "192.0.2.80:8080",
        1,
        false,
        Some(
            r"
import { WorkerEntrypoint } from 'cloudflare:workers';
export default {
  fetch() { return new Response('default', {headers: {'content-length': '7'}}); }
};
export class Admin extends WorkerEntrypoint {
  fetch() {
    const body = `${this.ctx.props.role}:${this.ctx.props.nested.value}`;
    return new Response(body, {headers: {'content-length': String(body.length)}});
  }
}
",
        ),
    );
    let mut plan = Plan::parse(&bytes, "ingress").unwrap();
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    let raw = ffi::bootstrap(
        runtime.context(),
        std::mem::take(&mut plan.words),
        &crate::options(),
        KjMaybe::None,
        KjMaybe::None,
    )
    .unwrap();
    runtime
        .block_on(async move {
            tokio::time::timeout(Duration::from_secs(10), admitted_target_scenario(raw, plan))
                .await
                .unwrap();
        })
        .unwrap();
}

fn admitted_designator(service: &str, entrypoint: Option<&str>, props: Option<&str>) -> Designator {
    Designator {
        service: service.to_owned(),
        entrypoint: entrypoint.map(str::to_owned),
        props_json: props.map(str::to_owned),
        error_context: "admitted target fixture".to_owned(),
    }
}

async fn admitted_target_scenario(raw: KjOwn<ffi::WorkerFactory>, plan: Plan) {
    let factory = Rc::new(Factory::new(raw));
    let message = factory.config().unwrap();
    let partition = WorkerPartition::new(message.get_root().unwrap(), 0, HashMap::new()).unwrap();
    let errors = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&errors);
    let reporter = Reporter::new(
        Box::new(move |error| sink.borrow_mut().push(error)),
        Box::new(|warning| eprintln!("{warning}")),
        false,
    );
    let server = Server::start_with_worker_partition(
        Rc::clone(&factory),
        &Overrides::default(),
        reporter,
        HashMap::new(),
        HashMap::from([(
            "internet".to_owned(),
            Rc::new(crate::DeniedInternet) as Rc<dyn Channel>,
        )]),
        partition,
    )
    .await
    .unwrap();
    let default = server.lookup(&plan.designator);
    assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
    let targets =
        Rc::new(ConnectionTargets::new(&factory, Rc::clone(&server), default, 0).unwrap());
    assert!(
        ConnectionTargets::new(
            &factory,
            Rc::clone(&server),
            server.lookup(&plan.designator),
            1,
        )
        .is_err()
    );
    let refused = [
        admitted_designator("other-worker", None, None),
        admitted_designator("", None, None),
        admitted_designator("app", Some("Missing"), None),
        admitted_designator("app", Some("Admin"), Some("{")),
    ];
    for target in &refused {
        assert!(targets.resolve(Some(target)).is_err(), "{target:?}");
    }
    assert!(errors.borrow().is_empty(), "refused route poisoned config");
    let chosen = Rc::new(RefCell::new(VecDeque::new()));
    let pending = Rc::clone(&chosen);
    let provider = ConnectionAdmission::Stream(Rc::new(move |stream, _, worker| {
        assert_eq!(worker, 0);
        let target = pending.borrow_mut().pop_front().unwrap();
        async move {
            let admitted =
                AdmittedConnection::new(stream, std::sync::Arc::new("test-only-routing-context"));
            Ok(match target {
                Some(target) => admitted.with_target(target),
                None => admitted,
            })
        }
        .boxed_local()
    }));
    let Protocol::Http(rewriter) = plan.protocol else {
        panic!("fixture requires HTTP");
    };
    let (sender, receiver) = mpsc::unbounded_channel();
    let receiver = Rc::new(Mutex::new(receiver));
    let owned = Rc::clone(&factory);
    let serving = factory.spawn(async move {
        composed_with_context(
            owned,
            rewriter,
            targets,
            provider,
            plan.config_sha256,
            0,
            move || {
                let receiver = Rc::clone(&receiver);
                async move { receiver.lock().await.recv().await.unwrap() }
            },
        )
        .await
    });
    check_admitted_requests(&sender, &chosen, refused).await;
    assert!(
        sender
            .send(Err(io::Error::from_raw_os_error(libc::ECANCELED)))
            .is_ok()
    );
    serving.await.unwrap();
    assert!(chosen.borrow().is_empty());
    assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
    drop(server);
    factory.settle_tasks().await.unwrap();
}

async fn check_admitted_requests(
    sender: &mpsc::UnboundedSender<io::Result<Connection<TcpStream>>>,
    chosen: &RefCell<VecDeque<Option<Designator>>>,
    refused: [Designator; 4],
) {
    for target in refused {
        chosen.borrow_mut().push_back(Some(target));
        let (mut peer, stream) = tcp_pair().await;
        assert!(sender.send(Ok(observe(stream))).is_ok());
        let mut byte = [0];
        assert_eq!(peer.read(&mut byte).await.unwrap(), 0);
    }
    for (target, expected) in [
        (None, "default"),
        (
            Some(admitted_designator(
                "app",
                Some("Admin"),
                Some(r#"{"role":"reader","nested":{"value":17}}"#),
            )),
            "reader:17",
        ),
        (None, "default"),
    ] {
        chosen.borrow_mut().push_back(target);
        let (mut peer, stream) = tcp_pair().await;
        assert!(sender.send(Ok(observe(stream))).is_ok());
        peer.write_all(
                    b"GET / HTTP/1.1\r\nHost: example.test\r\nX-Worker-Entrypoint: Admin\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
        let mut response = Vec::new();
        peer.read_to_end(&mut response).await.unwrap();
        assert!(response.starts_with(b"HTTP/1.1 200 "));
        let body = response
            .windows(4)
            .position(|bytes| bytes == b"\r\n\r\n")
            .unwrap()
            + 4;
        assert_eq!(&response[body..], expected.as_bytes());
    }
}

fn accepting(
    incoming: Incoming,
) -> impl FnMut() -> LocalBoxFuture<'static, io::Result<Connection<UnixStream>>> {
    move || {
        let incoming = Rc::clone(&incoming);
        async move {
            incoming
                .lock()
                .await
                .recv()
                .await
                .unwrap_or_else(|| Err(io::Error::from_raw_os_error(libc::ECONNABORTED)))
        }
        .boxed_local()
    }
}

fn admit(sender: &mpsc::UnboundedSender<io::Result<Connection<UnixStream>>>) -> UnixStream {
    let (frontend, guest) = UnixStream::pair().unwrap();
    assert!(sender.send(Ok(observe(guest))).is_ok());
    frontend
}

fn config(tcp: bool, address: &str, sockets: u32) -> Vec<u8> {
    fixture(tcp, address, sockets, true)
}

fn fixture(tcp: bool, address: &str, sockets: u32, provider: bool) -> Vec<u8> {
    fixture_source(tcp, address, sockets, provider, None)
}

fn fixture_source(
    tcp: bool,
    address: &str,
    sockets: u32,
    provider: bool,
    source: Option<&str>,
) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut root = message.init_root::<config::Builder>();
        let mut service = root.reborrow().init_services(1).get(0);
        service.set_name("app");
        let mut worker = service.init_worker();
        worker.set_compatibility_date("2026-10-19");
        let mut module = worker.reborrow().init_modules(1).get(0);
        module.set_name("main.js");
        module.set_es_module(source.unwrap_or(
            r"
          export default {
            async fetch(request, env, ctx) {
              const path = new URL(request.url).pathname;
              if (path === '/ws') {
                const pair = new WebSocketPair();
                const socket = pair[1];
                if (socket.binaryType !== 'blob') throw new Error('binaryType default');
                socket.binaryType = 'arraybuffer';
                socket.accept();
                socket.addEventListener('message', event => socket.send(event.data));
                socket.addEventListener('close', event => {
                  if (event.code === 1000) socket.close(event.code, event.reason);
                  else socket.close();
                });
                return new Response(null, {status: 101, webSocket: pair[0]});
              }
              if (path === '/wait') {
                ctx.waitUntil(env.PROBE.fetch('https://probe.test/gate')
                  .then(() => env.PROBE.fetch('https://probe.test/late')));
                return new Response('early', {headers: {'content-length': '5'}});
              }
              if (path === '/blocked') {
                await env.PROBE.fetch('https://probe.test/gate');
                return new Response('unexpected');
              }
              return new Response(request.body, {
                headers: {'content-length': request.headers.get('content-length') ?? '0'}
              });
            },
            async connect(socket) {
              const reader = socket.readable.getReader();
              const writer = socket.writable.getWriter();
              for (;;) {
                const {value, done} = await reader.read();
                if (done) break;
                await writer.write(value);
              }
              await writer.write(new TextEncoder().encode('|fin|'));
              await writer.close();
            }
          };
        ",
        ));
        if provider {
            let mut binding = worker.init_bindings(1).get(0);
            binding.set_name("PROBE");
            binding.init_service().set_name("probe");
        }
        let mut entries = root.init_sockets(sockets);
        for index in 0..sockets {
            let mut socket = entries.reborrow().get(index);
            socket.set_name("ingress");
            socket.set_address(address);
            socket.reborrow().init_service().set_name("app");
            if tcp {
                socket.init_tcp();
            } else {
                socket.init_http();
            }
        }
    }
    capnp::serialize::write_message_to_words(&message)
}

#[test]
fn terminal_plan_accepts_fixed_external_leaf_without_changing_worker_selection() {
    let bytes = external_fixture();
    let plan = Plan::parse(&bytes, "ingress").unwrap();
    assert_eq!(plan.designator.service, "app");
    let message = crate::read_config(&bytes).unwrap();
    let config = message.get_root::<config::Reader>().unwrap();
    assert_eq!(crate::selected_worker_from_socket(config).unwrap(), 0);
    assert_eq!(
        WorkerPartition::new(config, 0, HashMap::new())
            .unwrap()
            .local_index(),
        0
    );
    let service::Which::External(external) = config.get_services().unwrap().get(1).which().unwrap()
    else {
        panic!("external leaf changed");
    };
    assert_eq!(
        external.unwrap().get_address().unwrap().to_str().unwrap(),
        "192.0.2.80:8080"
    );
}

fn external_fixture() -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<config::Builder>();
    let mut services = root.reborrow().init_services(2);
    let mut app = services.reborrow().get(0);
    app.set_name("app");
    let mut worker = app.init_worker();
    worker.set_compatibility_date("2026-10-19");
    worker
        .reborrow()
        .init_global_outbound()
        .set_name("exact-peer");
    let mut module = worker.init_modules(1).get(0);
    module.set_name("main.js");
    module.set_es_module("export default { fetch() { return new Response('worker'); } };");
    let mut peer = services.get(1);
    peer.set_name("exact-peer");
    let mut external = peer.init_external();
    external.set_address("192.0.2.80:8080");
    external.init_http();
    let mut socket = root.init_sockets(1).get(0);
    socket.set_name("ingress");
    socket.set_address("192.0.2.81:8080");
    socket.reborrow().init_http().set_capnp_connect_host("rpc");
    socket.init_service().set_name("app");
    capnp::serialize::write_message_to_words(&message)
}

#[test]
fn terminal_plan_still_refuses_other_service_types_and_non_worker_ingress() {
    let bytes = external_fixture();
    let original = crate::read_config(&bytes).unwrap();
    for kind in ["network", "disk", "unspecified", "external-entry"] {
        let mut message = capnp::message::Builder::new_default();
        message
            .set_root(original.get_root::<config::Reader>().unwrap())
            .unwrap();
        let mut root = message.get_root::<config::Builder>().unwrap();
        if kind == "external-entry" {
            root.get_sockets()
                .unwrap()
                .get(0)
                .init_service()
                .set_name("exact-peer");
        } else {
            let mut peer = root.reborrow().get_services().unwrap().get(1);
            match kind {
                "network" => {
                    peer.init_network();
                }
                "disk" => {
                    peer.init_disk();
                }
                "unspecified" => peer.set_unspecified(()),
                _ => unreachable!(),
            }
        }
        let error = Plan::parse(
            &capnp::serialize::write_message_to_words(&message),
            "ingress",
        )
        .err()
        .unwrap();
        assert_eq!(
            error.to_string(),
            if kind == "external-entry" {
                "guest ingress service must be a configured Worker"
            } else {
                "guest ingress requires worker or external services and admitted channels"
            }
        );
    }
}

#[test]
fn terminal_external_leaf_retains_default_deny_and_supplied_name_conflicts() {
    if !child(
        "ingress::tests::terminal_external_leaf_retains_default_deny_and_supplied_name_conflicts",
    ) {
        return;
    }
    let bytes = external_fixture();
    let mut executor = crate::Executor::new(HashMap::new())
        .unwrap()
        .with_worker_admission_factory(crate::selected_worker_from_socket)
        .unwrap();
    let (mut plan, channels) = executor.prepare_ingress(&bytes, "ingress").unwrap();
    let message = crate::read_config(&bytes).unwrap();
    let partition = executor
        .take_worker_partition(message.get_root().unwrap())
        .unwrap()
        .unwrap();
    let raw = ffi::bootstrap(
        executor.runtime.context(),
        std::mem::take(&mut plan.words),
        &crate::options(),
        KjMaybe::None,
        KjMaybe::None,
    )
    .unwrap();
    executor
        .runtime
        .block_on(async move {
            let factory = Rc::new(Factory::new(raw));
            let mut conflicting = channels.clone();
            conflicting.insert(
                "exact-peer".to_owned(),
                Rc::new(crate::DeniedInternet) as Rc<dyn Channel>,
            );
            let reporter = || {
                Reporter::new(
                    Box::new(|error| panic!("unexpected configuration error: {error}")),
                    Box::new(|warning| eprintln!("{warning}")),
                    false,
                )
            };
            let error = Server::start_with_services(
                Rc::clone(&factory),
                &Overrides::default(),
                reporter(),
                HashMap::new(),
                conflicting,
            )
            .await
            .err()
            .unwrap();
            assert_eq!(
                error.description(),
                "Services supplied by the embedder conflict with configured services: exact-peer"
            );
            let server = Server::start_with_worker_partition(
                Rc::clone(&factory),
                &Overrides::default(),
                reporter(),
                HashMap::new(),
                channels,
                partition,
            )
            .await
            .unwrap();
            let mut names: Vec<_> = server.services().map(|(name, _)| name.to_owned()).collect();
            names.sort();
            assert_eq!(names, ["app", "exact-peer", "internet"]);
            let denied = server.lookup(&Designator {
                service: "internet".to_owned(),
                entrypoint: None,
                props_json: None,
                error_context: "terminal default denial".to_owned(),
            });
            let error = denied
                .start_request(ffi::new_request_metadata(KjMaybe::None, KjMaybe::None))
                .err()
                .unwrap();
            assert_eq!(
                error.description(),
                "Hyperlight executor has no admitted outbound channel"
            );
            drop(denied);
            drop(server);
            factory.settle_tasks().await.unwrap();
        })
        .unwrap();
    executor.close().unwrap();
}

#[test]
fn selected_config_requires_one_explicit_ipv4_http_or_tcp_socket() {
    for tcp in [false, true] {
        let bytes = config(tcp, "192.0.2.80:8080", 1);
        assert!(Plan::parse(&bytes, "ingress").is_ok());
        assert!(Plan::parse(&bytes, "missing").is_err());
        let mut trailing = bytes.clone();
        trailing.extend_from_slice(&[0; 8]);
        assert!(Plan::parse(&trailing, "ingress").is_err());
    }
    for address in [
        "",
        "0.0.0.0:8080",
        "[::1]:8080",
        "192.0.2.80:0",
        "224.0.0.1:8080",
        "255.255.255.255:8080",
    ] {
        assert!(Plan::parse(&config(false, address, 1), "ingress").is_err());
    }
    for sockets in [0, 2] {
        assert!(Plan::parse(&config(false, "192.0.2.80:8080", sockets), "ingress").is_err());
    }
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<config::Builder>();
    root.reborrow().init_services(1).get(0).init_network();
    assert!(
        Plan::parse(
            &capnp::serialize::write_message_to_words(&message),
            "ingress"
        )
        .is_err()
    );
}

#[test]
fn unexpected_accept_error_and_connection_overflow_cannot_be_controlled_drain() {
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    runtime
        .block_on(async {
            for errno in [libc::ECONNABORTED, libc::EIO] {
                let loopback = Loopback::default();
                loopback.enable();
                let _listener = loopback.listen(LOOPBACK_NAME).unwrap();
                let (drain, _draining) = watch::channel(false);
                let result = pump_listener::<UnixStream, _, _, _>(
                    move || async move { Err(io::Error::from_raw_os_error(errno)) },
                    loopback,
                    drain,
                    std::future::pending(),
                )
                .await;
                assert_eq!(
                    result
                        .unwrap_err()
                        .downcast_ref::<io::Error>()
                        .unwrap()
                        .raw_os_error(),
                    Some(errno)
                );
            }
            let loopback = Loopback::default();
            loopback.enable();
            let _listener = loopback.listen(LOOPBACK_NAME).unwrap();
            let (sender, receiver) = mpsc::unbounded_channel();
            let peers: Vec<_> = (0..=MAX_CONNECTIONS).map(|_| admit(&sender)).collect();
            let (drain, _draining) = watch::channel(false);
            let result = pump_listener(
                accepting(Rc::new(Mutex::new(receiver))),
                loopback,
                drain,
                std::future::pending(),
            )
            .await;
            assert!(result.unwrap_err().to_string().contains("connection bound"));
            drop(peers);
        })
        .unwrap();
}

#[test]
fn early_output_fin_and_peer_fin_preserve_the_remaining_slow_upload() {
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let loopback = Loopback::default();
                loopback.enable();
                let listener = loopback.listen(LOOPBACK_NAME).unwrap();
                let pipe = loopback.connect(LOOPBACK_NAME).unwrap();
                let (mut frontend, guest) = UnixStream::pair().unwrap();
                let consumed = Rc::new(Cell::new(0));
                let seen = Rc::clone(&consumed);
                let (finished, received) = oneshot::channel();
                let length = 1024 * 1024;
                let payload: Vec<_> = (0..length).map(|index| (index % 251) as u8).collect();
                let sending = async {
                    let mut early_output = Vec::new();
                    frontend.read_to_end(&mut early_output).await.unwrap();
                    assert!(early_output.is_empty());
                    frontend.write_all(&payload).await.unwrap();
                    assert!(
                        consumed.get() < length,
                        "test did not leave queued input at peer FIN"
                    );
                    frontend.shutdown().await.unwrap();
                    received.await.unwrap();
                };
                let receiving = async {
                    let mut connection = listener.accept().await.unwrap();
                    connection.shutdown().await.unwrap();
                    let mut bytes = [0; 4096];
                    let mut total = 0;
                    loop {
                        let count = connection.read(&mut bytes).await.unwrap();
                        if count == 0 {
                            break;
                        }
                        for (offset, &byte) in bytes[..count].iter().enumerate() {
                            assert_eq!(usize::from(byte), (total + offset) % 251);
                        }
                        total += count;
                        seen.set(total);
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                    assert_eq!(total, length, "locally induced HUP truncated the upload");
                    finished.send(()).unwrap();
                };
                let (pumped, (), ()) =
                    futures::join!(pump(observe(guest), pipe), sending, receiving);
                pumped.unwrap();
                assert_eq!(consumed.get(), length);
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn transport_error_after_local_fin_still_cancels_a_backpressured_pump() {
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    runtime
        .block_on(async {
            tokio::time::timeout(
                Duration::from_secs(10),
                Box::pin(async {
                    let loopback = Loopback::default();
                    loopback.enable();
                    let listener = loopback.listen(LOOPBACK_NAME).unwrap();
                    let pipe = loopback.connect(LOOPBACK_NAME).unwrap();
                    let (frontend, guest) = UnixStream::pair().unwrap();
                    let (closed, closed_receiver) = oneshot::channel();
                    let cancelled = Rc::new(Cell::new(false));
                    let observed = Rc::clone(&cancelled);
                    let before_abort = Rc::clone(&cancelled);
                    let receiving = async move {
                        let mut connection = listener.accept().await.unwrap();
                        connection.write_all(b"unread-response").await.unwrap();
                        connection.shutdown().await.unwrap();
                        closed.send(()).unwrap();
                        connection.hangup().await.unwrap();
                        observed.set(true);
                    };
                    let aborting = async move {
                        closed_receiver.await.unwrap();
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        assert!(
                            frontend
                                .ready(tokio::io::Interest::READABLE)
                                .await
                                .unwrap()
                                .is_read_closed(),
                            "raw local write shutdown was not observed before the abort"
                        );
                        frontend.writable().await.unwrap();
                        let mut written = 0;
                        let body = [b'x'; 16384];
                        loop {
                            match frontend.try_write(&body) {
                                Ok(count) => {
                                    assert!(count > 0);
                                    written += count;
                                    assert!(written < 1024 * 1024);
                                }
                                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                                Err(error) => panic!("upload failed before abort: {error}"),
                            }
                        }
                        assert!(written > 128 * 1024);
                        tokio::time::sleep(Duration::from_millis(5)).await;
                        assert!(!before_abort.get(), "local FIN cancelled the pending input");
                        // Leaving the response unread makes this Unix full close an actual transport
                        // error, unlike the benign two-FIN case above. No controller/VM revoke.
                        drop(frontend);
                    };
                    let (result, (), ()) =
                        futures::join!(pump(observe(guest), pipe), receiving, aborting);
                    assert!(
                        result.is_err(),
                        "late transport abort was ignored after local FIN"
                    );
                    assert!(
                        cancelled.get(),
                        "backpressured peer did not retire after late abort"
                    );
                }),
            )
            .await
            .unwrap();
        })
        .unwrap();
}

#[test]
fn watcher_setup_failure_drops_all_owned_raw_and_pipe_endpoints() {
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    runtime
        .block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                let loopback = Loopback::default();
                loopback.enable();
                let listener = loopback.listen(LOOPBACK_NAME).unwrap();
                let pipe = loopback.connect(LOOPBACK_NAME).unwrap();
                let internal = listener.accept().await.unwrap();
                let (mut frontend, guest) = UnixStream::pair().unwrap();
                let mut connection = observe(guest);
                assert!(futures::poll!(connection.disconnected.as_mut()).is_pending());
                connection.aborted =
                    std::future::ready(Err(anyhow::anyhow!("watch registration failed"))).boxed();
                assert!(pump(connection, pipe).await.is_err());
                let mut byte = [0];
                assert_eq!(frontend.read(&mut byte).await.unwrap(), 0);
                internal.hangup().await.unwrap();
            })
            .await
            .unwrap();
        })
        .unwrap();
}

#[derive(Default)]
struct Trace {
    active: Cell<usize>,
    late: Cell<usize>,
    completed: Cell<bool>,
    post_completion: Cell<usize>,
    started: Notify,
    retired: Notify,
    consumed: Cell<usize>,
    all_input: Notify,
    gates: RefCell<VecDeque<oneshot::Receiver<()>>>,
}

struct Probe(Rc<Trace>);

struct Active(Rc<Trace>);

impl Drop for Active {
    fn drop(&mut self) {
        self.0.active.set(self.0.active.get() - 1);
        self.0.retired.notify_one();
    }
}

impl Trace {
    fn called(&self) {
        if self.completed.get() {
            self.post_completion.set(self.post_completion.get() + 1);
        }
    }
}

impl Channel for Probe {
    fn start_request(
        &self,
        _: KjOwn<RequestMetadata>,
    ) -> workerd_server::Result<KjOwn<WorkerInterface>> {
        self.0.called();
        Ok(Self(Rc::clone(&self.0)).into_kj())
    }

    fn token(&self, _: TokenUsage) -> workerd_server::Result<KjOwn<PendingToken>> {
        Err(kj::failed!("probe capability is not transferable"))
    }
}

#[async_trait::async_trait(?Send)]
impl kj::http::Service for Probe {
    async fn request<'a>(
        &'a mut self,
        method: Method,
        url: &'a [u8],
        headers: HeadersRef<'a>,
        _: Pin<&'a mut AsyncInputStream>,
        response: ServiceResponse<'a>,
    ) -> workerd_server::Result<()> {
        self.0.called();
        self.0.active.set(self.0.active.get() + 1);
        let _active = Active(Rc::clone(&self.0));
        if url.ends_with(b"/gate") {
            let receiver = self
                .0
                .gates
                .borrow_mut()
                .pop_front()
                .ok_or_else(|| kj::failed!("gate reused"))?;
            self.0.started.notify_one();
            receiver
                .await
                .map_err(|_| kj::disconnected!("probe gate closed"))?;
        } else if url.ends_with(b"/late") {
            self.0.late.set(self.0.late.get() + 1);
        } else if url.ends_with(b"/consumed")
            || url == b"https://fake-host/consumed?urlencoded=true"
        {
            if url.contains(&b'?') && method != Method::GET {
                return Err(kj::failed!("unexpected KV method"));
            }
            self.0.consumed.set(self.0.consumed.get() + 1);
            self.0.all_input.notify_one();
        } else {
            return Err(kj::failed!("unadmitted provider destination"));
        }
        let headers = headers.clone_shallow();
        let mut body = response.send(200, "OK", &headers, Some(2))?;
        body.write(b"ok").await
    }

    async fn connect<'a>(
        &'a mut self,
        _: &'a [u8],
        _: HeadersRef<'a>,
        _: Pin<&'a mut AsyncIoStream>,
        _: ConnectResponse<'a>,
        _: ConnectSettings<'a>,
    ) -> workerd_server::Result<()> {
        Err(kj::failed!("probe has no network authority"))
    }
}

#[async_trait::async_trait(?Send)]
impl Interface for Probe {
    async fn run_scheduled(
        &mut self,
        _: &SystemTime,
        _: &str,
    ) -> worker::Result<worker::ScheduledResult> {
        Err(kj::not_implemented!("probe has no scheduled handler"))
    }

    async fn run_alarm(&mut self, _: &SystemTime, _: u32) -> worker::Result<worker::AlarmResult> {
        Err(kj::not_implemented!("probe has no alarm handler"))
    }
}

async fn head(stream: &mut (impl tokio::io::AsyncRead + Unpin)) -> Vec<u8> {
    let mut bytes = Vec::new();
    while !bytes.ends_with(b"\r\n\r\n") {
        assert!(bytes.len() < 8192);
        bytes.push(stream.read_u8().await.unwrap());
    }
    bytes
}

async fn websocket(
    sender: &mpsc::UnboundedSender<io::Result<Connection<UnixStream>>>,
) -> UnixStream {
    let mut stream = admit(sender);
    stream.write_all(b"GET /ws HTTP/1.1\r\nHost: example.test\r\nConnection: Upgrade\r\nUpgrade: websocket\r\nSec-WebSocket-Version: 13\r\nSec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\r\n").await.unwrap();
    assert!(head(&mut stream).await.starts_with(b"HTTP/1.1 101 "));
    stream
}

async fn frame(stream: &mut UnixStream, opcode: u8, bytes: &[u8]) {
    let mut frame = vec![
        0x80 | opcode,
        0x80 | u8::try_from(bytes.len()).unwrap(),
        1,
        2,
        3,
        4,
    ];
    frame.extend(
        bytes
            .iter()
            .enumerate()
            .map(|(i, byte)| byte ^ [1, 2, 3, 4][i % 4]),
    );
    stream.write_all(&frame).await.unwrap();
    assert_eq!(stream.read_u8().await.unwrap(), 0x80 | opcode);
    assert_eq!(
        stream.read_u8().await.unwrap(),
        u8::try_from(bytes.len()).unwrap()
    );
    let mut echoed = vec![0; bytes.len()];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, bytes);
}

fn child(name: &str) -> bool {
    if std::env::var_os("WORKERD_INGRESS_TEST_CHILD").is_some() {
        return true;
    }
    let output = std::process::Command::new(std::env::current_exe().unwrap())
        .args(["--exact", name, "--nocapture"])
        .env("WORKERD_INGRESS_TEST_CHILD", "1")
        .output()
        .unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout);
    print!("{stdout}");
    eprint!("{}", String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success());
    assert!(stdout.contains("test result: ok. 1 passed;"));
    false
}

const EARLY_TCP: &str = r"
export default {
  async connect(socket, env) {
    const reader = socket.readable.getReader();
    await socket.writable.getWriter().close();
    let length = 0;
    for (;;) {
      const {value, done} = await reader.read();
      if (done) break;
      for (const byte of value) {
        if (byte !== length % 251) throw new Error('upload byte mismatch');
        ++length;
      }
      await new Promise(resolve => setTimeout(resolve, 1));
    }
    if (length !== 1048576) throw new Error('upload was truncated');
    await env.PROBE.fetch('https://probe.test/consumed');
  }
};
";

fn exercise(tcp: bool, early: bool) {
    let bytes = if early {
        fixture_source(true, "192.0.2.80:8080", 1, true, Some(EARLY_TCP))
    } else {
        config(tcp, "192.0.2.80:8080", 1)
    };
    export_ingress_configs(&bytes, tcp, early);
    let mut plan = Plan::parse(&bytes, "ingress").unwrap();
    let trace = Rc::new(Trace::default());
    let observed = Rc::clone(&trace);
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    let raw = ffi::bootstrap(
        runtime.context(),
        std::mem::take(&mut plan.words),
        &crate::options(),
        KjMaybe::None,
        KjMaybe::None,
    )
    .unwrap();
    runtime.block_on(async move {
        tokio::time::timeout(Duration::from_secs(10), async move {
            let factory = Rc::new(Factory::new(raw));
            let (gate, receiver) = oneshot::channel();
            let (reset_gate, reset_receiver) = oneshot::channel();
            let mut reset_gate = Some(reset_gate);
            observed.gates.borrow_mut().extend([reset_receiver, receiver]);
            let probe: Rc<dyn Channel> = Rc::new(Probe(Rc::clone(&observed)));
            let errors = Rc::new(RefCell::new(Vec::new()));
            let sink = Rc::clone(&errors);
            let reporter = Reporter::new(Box::new(move |e| sink.borrow_mut().push(e)), Box::new(|w| eprintln!("{w}")), false);
            let server = Server::start_with_services(Rc::clone(&factory), &Overrides::default(), reporter, HashMap::new(),
                HashMap::from([("probe".to_owned(), probe), ("internet".to_owned(), Rc::new(crate::DeniedInternet) as Rc<dyn Channel>)])
            ).await.unwrap();
            assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
            let channel = server.lookup(&plan.designator);
            let (sender, receiver) = mpsc::unbounded_channel();
            let owned = Rc::clone(&factory);
            let mut serving = factory.spawn(async move {
                composed(owned, plan.protocol, plan.address.to_string(), channel,
                    accepting(Rc::new(Mutex::new(receiver)))).await
            });
            if early {
                early_tcp_exchange(&observed, &sender, &mut serving).await;
            } else if tcp {
                half_close_tcp_exchange(&factory, &sender, &mut serving).await;
            } else {
                reset_backpressured_upload(&factory, &observed, &sender, &mut reset_gate).await;
                assert!(futures::poll!(&mut serving).is_pending(), "disconnect retired the whole serve call");
                eprintln!("INGRESS_NATIVE raw_full_close_provider_retired active=0 serve_live=true");
                let payload: Vec<_> = (0_u32..1024 * 1024).map(|i| u8::try_from(i % 251).unwrap()).collect();
                let mut stream = admit(&sender);
                stream.write_all(format!("POST /echo HTTP/1.1\r\nHost: example.test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
                stream.write_all(&payload[..1024]).await.unwrap();
                assert!(head(&mut stream).await.starts_with(b"HTTP/1.1 200 "));
                let (mut read, mut write) = tokio::io::split(stream);
                let rest = payload[1024..].to_vec();
                let written = Rc::new(Cell::new(false));
                let signal = Rc::clone(&written);
                let writing = factory.spawn(async move {
                    write.write_all(&rest).await.unwrap();
                    signal.set(true);
                    write
                });
                factory.sleep(Duration::from_millis(1)).await;
                assert!(!written.get());
                let mut echoed = vec![0; payload.len()];
                read.read_exact(&mut echoed).await.unwrap();
                assert_eq!(echoed, payload);
                drop(writing.await);
                drop(read);
                drain_websockets(&factory, &observed, &sender, &mut serving).await;
                assert_eq!(observed.active.get(), 1);
            }
            drop(sender);
            drop(server);
            factory.settle_tasks().await.unwrap();
            assert_eq!(observed.active.get(), 0);
            drop(reset_gate);
            if tcp {
                drop(gate);
            } else {
                assert!(gate.send(()).is_err());
            }
            assert_eq!(observed.late.get(), 0);
            assert_eq!(observed.post_completion.get(), 0);
            let factory = Rc::try_unwrap(factory).unwrap_or_else(|_| panic!("leaked factory"));
            drop(factory);
        }).await.unwrap();
    }).unwrap();
    drop(runtime);
    trace.completed.set(true);
    assert_eq!(trace.active.get(), 0);
    assert_eq!(trace.post_completion.get(), 0);
}

fn export_ingress_configs(bytes: &[u8], tcp: bool, early: bool) {
    if let Some(directory) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::Path::new(&directory).join(if early {
                "ingress-tcp-early.config.bin"
            } else if tcp {
                "ingress-tcp.config.bin"
            } else {
                "ingress-http.config.bin"
            }),
            bytes,
        )
        .unwrap();
        let plain = fixture(tcp, "192.0.2.80:8080", 1, false);
        std::fs::write(
            std::path::Path::new(&directory).join(if tcp {
                "ingress-tcp-plain.config.bin"
            } else {
                "ingress-http-plain.config.bin"
            }),
            plain,
        )
        .unwrap();
    }
}

async fn early_tcp_exchange(
    observed: &Trace,
    sender: &mpsc::UnboundedSender<io::Result<Connection<UnixStream>>>,
    serving: &mut futures::future::RemoteHandle<Result<()>>,
) {
    let mut stream = admit(sender);
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    assert!(
        response.is_empty(),
        "early fixture sends no pre-EOF payload"
    );
    let bytes: Vec<_> = (0_u32..1024 * 1024)
        .map(|index| u8::try_from(index % 251).unwrap())
        .collect();
    stream.write_all(&bytes).await.unwrap();
    stream.shutdown().await.unwrap();
    observed.all_input.notified().await;
    assert_eq!(observed.consumed.get(), 1);
    assert!(
        futures::poll!(&mut *serving).is_pending(),
        "early EOF completed the serve call"
    );
    assert!(
        sender
            .send(Err(io::Error::from_raw_os_error(libc::ECANCELED)))
            .is_ok()
    );
    drop(stream);
    serving.await.unwrap();
}

async fn half_close_tcp_exchange(
    factory: &Factory,
    sender: &mpsc::UnboundedSender<io::Result<Connection<UnixStream>>>,
    serving: &mut futures::future::RemoteHandle<Result<()>>,
) {
    let mut stream = admit(sender);
    stream.write_all(&[0, 255, 0, 128]).await.unwrap();
    let mut echoed = [0; 4];
    stream.read_exact(&mut echoed).await.unwrap();
    assert_eq!(echoed, [0, 255, 0, 128]);
    assert!(
        sender
            .send(Err(io::Error::from_raw_os_error(libc::ECANCELED)))
            .is_ok()
    );
    factory.sleep(Duration::from_millis(1)).await;
    assert!(futures::poll!(&mut *serving).is_pending());
    stream.shutdown().await.unwrap();
    let mut tail = Vec::new();
    stream.read_to_end(&mut tail).await.unwrap();
    assert_eq!(tail, b"|fin|");
    drop(stream);
    serving.await.unwrap();
}

async fn reset_backpressured_upload(
    factory: &Factory,
    observed: &Trace,
    sender: &mpsc::UnboundedSender<io::Result<Connection<UnixStream>>>,
    reset_gate: &mut Option<oneshot::Sender<()>>,
) {
    eprintln!("INGRESS_NATIVE phase=unread-body-backpressure-full-close");
    let mut blocked = admit(sender);
    blocked
        .write_all(
            b"POST /blocked HTTP/1.1\r\nHost: example.test\r\nContent-Length: 1048576\r\n\r\n",
        )
        .await
        .unwrap();
    observed.started.notified().await;
    assert_eq!(observed.active.get(), 1);
    let (backpressured, stalled) = oneshot::channel();
    let upload = factory.spawn(async move {
        blocked.writable().await.unwrap();
        let mut written = 0;
        let bytes = [b'x'; 16384];
        loop {
            match blocked.try_write(&bytes) {
                Ok(amount) => {
                    assert!(amount > 0, "upload closed before the reset");
                    written += amount;
                    assert!(
                        written < 1024 * 1024,
                        "unread request was not backpressured"
                    );
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
                Err(error) => panic!("upload failed before peer reset: {error}"),
            }
        }
        assert!(
            written > 128 * 1024,
            "insufficient queued body to block the Loopback pipe"
        );
        backpressured.send(()).unwrap();
        std::future::pending::<()>().await;
        drop(blocked);
    });
    stalled.await.unwrap();
    factory.sleep(Duration::from_millis(1)).await;
    assert_eq!(observed.active.get(), 1);
    drop(upload);
    observed.retired.notified().await;
    assert_eq!(
        observed.active.get(),
        0,
        "raw reset did not cancel unread-body provider"
    );
    assert!(reset_gate.take().unwrap().send(()).is_err());
}

async fn drain_websockets(
    factory: &Factory,
    observed: &Trace,
    sender: &mpsc::UnboundedSender<io::Result<Connection<UnixStream>>>,
    serving: &mut futures::future::RemoteHandle<Result<()>>,
) {
    let mut ws = websocket(sender).await;
    frame(&mut ws, 1, b"text").await;
    frame(&mut ws, 2, &[0, 255, 0, 128]).await;
    let abrupt = websocket(sender).await;
    drop(abrupt);
    let mut reconnected = websocket(sender).await;
    frame(&mut reconnected, 1, b"reconnected").await;
    drop(reconnected);
    let mut pending_response = admit(sender);
    pending_response
        .write_all(b"GET /wait HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    assert!(
        head(&mut pending_response)
            .await
            .starts_with(b"HTTP/1.1 200 ")
    );
    let mut body = Vec::new();
    pending_response.read_to_end(&mut body).await.unwrap();
    assert_eq!(body, b"early");
    observed.started.notified().await;
    drop(pending_response);
    assert!(
        sender
            .send(Err(io::Error::from_raw_os_error(libc::ECANCELED)))
            .is_ok()
    );
    factory.sleep(Duration::from_millis(1)).await;
    assert!(futures::poll!(&mut *serving).is_pending());
    frame(&mut ws, 8, &[3, 232]).await;
    drop(ws);
    serving.await.unwrap();
}

#[test]
fn owned_http_pumps_stream_upgrade_reconnect_and_cancel_background_work() {
    if child("ingress::tests::owned_http_pumps_stream_upgrade_reconnect_and_cancel_background_work")
    {
        exercise(false, false);
    }
}

#[test]
fn owned_tcp_pumps_keep_half_close_alive_during_controlled_drain() {
    if child("ingress::tests::owned_tcp_pumps_keep_half_close_alive_during_controlled_drain") {
        exercise(true, false);
    }
}

#[test]
fn worker_closes_output_before_consuming_and_verifying_large_input() {
    if child("ingress::tests::worker_closes_output_before_consuming_and_verifying_large_input") {
        exercise(true, true);
    }
}

#[test]
fn terminal_factory_refuses_before_bootstrap_or_bind_and_cannot_be_retried() {
    let held = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let address = held.local_addr().unwrap().to_string();
    let bytes = fixture_source(
        false,
        &address,
        1,
        false,
        Some("throw new Error('bootstrap must not run'); export default { fetch() {} };"),
    );
    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let executor = crate::Executor::with_service_factory(move |_| {
        observed.set(observed.get() + 1);
        anyhow::bail!("terminal factory refused before bootstrap");
    })
    .unwrap();
    let error = executor.serve_config(|| Ok(bytes), "ingress").unwrap_err();
    assert_eq!(
        error.to_string(),
        "terminal factory refused before bootstrap"
    );
    assert_eq!(calls.get(), 1);
    assert!(held.local_addr().is_ok());

    let bytes = fixture(false, &address, 1, false);
    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let mut executor = crate::Executor::with_service_factory(move |_| {
        observed.set(observed.get() + 1);
        anyhow::bail!("sticky refusal");
    })
    .unwrap();
    let error = executor.prepare_ingress(&bytes, "ingress").err().unwrap();
    assert_eq!(error.to_string(), "sticky refusal");
    assert!(executor.configure(&bytes, "ingress", 4096).is_err());
    assert!(executor.prepare_ingress(&bytes, "ingress").is_err());
    assert_eq!(calls.get(), 1);
    executor.close().unwrap();

    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let mut executor = crate::Executor::with_service_factory(move |_| {
        observed.set(observed.get() + 1);
        Ok(HashMap::new())
    })
    .unwrap();
    let (_, channels) = executor.prepare_ingress(&bytes, "ingress").unwrap();
    assert!(channels.contains_key("internet"));
    assert!(executor.configure(&bytes, "ingress", 4096).is_err());
    assert!(executor.prepare_ingress(&bytes, "ingress").is_err());
    assert_eq!(calls.get(), 1);
    executor.close().unwrap();
}

fn terminal_kv_config() -> Vec<u8> {
    let bytes = fixture_source(
        false,
        "192.0.2.80:8080",
        1,
        false,
        Some(
            "export default { async fetch(request, env) { return new Response('terminal:' + await env.KV.get('consumed')); } };",
        ),
    );
    let original = crate::read_config(&bytes).unwrap();
    let mut message = capnp::message::Builder::new_default();
    message
        .set_root(original.get_root::<config::Reader>().unwrap())
        .unwrap();
    let workerd_capnp::service::Which::Worker(worker) = message
        .get_root::<config::Builder>()
        .unwrap()
        .get_services()
        .unwrap()
        .get(0)
        .which()
        .unwrap()
    else {
        panic!("worker expected")
    };
    let mut binding = worker.unwrap().init_bindings(1).get(0);
    binding.set_name("KV");
    binding.init_kv_namespace().set_name("probe");
    capnp::serialize::write_message_to_words(&message)
}

fn terminal_kv_channels(
    config: config::Reader<'_>,
    observed: &Rc<Trace>,
) -> Result<crate::ServiceChannels> {
    let workerd_capnp::service::Which::Worker(worker) = config.get_services()?.get(0).which()?
    else {
        panic!("worker expected")
    };
    let binding = worker?.get_bindings()?.get(0);
    assert_eq!(binding.get_name()?.to_str()?, "KV");
    let workerd_capnp::worker::binding::Which::KvNamespace(designator) = binding.which()? else {
        panic!("KV binding expected")
    };
    let id = designator?.get_name()?.to_string()?;
    let channel: Rc<dyn Channel> = Rc::new(Probe(Rc::clone(observed)));
    Ok(HashMap::from([(id, channel)]))
}

#[test]
fn terminal_factory_installs_kv_on_the_actual_worker_http_event_path() {
    if !child("ingress::tests::terminal_factory_installs_kv_on_the_actual_worker_http_event_path") {
        return;
    }
    let bytes = terminal_kv_config();
    let trace = Rc::new(Trace::default());
    let observed = Rc::clone(&trace);
    let calls = Rc::new(Cell::new(0));
    let invoked = Rc::clone(&calls);
    let mut executor = crate::Executor::with_service_factory(move |config| {
        invoked.set(invoked.get() + 1);
        terminal_kv_channels(config, &observed)
    })
    .unwrap();
    let (mut plan, channels) = executor.prepare_ingress(&bytes, "ingress").unwrap();
    assert_eq!(calls.get(), 1);
    assert!(channels.contains_key("probe"));
    assert!(channels.contains_key("internet"));
    let raw = ffi::bootstrap(
        executor.runtime.context(),
        std::mem::take(&mut plan.words),
        &crate::options(),
        KjMaybe::None,
        KjMaybe::None,
    )
    .unwrap();
    executor
        .runtime
        .block_on(async move {
            tokio::time::timeout(Duration::from_secs(10), async move {
                let factory = Rc::new(Factory::new(raw));
                let errors = Rc::new(RefCell::new(Vec::new()));
                let sink = Rc::clone(&errors);
                let reporter = Reporter::new(
                    Box::new(move |error| sink.borrow_mut().push(error)),
                    Box::new(|warning| eprintln!("{warning}")),
                    false,
                );
                let server = Server::start_with_services(
                    Rc::clone(&factory),
                    &Overrides::default(),
                    reporter,
                    HashMap::new(),
                    channels,
                )
                .await
                .unwrap();
                assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
                let channel = server.lookup(&plan.designator);
                let (sender, receiver) = mpsc::unbounded_channel();
                let owned = Rc::clone(&factory);
                let serving = factory.spawn(async move {
                    composed(
                        owned,
                        plan.protocol,
                        plan.address.to_string(),
                        channel,
                        accepting(Rc::new(Mutex::new(receiver))),
                    )
                    .await
                });
                let mut frontend = admit(&sender);
                frontend
                    .write_all(b"GET / HTTP/1.1\r\nHost: fixture.test\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                assert!(head(&mut frontend).await.starts_with(b"HTTP/1.1 200 "));
                let mut body = Vec::new();
                frontend.read_to_end(&mut body).await.unwrap();
                assert_eq!(body, b"terminal:ok");
                assert_eq!(trace.consumed.get(), 1);
                drop(frontend);
                assert!(
                    sender
                        .send(Err(io::Error::from_raw_os_error(libc::ECANCELED)))
                        .is_ok()
                );
                serving.await.unwrap();
                drop(sender);
                drop(server);
                factory.settle_tasks().await.unwrap();
                assert_eq!(trace.active.get(), 0);
                drop(Rc::try_unwrap(factory).unwrap_or_else(|_| panic!("leaked factory")));
            })
            .await
            .unwrap();
        })
        .unwrap();
    executor.close().unwrap();
    assert_eq!(calls.get(), 1);
}
