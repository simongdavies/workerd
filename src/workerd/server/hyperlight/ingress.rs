// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Private guest ingress over normal released listeners, owned by one terminal call.

use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::SocketAddr;
use std::os::fd::AsFd;
use std::os::fd::BorrowedFd;
use std::rc::Rc;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use anyhow::ensure;
use futures::FutureExt;
use futures::StreamExt;
use futures::future::BoxFuture;
use futures::future::Either;
use futures::future::LocalBoxFuture;
use futures::stream::FuturesUnordered;
use kj_rs::KjMaybe;
use tokio::io::AsyncRead;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::net::TcpSocket;
use tokio::net::TcpStream;
use tokio::sync::watch;
use workerd_capnp::config;
use workerd_capnp::service;
use workerd_capnp::socket;
use workerd_server::bindings::Designator;
use workerd_server::bridge::ffi;
use workerd_server::channels::Channel;
use workerd_server::config::ActorStorageProvider;
use workerd_server::config::Factory;
use workerd_server::config::Overrides;
use workerd_server::config::Reporter;
use workerd_server::config::Server;
use workerd_server::config::WorkerPartition;
use workerd_server::listen;
use workerd_server::listen::HttpSocket;
use workerd_server::listen::ListenContext;
use workerd_server::listen::StreamListener;
use workerd_server::listen::loopback::Loopback;
use workerd_server::request_context;
use workerd_server::services::rewriter::HttpRewriter;

use crate::engine_error;

// Matches the maximum private host active/reservation bound. Each pump adds only bounded
// copy buffers and a normal 64 KiB-per-direction loopback pipe, never a full request body.
const MAX_CONNECTIONS: usize = 32;
const LOOPBACK_NAME: &str = "hyperlight-ingress";

enum Protocol {
    Http(Rc<HttpRewriter>),
    Tcp,
}

pub struct Plan {
    words: Vec<u64>,
    address: SocketAddr,
    designator: Designator,
    protocol: Protocol,
    config_sha256: [u8; 32],
}

pub type ConnectionContextProvider = dyn for<'fd, 'config> Fn(
    BorrowedFd<'fd>,
    &'config [u8; 32],
    u32,
) -> Result<request_context::Value>;

/// An owned post-admission stream, including a TLS wrapper supplied by the embedder.
pub trait IngressStream: AsyncRead + AsyncWrite + Unpin + Send {}

impl<T: AsyncRead + AsyncWrite + Unpin + Send> IngressStream for T {}

/// Returned only after trusted embedding code completes its actual admission.
/// The opaque value is not derived by workerd from a peer address or application headers.
pub struct AdmittedConnection {
    stream: Box<dyn IngressStream>,
    context: request_context::Value,
    target: Option<Designator>,
}

impl AdmittedConnection {
    pub fn new(stream: impl IngressStream + 'static, context: request_context::Value) -> Self {
        Self {
            stream: Box::new(stream),
            context,
            target: None,
        }
    }

    /// Selects an entrypoint and props in the already selected local Worker.
    /// Only the trusted admission provider supplies this route, never application headers.
    #[must_use]
    pub fn with_target(mut self, target: Designator) -> Self {
        self.target = Some(target);
        self
    }
}

pub type ConnectionAdmissionProvider =
    dyn Fn(TcpStream, [u8; 32], u32) -> LocalBoxFuture<'static, Result<AdmittedConnection>>;

#[derive(Clone)]
pub enum ConnectionAdmission {
    Descriptor(Rc<ConnectionContextProvider>),
    Stream(Rc<ConnectionAdmissionProvider>),
}

impl ConnectionAdmission {
    async fn admit(
        &self,
        stream: TcpStream,
        config: [u8; 32],
        worker: u32,
    ) -> Result<AdmittedConnection> {
        match self {
            Self::Descriptor(provider) => {
                let context = provider(stream.as_fd(), &config, worker)?;
                Ok(AdmittedConnection::new(stream, context))
            }
            Self::Stream(provider) => provider(stream, config, worker).await,
        }
    }
}

struct ConnectionTargets {
    server: Rc<Server>,
    default: Rc<dyn Channel>,
    selected_service: String,
}

impl ConnectionTargets {
    fn new(
        factory: &Factory,
        server: Rc<Server>,
        default: Rc<dyn Channel>,
        worker: u32,
    ) -> Result<Self> {
        let message = factory.config().map_err(engine_error)?;
        let services = message.get_root::<config::Reader>()?.get_services()?;
        ensure!(
            worker < services.len(),
            "admitted Worker index is out of range"
        );
        let service = services.get(worker);
        ensure!(
            matches!(service.which(), Ok(service::Which::Worker(_))),
            "admitted target is not a configured Worker"
        );
        let selected_service = service.get_name()?.to_str()?.to_owned();
        Ok(Self {
            server,
            default,
            selected_service,
        })
    }

    fn resolve(&self, target: Option<&Designator>) -> Result<Rc<dyn Channel>> {
        let Some(target) = target else {
            return Ok(Rc::clone(&self.default));
        };
        ensure!(
            target.service == self.selected_service,
            "admitted target must name the selected Worker {:?}",
            self.selected_service
        );
        self.server.resolve(target).map_err(engine_error)
    }
}

/// An owned local service that runs alongside ingress, then drains after the isolate drops.
/// Its future must not hold the factory alive.
pub struct OwnerTask {
    future: LocalBoxFuture<'static, Result<()>>,
    stop: Option<Box<dyn FnOnce()>>,
    completed: bool,
}

impl OwnerTask {
    pub fn new(future: LocalBoxFuture<'static, Result<()>>, stop: impl FnOnce() + 'static) -> Self {
        Self {
            future,
            stop: Some(Box::new(stop)),
            completed: false,
        }
    }

    async fn alongside(&mut self, main: impl Future<Output = Result<()>>) -> Result<()> {
        match futures::future::select(main.boxed_local(), self.future.as_mut()).await {
            Either::Left((result, _)) => result,
            Either::Right((result, _)) => {
                self.completed = true;
                result?;
                bail!("owned service ended before private ingress teardown")
            }
        }
    }

    async fn finish(mut self) -> Result<()> {
        if let Some(stop) = self.stop.take() {
            stop();
        }
        if !self.completed {
            self.future.await?;
        }
        Ok(())
    }
}

impl Plan {
    #[cfg(test)]
    fn parse(bytes: &[u8], name: &str) -> Result<Self> {
        let message = crate::read_config(bytes)?;
        Self::from_config(bytes, message.get_root::<config::Reader>()?, name)
    }

    pub fn from_config(bytes: &[u8], config: config::Reader<'_>, name: &str) -> Result<Self> {
        for service in config.get_services()? {
            ensure!(
                matches!(
                    service.which(),
                    Ok(service::Which::Worker(_) | service::Which::External(_))
                ),
                "guest ingress requires worker or external services and admitted channels"
            );
        }
        let sockets = config.get_sockets()?;
        ensure!(
            sockets.len() == 1,
            "guest ingress requires exactly one configured socket"
        );
        let socket = sockets.get(0);
        ensure!(
            socket.get_name()?.to_str()? == name,
            "configured ingress socket was not found"
        );
        let address: SocketAddr = socket.get_address()?.to_str()?.parse()?;
        let SocketAddr::V4(selector) = address else {
            bail!("guest ingress requires an IPv4 selector");
        };
        ensure!(
            !selector.ip().is_unspecified()
                && !selector.ip().is_multicast()
                && !selector.ip().is_broadcast()
                && selector.port() != 0,
            "guest ingress requires an explicit unicast IPv4 selector and nonzero port"
        );
        let protocol = match socket.which()? {
            socket::Which::Http(options) => {
                Protocol::Http(Rc::new(HttpRewriter::new(options?).map_err(engine_error)?))
            }
            socket::Which::Tcp(options) => {
                ensure!(
                    !options.has_tls_options(),
                    "private TCP ingress does not enable TLS"
                );
                Protocol::Tcp
            }
            _ => bail!("private ingress requires cleartext HTTP or TCP"),
        };
        let mut errors = Vec::new();
        let designator = Designator::from_reader(
            socket.get_service()?,
            "Hyperlight ingress".to_owned(),
            &mut errors,
        )?;
        ensure!(errors.is_empty(), "{}", errors.join("\n"));
        let mut worker_entry = false;
        for service in config.get_services()? {
            if service.get_name()?.to_str()? == designator.service {
                ensure!(
                    matches!(service.which(), Ok(service::Which::Worker(_))),
                    "guest ingress service must be a configured Worker"
                );
                worker_entry = true;
            }
        }
        ensure!(
            worker_entry,
            "guest ingress service must be a configured Worker"
        );
        let words = bytes
            .chunks_exact(8)
            .map(|word| word.try_into().map(u64::from_ne_bytes))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        Ok(Self {
            words,
            address,
            designator,
            protocol,
            config_sha256: ring::digest::digest(&ring::digest::SHA256, bytes)
                .as_ref()
                .try_into()?,
        })
    }
}

struct Connection<S> {
    stream: S,
    disconnected: BoxFuture<'static, Result<()>>,
    aborted: BoxFuture<'static, Result<()>>,
}

fn observe<S: AsFd + 'static>(stream: S) -> Connection<S> {
    let disconnected = kj_rs_io::when_write_disconnected(&stream)
        .map(|result| result.map_err(|error| engine_error(error.into())))
        .boxed();
    let error_fd = stream.as_fd().try_clone_to_owned();
    let aborted = async move {
        let socket =
            tokio::io::unix::AsyncFd::with_interest(error_fd?, tokio::io::Interest::ERROR)?;
        socket
            .ready(tokio::io::Interest::ERROR)
            .await?
            .retain_ready();
        bail!("private ingress socket reported transport error");
    }
    .boxed();
    Connection {
        stream,
        disconnected,
        aborted,
    }
}

async fn accept(
    listener: &tokio::net::TcpListener,
    address: SocketAddr,
) -> io::Result<Connection<TcpStream>> {
    let (stream, peer) = listener.accept().await?;
    let SocketAddr::V4(peer) = peer else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "private ingress peer is not IPv4",
        ));
    };
    if peer.ip().is_unspecified()
        || peer.ip().is_multicast()
        || peer.ip().is_broadcast()
        || peer.port() == 0
        || stream.local_addr()? != address
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid private ingress address",
        ));
    }
    if let Err(error) = stream.set_nodelay(true)
        && !matches!(
            error.raw_os_error(),
            Some(libc::EOPNOTSUPP | libc::ENOPROTOOPT)
        )
    {
        return Err(error);
    }
    // Known frontend transport metadata is not authenticated client identity. Standard HTTP
    // metadata policy supplies cf; the in-process transport deliberately has no TCP peer.
    Ok(observe(stream))
}

async fn pump<S>(connection: Connection<S>, pipe: listen::loopback::LoopbackStream) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let Connection {
        stream,
        disconnected,
        aborted,
    } = connection;
    let (mut raw_read, mut raw_write) = tokio::io::split(stream);
    let (mut pipe_read, mut pipe_write) = tokio::io::split(pipe);
    let local_write_shutdown = Cell::new(false);
    // Before we close our write side, independent HUP/reset detection cancels a blocked
    // request even when its unread body has filled the pipe. Once we deliberately send FIN,
    // a peer FIN can cause HUP with valid upload bytes still queued: preserve that read side.
    let mut disconnected = std::pin::pin!(disconnected.fuse());
    let mut aborted = std::pin::pin!(aborted.fuse());
    let mut incoming = std::pin::pin!(
        async {
            tokio::io::copy(&mut raw_read, &mut pipe_write).await?;
            pipe_write.shutdown().await
        }
        .fuse()
    );
    let mut outgoing = std::pin::pin!(
        async {
            tokio::io::copy(&mut pipe_read, &mut raw_write).await?;
            local_write_shutdown.set(true);
            raw_write.shutdown().await
        }
        .fuse()
    );
    let mut read_done = false;
    let mut write_done = false;
    while !read_done || !write_done {
        futures::select_biased! {
            result = aborted => {
                result?;
                unreachable!()
            }
            result = incoming => {
                result?;
                read_done = true;
            }
            result = outgoing => {
                result?;
                write_done = true;
            }
            result = disconnected => {
                result?;
                if !local_write_shutdown.get() {
                    return Ok(());
                }
            }
        }
    }
    Ok(())
}

/// Owns the acceptor and every pump alongside the unchanged Workerd listener. Only the
/// private host's owned ECANCELED transition is a successful drain; other accept errors fail.
async fn pump_listener<S, A, AF, F>(
    accept: A,
    loopback: Loopback,
    drain: watch::Sender<bool>,
    serving: F,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
    A: FnMut() -> AF,
    AF: Future<Output = io::Result<Connection<S>>>,
    F: Future<Output = workerd_server::Result<()>>,
{
    serve_connections(
        accept,
        move |connection| {
            let pipe = loopback.connect(LOOPBACK_NAME).map_err(engine_error)?;
            Ok(pump(connection, pipe).boxed_local())
        },
        drain,
        serving,
    )
    .await
}

async fn serve_connections<T, A, AF, F, C>(
    mut accept: A,
    mut start: C,
    drain: watch::Sender<bool>,
    serving: F,
) -> Result<()>
where
    A: FnMut() -> AF,
    AF: Future<Output = io::Result<T>>,
    F: Future<Output = workerd_server::Result<()>>,
    C: FnMut(T) -> Result<LocalBoxFuture<'static, Result<()>>>,
{
    enum Event<T> {
        Served(workerd_server::Result<()>),
        Pump(Result<()>),
        Accept(io::Result<T>),
    }
    let mut serving = std::pin::pin!(serving.fuse());
    let mut pumps: FuturesUnordered<LocalBoxFuture<'static, Result<()>>> = FuturesUnordered::new();
    let mut draining = false;
    let mut listener_done = false;
    loop {
        if draining && listener_done && pumps.is_empty() {
            return Ok(());
        }
        let event = {
            let accepting = if draining {
                Either::Left(std::future::pending())
            } else {
                Either::Right(accept())
            };
            let mut accepting = std::pin::pin!(accepting.fuse());
            futures::select_biased! {
                result = serving => Event::Served(result),
                result = pumps.select_next_some() => Event::Pump(result),
                result = accepting => Event::Accept(result),
            }
        };
        match event {
            Event::Served(result) => {
                result.map_err(engine_error)?;
                ensure!(draining, "Workerd listener ended before owned drain");
                listener_done = true;
            }
            Event::Pump(result) => {
                if let Err(error) = result {
                    eprintln!("Hyperlight ingress connection failed: {error}");
                }
            }
            Event::Accept(accepted) => match accepted {
                Err(error) if error.raw_os_error() == Some(libc::ECANCELED) => {
                    draining = true;
                    drain
                        .send(true)
                        .context("Workerd drain receiver was lost")?;
                }
                Err(error) => return Err(error).context("private ingress accept failed"),
                Ok(connection) => {
                    ensure!(
                        pumps.len() < MAX_CONNECTIONS,
                        "private ingress connection bound exceeded"
                    );
                    pumps.push(start(connection)?);
                }
            },
        }
    }
}

async fn configured(
    factory: Rc<Factory>,
    plan: Plan,
    channels: HashMap<String, Rc<dyn Channel>>,
    partition: Option<WorkerPartition>,
    connection_context: Option<ConnectionAdmission>,
    owner_task: &mut Option<OwnerTask>,
) -> Result<()> {
    let errors = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&errors);
    let reporter = Reporter::new(
        Box::new(move |error| sink.borrow_mut().push(error)),
        Box::new(|warning| eprintln!("Hyperlight configuration warning: {warning}")),
        false,
    );
    let worker_index = partition.as_ref().map(WorkerPartition::local_index);
    let server = match partition {
        Some(partition) => {
            Server::start_with_worker_partition(
                Rc::clone(&factory),
                &Overrides::default(),
                reporter,
                HashMap::new(),
                channels,
                partition,
            )
            .await
        }
        None => {
            Server::start_with_services(
                Rc::clone(&factory),
                &Overrides::default(),
                reporter,
                HashMap::new(),
                channels,
            )
            .await
        }
    }
    .map_err(engine_error)?;
    let serving = async {
        let channel = server.lookup(&plan.designator);
        ensure!(
            errors.borrow().is_empty(),
            "invalid Workerd configuration: {}",
            errors.borrow().join("\n")
        );
        let socket = TcpSocket::new_v4()?;
        socket.set_reuseaddr(true)?;
        socket.bind(plan.address)?;
        let listener = socket.listen(libc::SOMAXCONN.unsigned_abs())?;
        ensure!(
            listener.local_addr()? == plan.address,
            "private listener address mismatch"
        );
        if let Some(provider) = connection_context {
            let Protocol::Http(rewriter) = plan.protocol else {
                bail!("trusted connection context requires HTTP ingress");
            };
            let index = worker_index.context("connection context requires Worker admission")?;
            let targets = Rc::new(ConnectionTargets::new(
                &factory,
                Rc::clone(&server),
                channel,
                index,
            )?);
            composed_with_context(
                Rc::clone(&factory),
                rewriter,
                targets,
                provider,
                plan.config_sha256,
                index,
                || accept(&listener, plan.address),
            )
            .await
        } else {
            composed(
                Rc::clone(&factory),
                plan.protocol,
                plan.address.to_string(),
                channel,
                || accept(&listener, plan.address),
            )
            .await
        }
    };
    let result = match owner_task {
        Some(task) => task.alongside(serving).await,
        None => serving.await,
    };
    // Listener return is not the background-work barrier. Unlink workers and cancel their
    // waitUntil/provider futures while the named call still permits HOSTCALLs.
    drop(server);
    result
}

async fn composed_with_context<A, AF>(
    factory: Rc<Factory>,
    rewriter: Rc<HttpRewriter>,
    targets: Rc<ConnectionTargets>,
    provider: ConnectionAdmission,
    config_sha256: [u8; 32],
    worker: u32,
    mut accept: A,
) -> Result<()>
where
    A: FnMut() -> AF,
    AF: Future<Output = io::Result<Connection<TcpStream>>>,
{
    let (drain, mut draining) = watch::channel(false);
    let context = Rc::new(ListenContext {
        factory,
        draining: draining.clone(),
    });
    let shutdown = Rc::new(kj_hyper::server::Shutdown::new());
    let settings = Rc::new(kj_hyper::server::ServerSettings {
        websocket_errors: Some(ffi::new_jsgify_websocket_errors()),
        websocket_compression: kj_hyper::WebSocketCompression::MANUAL,
        ..kj_hyper::server::ServerSettings::default()
    });
    let stopping = Rc::clone(&shutdown);
    let serving = async move {
        while !*draining.borrow() {
            draining
                .changed()
                .await
                .map_err(|_| kj::failed!("private ingress drain sender was lost"))?;
        }
        stopping.shutdown();
        Ok(())
    };
    let accepting = || {
        let accepted = accept();
        let provider = provider.clone();
        let context = Rc::clone(&context);
        let targets = Rc::clone(&targets);
        let rewriter = Rc::clone(&rewriter);
        let settings = Rc::clone(&settings);
        let shutdown = Rc::clone(&shutdown);
        async move {
            let connection = accepted.await?;
            let task = async move {
                let Connection {
                    stream,
                    disconnected,
                    aborted,
                } = connection;
                let hangup = async move {
                    let mut disconnected = std::pin::pin!(disconnected.fuse());
                    let mut aborted = std::pin::pin!(aborted.fuse());
                    futures::select_biased! {
                        result = aborted => result,
                        result = disconnected => result,
                    }
                    .map_err(|error| kj::failed!("private ingress transport failed: {error}"))
                }
                .boxed();
                let admission = provider.admit(stream, config_sha256, worker).boxed_local();
                let (admitted, hangup) = match futures::future::select(admission, hangup).await {
                    Either::Left((result, hangup)) => (result?, hangup),
                    Either::Right((result, _admission)) => {
                        result.map_err(engine_error)?;
                        bail!("private ingress connection closed before admission");
                    }
                };
                let AdmittedConnection {
                    stream,
                    context: value,
                    target,
                } = admitted;
                let channel = targets.resolve(target.as_ref())?;
                let channel = request_context::channel(channel, request_context::new(value));
                let socket = Rc::new(HttpSocket {
                    channel,
                    rewriter,
                    physical_protocol: "http",
                    tls: None,
                });
                let handler = listen::http_connection_handler(Rc::clone(&context), socket, None);
                kj_hyper::server::serve_connection(
                    stream,
                    hangup,
                    workerd_server::services::header_table(&context.factory),
                    settings,
                    handler,
                    &shutdown,
                )
                .await
                .map_err(engine_error)
            };
            Ok(task.boxed_local())
        }
    };
    serve_connections(accepting, Ok, drain, serving).await
}

async fn composed<S, A, AF>(
    factory: Rc<Factory>,
    protocol: Protocol,
    authority: String,
    channel: Rc<dyn Channel>,
    accept: A,
) -> Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + 'static,
    A: FnMut() -> AF,
    AF: Future<Output = io::Result<Connection<S>>>,
{
    factory.loopback().enable();
    let internal = StreamListener::Loopback(
        factory
            .loopback()
            .listen(LOOPBACK_NAME)
            .map_err(engine_error)?,
    );
    let (drain, draining) = watch::channel(false);
    let context = Rc::new(ListenContext {
        factory: Rc::clone(&factory),
        draining,
    });
    let serving: LocalBoxFuture<'_, workerd_server::Result<()>> = match protocol {
        Protocol::Http(rewriter) => Box::pin(listen::listen_http(
            context,
            internal,
            Rc::new(HttpSocket {
                channel,
                rewriter,
                physical_protocol: "http",
                tls: None,
            }),
        )),
        Protocol::Tcp => Box::pin(listen::listen_tcp(
            context, internal, channel, None, authority,
        )),
    };
    pump_listener(accept, factory.loopback().clone(), drain, serving).await
}

pub fn serve(
    runtime: &mut kj_rs_tokio::Runtime,
    plan: Plan,
    channels: HashMap<String, Rc<dyn Channel>>,
) -> Result<()> {
    serve_with_admission(runtime, plan, channels, None, None, None, None)
}

pub fn serve_with_admission(
    runtime: &mut kj_rs_tokio::Runtime,
    mut plan: Plan,
    channels: HashMap<String, Rc<dyn Channel>>,
    partition: Option<WorkerPartition>,
    actor_storage: Option<Rc<dyn ActorStorageProvider>>,
    connection_context: Option<ConnectionAdmission>,
    mut owner_task: Option<OwnerTask>,
) -> Result<()> {
    let factory = ffi::bootstrap(
        runtime.context(),
        std::mem::take(&mut plan.words),
        &crate::options(),
        KjMaybe::None,
        KjMaybe::None,
    )?;
    runtime
        .block_on(async move {
            let factory = Factory::new(factory);
            let factory = Rc::new(match actor_storage {
                Some(provider) => factory.with_actor_storage_provider(provider),
                None => factory,
            });
            let result = configured(
                Rc::clone(&factory),
                plan,
                channels,
                partition,
                connection_context,
                &mut owner_task,
            )
            .await;
            let settled = factory.settle_tasks().await.map_err(engine_error);
            let retired = Rc::try_unwrap(factory).map(drop).map_err(|_| {
                anyhow::anyhow!("worker factory remains referenced after ingress teardown")
            });
            let auxiliary = match owner_task {
                Some(task) if retired.is_ok() => task.finish().await,
                Some(task) => {
                    drop(task);
                    Err(anyhow::anyhow!(
                        "isolate teardown failed; context cleanup requires owner retirement"
                    ))
                }
                None => Ok(()),
            };
            let mut errors = Vec::new();
            for outcome in [result, settled, retired, auxiliary] {
                if let Err(error) = outcome {
                    errors.push(format!("{error:#}"));
                }
            }
            ensure!(errors.is_empty(), "{}", errors.join("; "));
            Ok(())
        })
        .map_err(engine_error)?
}

#[cfg(test)]
#[path = "ingress-test.rs"]
mod tests;
