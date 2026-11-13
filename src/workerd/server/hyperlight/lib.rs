// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Finite HTTP and terminal private ingress using standard Workerd configuration.
//! Call completion is not a capture-safe or background-work-retirement signal.

#![cfg_attr(test, deny(unnameable_test_items))]

/// Standard Config types used by the public service-factory interface.
pub use workerd_capnp as schema;

#[cfg(target_os = "linux")]
mod artifact;
mod finite;
mod framing;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod ingress;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use ingress::AdmittedConnection;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use ingress::ConnectionAdmissionProvider;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use ingress::ConnectionContextProvider;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use ingress::IngressStream;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub use ingress::OwnerTask;
pub mod protocol;
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
mod queue;

mod selection;
use std::cell::Cell;
use std::cell::RefCell;
use std::collections::HashMap;
use std::pin::Pin;
use std::rc::Rc;
use std::time::SystemTime;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use kj::http::Headers;
use kj::http::HeadersRef;
use kj::http::Method;
use kj::http::Service;
use kj::http::ServiceResponse;
use kj::io::AsyncInputStream;
use kj_rs::KjMaybe;
use kj_rs::KjOwn;
pub use selection::selected_worker_from_socket;
use worker::Interface as _;
use workerd_capnp::config;
use workerd_capnp::service;
use workerd_capnp::socket;
use workerd_server::bindings::Designator;
use workerd_server::bridge;
use workerd_server::bridge::ffi;
use workerd_server::channels::Channel;
use workerd_server::channels::PendingToken;
use workerd_server::channels::RequestMetadata;
use workerd_server::channels::TokenUsage;
use workerd_server::channels::WorkerInterface;
use workerd_server::config::ActorStorageProvider;
use workerd_server::config::Factory;
use workerd_server::config::Overrides;
use workerd_server::config::Reporter;
use workerd_server::config::Server;
use workerd_server::config::WorkerEndpoint;
use workerd_server::config::WorkerPartition;
use workerd_server::listen::HttpSocket;
use workerd_server::loader::DynamicWorkerLoader;
use workerd_server::services::header_table;
use workerd_server::services::rewriter::HttpRewriter;

/// No ambient native network authority is granted by the standalone guest entry.
struct DeniedInternet;

impl Channel for DeniedInternet {
    fn start_request(
        &self,
        _metadata: KjOwn<RequestMetadata>,
    ) -> workerd_server::Result<KjOwn<WorkerInterface>> {
        Err(kj::failed!(
            "Hyperlight executor has no admitted outbound channel"
        ))
    }

    fn token(&self, _usage: TokenUsage) -> workerd_server::Result<KjOwn<PendingToken>> {
        Err(kj::failed!(
            "Hyperlight executor outbound channel is not transferable"
        ))
    }
}

struct Configured {
    socket: Rc<HttpSocket>,
    server: Rc<Server>,
    factory: Rc<Factory>,
    reply_limit: usize,
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "a map_err adapter consumes the engine error"
)]
fn engine_error(error: workerd_server::Error) -> anyhow::Error {
    anyhow::anyhow!("Workerd engine error: {}", error.description())
}

/// Owns the engine on its single thread. Supplied channels remain embedder capabilities.
pub struct Executor {
    configured: Option<Configured>,
    configure_started: bool,
    channels: ServiceChannels,
    service_factory: Option<Box<ServiceFactory>>,
    actor_storage: Option<Rc<dyn ActorStorageProvider>>,
    actor_storage_factory: Option<Box<ActorStorageFactory>>,
    worker_admission: Option<u32>,
    worker_admission_factory: Option<Box<WorkerAdmissionFactory>>,
    worker_endpoints: Option<HashMap<String, Rc<dyn WorkerEndpoint>>>,
    dynamic_loader: Option<Rc<dyn DynamicWorkerLoader>>,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    connection_context: Option<ingress::ConnectionAdmission>,
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    owner_task: Option<OwnerTask>,
    runtime: kj_rs_tokio::Runtime,
}

type ServiceChannels = HashMap<String, Rc<dyn Channel>>;
type ServiceFactory = dyn for<'a> FnOnce(config::Reader<'a>) -> Result<ServiceChannels>;
type ActorStorageFactory =
    dyn for<'a> FnOnce(config::Reader<'a>) -> Result<Rc<dyn ActorStorageProvider>>;
type WorkerAdmissionFactory = dyn for<'a> FnOnce(config::Reader<'a>) -> Result<u32>;

fn read_config(
    bytes: &[u8],
) -> Result<capnp::message::Reader<impl capnp::message::ReaderSegments + '_>> {
    if bytes.is_empty() || !bytes.len().is_multiple_of(8) {
        bail!("invalid standard Workerd configuration");
    }
    let mut remaining = bytes;
    let options = *capnp::message::ReaderOptions::new().traversal_limit_in_words(Some(bytes.len()));
    let message = capnp::serialize::read_message_from_flat_slice(&mut remaining, options)?;
    if !remaining.is_empty() {
        bail!("trailing bytes after standard Workerd configuration");
    }
    message.get_root::<config::Reader>()?.total_size()?;
    Ok(message)
}

fn finite_ingress(
    config: config::Reader<'_>,
    socket_name: &str,
) -> Result<(Rc<HttpRewriter>, Designator)> {
    // The finite entry accepts admitted Workers, not ambient transports or filesystem services.
    for service in config.get_services()? {
        if !matches!(service.which(), Ok(service::Which::Worker(_))) {
            bail!("finite guest entry requires worker services and admitted channels");
        }
    }
    let mut selected = None;
    for socket in config.get_sockets()? {
        if socket.get_name()?.to_str()? == socket_name {
            if selected.is_some() {
                bail!("duplicate configured ingress socket name");
            }
            selected = Some(socket);
        }
    }
    let socket = selected.context("configured ingress socket was not found")?;
    let http = match socket.which()? {
        socket::Which::Http(options) => options?,
        _ => bail!("finite guest ingress requires an HTTP socket"),
    };
    let rewriter = Rc::new(HttpRewriter::new(http).map_err(engine_error)?);
    let mut errors = Vec::new();
    let designator = Designator::from_reader(
        socket.get_service()?,
        "Hyperlight ingress".to_owned(),
        &mut errors,
    )?;
    if !errors.is_empty() {
        bail!("{}", errors.join("\n"));
    }
    Ok((rewriter, designator))
}

fn options() -> ffi::ServeOrTestOptions {
    ffi::ServeOrTestOptions {
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
    }
}

impl Executor {
    pub fn new(mut channels: HashMap<String, Rc<dyn Channel>>) -> Result<Self> {
        channels
            .entry("internet".to_owned())
            .or_insert_with(|| Rc::new(DeniedInternet));
        Ok(Self {
            configured: None,
            runtime: kj_rs_tokio::Runtime::new().map_err(engine_error)?,
            configure_started: false,
            channels,
            service_factory: None,
            actor_storage: None,
            actor_storage_factory: None,
            worker_admission: None,
            worker_admission_factory: None,
            worker_endpoints: None,
            dynamic_loader: None,
            #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
            connection_context: None,
            #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
            owner_task: None,
        })
    }

    /// Constructs capabilities from the same validated config used by the engine.
    /// The factory is consumed once, before bootstrap, and a failure cannot be retried.
    pub fn with_service_factory(
        factory: impl for<'a> FnOnce(config::Reader<'a>) -> Result<HashMap<String, Rc<dyn Channel>>>
        + 'static,
    ) -> Result<Self> {
        let mut executor = Self::new(HashMap::new())?;
        executor.service_factory = Some(Box::new(factory));
        Ok(executor)
    }

    /// Selects one configured Worker before opening the guest call queue. This is a trusted
    /// embedding option, not a named-call argument or a guest-selected routing authority.
    pub fn with_worker_admission(mut self, index: u32) -> Result<Self> {
        if self.configure_started
            || self.worker_admission.is_some()
            || self.worker_admission_factory.is_some()
        {
            bail!("Worker admission must be selected once before configuration");
        }
        self.worker_admission = Some(index);
        Ok(self)
    }

    /// Supplies every other configured Worker's endpoint for an explicitly selected owner.
    /// Endpoint names are validated against the admitted Config before bootstrap.
    pub fn with_worker_endpoints(
        mut self,
        endpoints: HashMap<String, Rc<dyn WorkerEndpoint>>,
    ) -> Result<Self> {
        if self.configure_started
            || self.worker_endpoints.is_some()
            || (self.worker_admission.is_none() && self.worker_admission_factory.is_none())
        {
            bail!("Worker endpoints require one selected admission before configuration");
        }
        self.worker_endpoints = Some(endpoints);
        Ok(self)
    }

    /// Places dynamically loaded descendants outside the selected Worker's factory.
    pub fn with_dynamic_worker_loader(
        mut self,
        loader: Rc<dyn DynamicWorkerLoader>,
    ) -> Result<Self> {
        if self.configure_started
            || self.dynamic_loader.is_some()
            || (self.worker_admission.is_none() && self.worker_admission_factory.is_none())
        {
            bail!("dynamic Worker placement requires selected admission before configuration");
        }
        self.dynamic_loader = Some(loader);
        Ok(self)
    }

    /// Derives the trusted selection from the same verified Config used for bootstrap.
    pub fn with_worker_admission_factory(
        mut self,
        select: impl for<'a> FnOnce(config::Reader<'a>) -> Result<u32> + 'static,
    ) -> Result<Self> {
        if self.configure_started
            || self.worker_admission.is_some()
            || self.worker_admission_factory.is_some()
        {
            bail!("Worker admission must be selected once before configuration");
        }
        self.worker_admission_factory = Some(Box::new(select));
        Ok(self)
    }

    /// Supplies trusted provenance before a connection's protocol or Worker event execution.
    /// The callback is local embedding code; it receives no application-selected context ID.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn with_connection_context_provider(
        mut self,
        provider: impl for<'fd, 'config> Fn(
            std::os::fd::BorrowedFd<'fd>,
            &'config [u8; 32],
            u32,
        ) -> Result<workerd_server::request_context::Value>
        + 'static,
    ) -> Result<Self> {
        if self.configure_started || self.connection_context.is_some() {
            bail!("connection context must be selected once before configuration");
        }
        if self.worker_admission.is_none() && self.worker_admission_factory.is_none() {
            bail!("connection context requires explicit Worker admission");
        }
        self.connection_context = Some(ingress::ConnectionAdmission::Descriptor(Rc::new(provider)));
        Ok(self)
    }

    /// Owns the accepted stream through asynchronous trusted admission before protocol dispatch.
    /// The callback must retain and clean up its actual native admission on refusal/cancellation.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn with_connection_admission(
        mut self,
        provider: impl Fn(
            tokio::net::TcpStream,
            [u8; 32],
            u32,
        )
            -> futures::future::LocalBoxFuture<'static, Result<AdmittedConnection>>
        + 'static,
    ) -> Result<Self> {
        if self.configure_started || self.connection_context.is_some() {
            bail!("connection admission must be selected once before configuration");
        }
        if self.worker_admission.is_none() && self.worker_admission_factory.is_none() {
            bail!("connection admission requires explicit Worker selection");
        }
        self.connection_context = Some(ingress::ConnectionAdmission::Stream(Rc::new(provider)));
        Ok(self)
    }

    /// Owns a local cleanup service through listener shutdown and isolate destruction.
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    pub fn with_owner_task(mut self, task: OwnerTask) -> Result<Self> {
        if self.configure_started || self.owner_task.is_some() {
            bail!("owner task must be selected once before configuration");
        }
        if self.connection_context.is_none() {
            bail!("owner cleanup task requires trusted connection context");
        }
        self.owner_task = Some(task);
        Ok(self)
    }

    /// The configured response budget, also available to typed event embedders.
    pub fn max_response_bytes(&self) -> Result<usize> {
        Ok(self
            .configured
            .as_ref()
            .context("executor is not configured")?
            .reply_limit)
    }

    /// Supplies trusted persistent namespace capabilities to finite Config dispatch.
    pub fn with_actor_storage_provider(
        mut self,
        provider: Rc<dyn ActorStorageProvider>,
    ) -> Result<Self> {
        if self.configure_started
            || self.actor_storage.is_some()
            || self.actor_storage_factory.is_some()
        {
            bail!("actor storage must be selected once before configuration");
        }
        self.actor_storage = Some(provider);
        Ok(self)
    }

    /// Builds explicit namespace capabilities from the exact admitted Config before linking.
    pub fn with_actor_storage_factory(
        mut self,
        factory: impl for<'a> FnOnce(config::Reader<'a>) -> Result<Rc<dyn ActorStorageProvider>>
        + 'static,
    ) -> Result<Self> {
        if self.configure_started
            || self.actor_storage.is_some()
            || self.actor_storage_factory.is_some()
        {
            bail!("actor storage must be selected once before configuration");
        }
        self.actor_storage_factory = Some(Box::new(factory));
        Ok(self)
    }

    /// Dispatches to the same Config-selected service as finite ingress, without a JS adapter.
    pub fn run_scheduled(
        &mut self,
        time: &SystemTime,
        cron: &str,
    ) -> Result<worker::ScheduledResult> {
        let time = *time;
        let cron = cron.to_owned();
        let channel = Rc::clone(
            &self
                .configured
                .as_ref()
                .context("executor is not configured")?
                .socket
                .channel,
        );
        self.runtime
            .block_on(async move {
                let metadata = ffi::new_request_metadata(KjMaybe::None, KjMaybe::None);
                let mut target = worker::CxxWorkerInterface::new(channel.start_request(metadata)?);
                target.run_scheduled(&time, &cron).await
            })
            .map_err(engine_error)?
            .map_err(engine_error)
    }

    /// Owns one existing custom event through completion on the Config-selected service.
    pub fn custom_event(
        &mut self,
        event: KjOwn<worker::CustomEvent>,
    ) -> Result<worker::CustomEventResult> {
        let channel = Rc::clone(
            &self
                .configured
                .as_ref()
                .context("executor is not configured")?
                .socket
                .channel,
        );
        self.runtime
            .block_on(async move {
                let metadata = ffi::new_request_metadata(KjMaybe::None, KjMaybe::None);
                let mut target = worker::CxxWorkerInterface::new(channel.start_request(metadata)?);
                target.custom_event(event).await
            })
            .map_err(engine_error)?
            .map_err(engine_error)
    }

    /// Dispatches only to an explicit admitted actor channel, not the HTTP socket service.
    pub fn run_alarm(
        &mut self,
        namespace_key: &str,
        actor_id: &str,
        time: &SystemTime,
        retry_count: u32,
    ) -> Result<worker::AlarmResult> {
        let configured = self
            .configured
            .as_ref()
            .context("executor is not configured")?;
        let channel = configured
            .server
            .actor_channel(namespace_key, actor_id)
            .map_err(engine_error)?;
        let time = *time;
        self.runtime
            .block_on(async move {
                let metadata = ffi::new_request_metadata(KjMaybe::None, KjMaybe::None);
                let mut target = worker::CxxWorkerInterface::new(channel.start_request(metadata)?);
                target.run_alarm(&time, retry_count).await
            })
            .map_err(engine_error)?
            .map_err(engine_error)
    }

    /// Dispatches only names explicitly registered by a downstream entry.
    /// A handled call returns bytes through the existing device result, not console output.
    pub fn dispatch_registered_string(
        &mut self,
        frame: &[u8],
        capacity: usize,
        names: &[&str],
        handle: impl FnOnce(&mut Self, &str, String) -> Result<Vec<u8>>,
    ) -> Result<Option<Vec<u8>>> {
        validate_registered_names(names)?;
        if names.is_empty() {
            return Ok(None);
        }
        let Some((name, argument)) = protocol::decode_string_call(frame, capacity, names)? else {
            return Ok(None);
        };
        let limit = self.max_response_bytes()?;
        let response = handle(self, &name, argument)?;
        if response.len() > limit {
            bail!("registered call result exceeds the configured response budget");
        }
        Ok(Some(response))
    }

    /// Dispatches finite operations. The queue owner consumes the executor for terminal serve.
    pub fn dispatch(&mut self, call: protocol::Call) -> Result<Vec<u8>> {
        match call {
            protocol::Call::Configure {
                config,
                socket,
                reply_limit,
            } => {
                self.configure(&config, &socket, reply_limit)?;
                Ok(Vec::new())
            }
            protocol::Call::Fetch(request) => self.fetch(request),
            protocol::Call::ConfigureArtifact {
                length,
                budget,
                digest,
                socket,
                reply_limit,
            } => {
                if self.configure_started {
                    bail!("executor configuration may be attempted only once");
                }
                #[cfg(target_os = "linux")]
                {
                    let bytes = artifact::load(length, budget, &digest)?;
                    self.configure(&bytes, &socket, reply_limit)?;
                    Ok(Vec::new())
                }
                #[cfg(not(target_os = "linux"))]
                {
                    let _ = (length, budget, digest, socket, reply_limit);
                    bail!("guest artifact configuration requires Linux");
                }
            }
            protocol::Call::ServeArtifact { .. } => {
                bail!("serve_artifact requires the terminal guest run loop")
            }
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    /// Starts terminal ingress directly from the same verified fixed-path artifact as the
    /// named-call entry. Native process entrypoints need not depend on a host queue driver.
    pub fn serve_artifact(
        self,
        length: usize,
        budget: usize,
        digest: &[u8; 32],
        socket: &str,
    ) -> Result<()> {
        self.serve_config(|| artifact::load(length, budget, digest), socket)
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn serve_config(mut self, load: impl FnOnce() -> Result<Vec<u8>>, socket: &str) -> Result<()> {
        if self.configure_started {
            self.close()?;
            bail!("serve_artifact requires an unconfigured executor");
        }
        let bytes = load()?;
        let (plan, channels) = self.prepare_ingress(&bytes, socket)?;
        // The consumed executor drops its Runtime before the caller writes status or ACKs.
        if self.worker_admission.is_some() {
            let message = read_config(&bytes)?;
            let partition = self.take_worker_partition(message.get_root::<config::Reader>()?)?;
            ingress::serve_with_admission(
                &mut self.runtime,
                plan,
                channels,
                partition,
                self.actor_storage.take(),
                self.connection_context.take(),
                self.owner_task.take(),
            )
        } else {
            ingress::serve(&mut self.runtime, plan, channels)
        }
    }

    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    fn prepare_ingress(
        &mut self,
        bytes: &[u8],
        socket: &str,
    ) -> Result<(ingress::Plan, ServiceChannels)> {
        if (self.actor_storage.is_some() || self.actor_storage_factory.is_some())
            && self.worker_admission.is_none()
            && self.worker_admission_factory.is_none()
        {
            bail!("terminal ingress does not accept external actor storage");
        }
        if self.configure_started {
            bail!("serve_artifact requires an unconfigured executor");
        }
        let message = read_config(bytes)?;
        let config = message.get_root::<config::Reader>()?;
        let plan = ingress::Plan::from_config(bytes, config, socket)?;
        self.begin_configuration(config)?;
        Ok((plan, std::mem::take(&mut self.channels)))
    }

    fn configure(&mut self, bytes: &[u8], socket_name: &str, reply_limit: usize) -> Result<()> {
        if self.configure_started {
            bail!("executor configuration may be attempted only once");
        }
        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        if self.connection_context.is_some() {
            self.configure_started = true;
            bail!("connection-bound executor requires terminal private ingress");
        }
        if reply_limit == 0 {
            bail!("invalid response limit");
        }
        let message = read_config(bytes)?;
        let config = message.get_root::<config::Reader>()?;
        let (rewriter, designator) = finite_ingress(config, socket_name)?;
        let words = bytes
            .chunks_exact(8)
            .map(|word| word.try_into().map(u64::from_ne_bytes))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        self.begin_configuration(config)?;
        let partition = self.take_worker_partition(config)?;
        let factory = ffi::bootstrap(
            self.runtime.context(),
            words,
            &options(),
            KjMaybe::None,
            KjMaybe::None,
        )?;
        let factory = Factory::new(factory);
        let factory = Rc::new(match self.actor_storage.take() {
            Some(provider) => factory.with_actor_storage_provider(provider),
            None => factory,
        });
        let channels = std::mem::take(&mut self.channels);
        let configured = self
            .runtime
            .block_on(async move {
                let errors = Rc::new(RefCell::new(Vec::new()));
                let sink = Rc::clone(&errors);
                let reporter = Reporter::new(
                    Box::new(move |message| sink.borrow_mut().push(message)),
                    Box::new(|message| eprintln!("Hyperlight configuration warning: {message}")),
                    false,
                );
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
                        .await?
                    }
                    None => {
                        Server::start_with_services(
                            Rc::clone(&factory),
                            &Overrides::default(),
                            reporter,
                            HashMap::new(),
                            channels,
                        )
                        .await?
                    }
                };
                let channel = server.lookup(&designator);
                if !errors.borrow().is_empty() {
                    return Err(kj::failed!(
                        "invalid Workerd configuration: {}",
                        errors.borrow().join("\n")
                    ));
                }
                Ok(Configured {
                    socket: Rc::new(HttpSocket {
                        channel,
                        rewriter,
                        physical_protocol: "http",
                        tls: None,
                    }),
                    server,
                    factory: Rc::clone(&factory),
                    reply_limit,
                })
            })
            .map_err(engine_error)?
            .map_err(engine_error)?;
        self.configured = Some(configured);
        Ok(())
    }

    fn take_worker_partition(
        &mut self,
        config: config::Reader<'_>,
    ) -> Result<Option<WorkerPartition>> {
        self.worker_admission
            .map(|index| {
                let partition = WorkerPartition::new(
                    config,
                    index,
                    self.worker_endpoints.take().unwrap_or_default(),
                )
                .map_err(engine_error)?;
                Ok(match self.dynamic_loader.take() {
                    Some(loader) => partition.with_dynamic_loader(loader),
                    None => partition,
                })
            })
            .transpose()
    }

    fn begin_configuration(&mut self, config: config::Reader<'_>) -> Result<()> {
        if self.configure_started {
            bail!("executor configuration may be attempted only once");
        }
        self.configure_started = true;
        if let Some(select) = self.worker_admission_factory.take() {
            self.worker_admission = Some(select(config)?);
        }
        if let Some(index) = self.worker_admission {
            let services = config.get_services()?;
            if index >= services.len()
                || !matches!(services.get(index).which(), Ok(service::Which::Worker(_)))
            {
                bail!("selected Worker index does not identify a configured Worker");
            }
        }
        if let Some(make_storage) = self.actor_storage_factory.take() {
            self.actor_storage = Some(make_storage(config)?);
        }
        if let Some(make_services) = self.service_factory.take() {
            let mut channels = make_services(config)?;
            channels
                .entry("internet".to_owned())
                .or_insert_with(|| Rc::new(DeniedInternet));
            self.channels = channels;
        }
        Ok(())
    }

    fn fetch(&mut self, request: Vec<u8>) -> Result<Vec<u8>> {
        framing::validate(&request)?;
        let configured = self
            .configured
            .as_ref()
            .context("executor is not configured")?;
        let factory = Rc::clone(&configured.factory);
        let socket = Rc::clone(&configured.socket);
        let reply_limit = configured.reply_limit;
        self.runtime
            .block_on(async move {
                let (io, output) = finite::Exchange::new(request, reply_limit);
                let handler = Rc::new(FiniteHandler {
                    factory: Rc::clone(&factory),
                    socket,
                    requests: Cell::new(0),
                    failure: RefCell::new(None),
                });
                let shutdown = kj_hyper::server::Shutdown::new();
                kj_hyper::server::serve_connection(
                    io,
                    Box::pin(std::future::pending()),
                    header_table(&factory),
                    Rc::new(kj_hyper::server::ServerSettings::default()),
                    Rc::clone(&handler) as Rc<dyn kj_hyper::server::Handler>,
                    &shutdown,
                )
                .await?;
                if let Some(error) = handler.failure.borrow_mut().take() {
                    return Err(error);
                }
                let response = std::mem::take(
                    &mut *output
                        .lock()
                        .map_err(|_| kj::failed!("HTTP reply buffer lock poisoned"))?,
                );
                if response.is_empty() {
                    return Err(kj::failed!("finite HTTP call produced no response"));
                }
                Ok(response)
            })
            .map_err(engine_error)?
            .map_err(engine_error)
    }

    /// Ends this engine; this is teardown, not a snapshot-safe-point operation.
    pub fn close(mut self) -> Result<()> {
        if let Some(configured) = self.configured.take() {
            let Configured {
                socket,
                server,
                factory,
                ..
            } = configured;
            drop(socket);
            drop(server);
            self.runtime
                .block_on(async move { factory.settle_tasks().await })
                .map_err(engine_error)?
                .map_err(engine_error)?;
        }
        Ok(())
    }
}

struct FiniteHandler {
    factory: Rc<Factory>,
    socket: Rc<HttpSocket>,
    requests: Cell<usize>,
    failure: RefCell<Option<workerd_server::Error>>,
}

#[async_trait::async_trait(?Send)]
impl kj_hyper::server::Handler for FiniteHandler {
    async fn request<'a>(
        &'a self,
        method: Method,
        url: &'a [u8],
        headers: HeadersRef<'a>,
        body: Pin<&'a mut AsyncInputStream>,
        response: ServiceResponse<'a>,
    ) -> workerd_server::Result<()> {
        let result = async {
            if self.requests.replace(self.requests.get() + 1) != 0 {
                return Err(kj::failed!(
                    "finite ingress accepts one HTTP request per call"
                ));
            }
            if headers.get(kj::http::HeaderId::UPGRADE).is_some() {
                return Err(kj::not_implemented!(
                    "finite ingress does not support HTTP upgrades"
                ));
            }
            let table = header_table(&self.factory);
            let url =
                std::str::from_utf8(url).map_err(|_| kj::failed!("request URL is not UTF-8"))?;
            let Some((request, cf_blob)) = self
                .socket
                .rewriter
                .rewrite_incoming_request(table, url, "http", headers)?
            else {
                return workerd_server::services::send_error(
                    response,
                    400,
                    "Bad Request",
                    &Headers::new(table),
                )
                .await;
            };
            let metadata = ffi::new_request_metadata(cf_blob.as_deref().into(), KjMaybe::None);
            let mut worker =
                worker::CxxWorkerInterface::new(self.socket.channel.start_request(metadata)?);
            let headers = request.headers.as_deref().map_or(headers, HeadersRef::from);
            let mut response = bridge::rewriting_response(
                response.into_ffi(),
                table,
                self.socket.rewriter.response_edits(),
            )?;
            worker
                .request(
                    method,
                    request.url.as_bytes(),
                    headers,
                    body,
                    ServiceResponse::from(response.as_mut()),
                )
                .await
        }
        .await;
        if let Err(error) = &result {
            *self.failure.borrow_mut() = Some(error.clone());
        }
        result
    }

    async fn connect<'a>(
        &'a self,
        _host: &'a [u8],
        _headers: HeadersRef<'a>,
        _connect: kj_hyper::server::Connect,
    ) -> workerd_server::Result<()> {
        let error = kj::not_implemented!("finite ingress does not support CONNECT");
        *self.failure.borrow_mut() = Some(error.clone());
        Err(error)
    }
}

/// Runs the generic guest entry with no product provider implementation.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run() -> Result<()> {
    run_with_services(HashMap::new())
}

/// The generic entry/run loop; a downstream embedder supplies admitted provider channels here.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_with_services(
    channels: impl IntoIterator<Item = (String, Rc<dyn Channel>)>,
) -> Result<()> {
    run_with_executor(Executor::new(channels.into_iter().collect())?)
}

/// Opens the standard call queue after all trusted embedding options have been installed.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_with_executor(executor: Executor) -> Result<()> {
    let queue = queue::Queue::open()?;
    run_executor(queue, executor)
}

/// Defers capability construction until the actual standard config has been validated.
/// Artifact calls keep their existing length, digest and fixed-path admission checks.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_with_service_factory(
    factory: impl for<'a> FnOnce(config::Reader<'a>) -> Result<HashMap<String, Rc<dyn Channel>>>
    + 'static,
) -> Result<()> {
    let queue = queue::Queue::open()?;
    run_executor(queue, Executor::with_service_factory(factory)?)
}

/// An opt-in entry for existing one-String operations supplied by an embedder.
/// Standard names and their argument/result contracts cannot be overridden.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_with_service_factory_and_calls(
    factory: impl for<'a> FnOnce(config::Reader<'a>) -> Result<ServiceChannels> + 'static,
    names: &'static [&'static str],
    handle: impl FnMut(&mut Executor, &str, String) -> Result<Vec<u8>>,
) -> Result<()> {
    run_with_executor_and_calls(Executor::with_service_factory(factory)?, names, handle)
}

/// Runs an explicitly constructed executor, retaining its admitted storage capabilities.
#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
pub fn run_with_executor_and_calls(
    executor: Executor,
    names: &'static [&'static str],
    handle: impl FnMut(&mut Executor, &str, String) -> Result<Vec<u8>>,
) -> Result<()> {
    validate_registered_names(names)?;
    let queue = queue::Queue::open()?;
    run_executor_with_calls(queue, executor, names, handle)
}

fn validate_registered_names(names: &[&str]) -> Result<()> {
    for (index, name) in names.iter().enumerate() {
        if name.is_empty()
            || matches!(
                *name,
                "configure" | "configure_artifact" | "serve_artifact" | "fetch"
            )
            || names[..index].contains(name)
        {
            bail!("invalid, duplicate or reserved registered call name");
        }
    }
    Ok(())
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn run_executor(queue: queue::Queue, executor: Executor) -> Result<()> {
    run_executor_with_calls(queue, executor, &[], |_, _, _| {
        bail!("no additional calls registered")
    })
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn run_executor_with_calls(
    mut queue: queue::Queue,
    executor: Executor,
    names: &[&str],
    handle: impl FnMut(&mut Executor, &str, String) -> Result<Vec<u8>>,
) -> Result<()> {
    let mut executor = Some(executor);
    let result = run_calls(&mut queue, &mut executor, names, handle);
    let closed = executor.map_or(Ok(()), Executor::close);
    if let Err(error) = &closed {
        eprintln!("Hyperlight executor teardown failed: {error}");
    }
    result?;
    closed
}

#[cfg(all(target_os = "linux", target_arch = "x86_64"))]
fn run_calls(
    queue: &mut queue::Queue,
    executor: &mut Option<Executor>,
    names: &[&str],
    mut handle: impl FnMut(&mut Executor, &str, String) -> Result<Vec<u8>>,
) -> Result<()> {
    loop {
        let capacity = queue.capacity();
        let frame = queue.next()?;
        let additional = if names.is_empty() {
            Ok(None)
        } else {
            executor
                .as_mut()
                .context("executor instance is retired")
                .and_then(|executor| {
                    executor.dispatch_registered_string(frame, capacity, names, &mut handle)
                })
        };
        let call = match additional {
            Ok(Some(response)) => {
                queue.complete(0, &response)?;
                continue;
            }
            Ok(None) => protocol::decode(frame, capacity),
            Err(error) => Err(error),
        };
        let result = call.and_then(|call| {
            if let protocol::Call::ServeArtifact {
                length,
                budget,
                digest,
                socket,
            } = call
            {
                executor
                    .take()
                    .context("executor instance is retired")?
                    .serve_artifact(length, budget, &digest, &socket)?;
                Ok(Vec::new())
            } else {
                executor
                    .as_mut()
                    .context("executor instance is retired")?
                    .dispatch(call)
            }
        });
        match result {
            Ok(response) => {
                if let Err(error) = queue.complete(0, &response) {
                    eprintln!("Hyperlight call result failed: {error}");
                    queue.complete(-1, &[])?;
                }
            }
            Err(error) => {
                eprintln!("Hyperlight executor call failed: {error}");
                queue.complete(-1, &[])?;
            }
        }
    }
}

#[cfg(test)]
#[path = "lib-test.rs"]
mod tests;
