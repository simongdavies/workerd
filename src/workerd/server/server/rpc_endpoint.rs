// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Explicitly unscoped standard RPC over one admitted owned stream per event.

use std::cell::Cell;
use std::cell::OnceCell;
use std::cell::RefCell;
use std::pin::Pin;
use std::rc::Rc;
use std::rc::Weak;
use std::time::SystemTime;

use futures::FutureExt;
use futures::future::Either;
use futures::future::LocalBoxFuture;
use kj::http::ConnectResponse;
use kj::http::ConnectSettings;
use kj::http::HeadersRef;
use kj::http::Method;
use kj::http::Service;
use kj::http::ServiceResponse;
use kj::io::AsyncInputStream;
use kj::io::AsyncIoStream;
use kj_rs::KjOwn;
use tokio::sync::Mutex;
use tokio::sync::Notify;
use tokio::sync::watch;
use worker::AlarmResult;
use worker::CustomEvent;
use worker::CustomEventResult;
use worker::Interface;
use worker::PromisedInterface;
use worker::ScheduledResult;

use crate::Result;
use crate::bindings::Designator;
use crate::bridge::ffi;
use crate::channels::Channel;
use crate::channels::PendingToken;
use crate::channels::RequestMetadata;
use crate::channels::TokenUsage;
use crate::channels::WorkerInterface;
use crate::channels::attach;
use crate::config::Factory;
use crate::config::WorkerEndpoint;

/// Returns an exclusively owned, already-admitted standard Capnp stream, not an HTTP socket
/// with an unconsumed CONNECT handshake. Cancellation of pending admission remains provider-owned.
pub trait OwnedWorkerConnector {
    fn connect<'a>(
        &'a self,
        target: &'a Designator,
        metadata: &'a RequestMetadata,
    ) -> LocalBoxFuture<'a, Result<KjOwn<AsyncIoStream>>>;
}

/// Ordinary unscoped RPC only. This must not be installed as a scoped native authority path.
pub struct UnscopedRpcWorkerEndpoint(Rc<Endpoint>);

struct Endpoint {
    connector: Rc<dyn OwnedWorkerConnector>,
    factory: OnceCell<Weak<Factory>>,
    closed: watch::Sender<bool>,
    requests: RefCell<Vec<Weak<Request>>>,
    retired: RefCell<Vec<Rc<Request>>>,
    changed: Notify,
}

struct Start {
    metadata: KjOwn<RequestMetadata>,
    factory: Rc<Factory>,
}

struct Request {
    target: Mutex<Option<PromisedInterface>>,
    start: RefCell<Option<Rc<Start>>>,
    producer_finished: Rc<Cell<bool>>,
}

struct ProducerFinished {
    finished: Rc<Cell<bool>>,
    endpoint: Weak<Endpoint>,
}

impl Drop for ProducerFinished {
    fn drop(&mut self) {
        self.finished.set(true);
        if let Some(endpoint) = self.endpoint.upgrade() {
            endpoint.changed.notify_waiters();
        }
    }
}

impl UnscopedRpcWorkerEndpoint {
    #[must_use]
    pub fn new(connector: Rc<dyn OwnedWorkerConnector>) -> Self {
        let (closed, _) = watch::channel(false);
        Self(Rc::new(Endpoint {
            connector,
            factory: OnceCell::new(),
            closed,
            requests: RefCell::new(Vec::new()),
            retired: RefCell::new(Vec::new()),
            changed: Notify::new(),
        }))
    }
}

fn retired() -> crate::Error {
    kj::disconnected!("unscoped RPC endpoint is retired")
}

impl WorkerEndpoint for UnscopedRpcWorkerEndpoint {
    fn bind_factory(&self, factory: &Rc<Factory>) -> Result<()> {
        let weak = Rc::downgrade(factory);
        if *self.0.closed.borrow() {
            return Err(retired());
        }
        if let Some(bound) = self.0.factory.get() {
            if !Weak::ptr_eq(bound, &weak) {
                return Err(kj::failed!("RPC endpoint belongs to another factory"));
            }
        } else {
            self.0
                .factory
                .set(weak)
                .map_err(|_| kj::failed!("RPC endpoint factory was already bound"))?;
        }
        Ok(())
    }

    fn resolve(&self, designator: &Designator) -> Result<Rc<dyn Channel>> {
        self.0.factory()?;
        if *self.0.closed.borrow() {
            return Err(retired());
        }
        Ok(Rc::new(RpcChannel {
            endpoint: Rc::clone(&self.0),
            target: designator.clone(),
        }))
    }

    fn unlink(&self) -> LocalBoxFuture<'static, Result<()>> {
        self.0.closed.send_replace(true);
        let endpoint = Rc::clone(&self.0);
        async move {
            loop {
                let changed = endpoint.changed.notified();
                let requests = endpoint
                    .requests
                    .borrow()
                    .iter()
                    .filter_map(Weak::upgrade)
                    .collect::<Vec<_>>();
                let mut busy = false;
                for request in &requests {
                    if let Ok(mut target) = request.target.try_lock() {
                        // This future is owned outside the cancellable task set and awaited
                        // outside V8 before the factory can retire.
                        drop(target.take());
                        if request.producer_finished.get() {
                            drop(request.start.borrow_mut().take());
                        } else {
                            busy = true;
                        }
                    } else {
                        busy = true;
                    }
                }
                if !busy {
                    endpoint.retired.borrow_mut().clear();
                    endpoint.requests.borrow_mut().clear();
                    return Ok(());
                }
                changed.await;
            }
        }
        .boxed_local()
    }
}

impl Endpoint {
    fn factory(&self) -> Result<Rc<Factory>> {
        self.factory
            .get()
            .and_then(Weak::upgrade)
            .ok_or_else(|| kj::disconnected!("RPC endpoint factory is unavailable"))
    }

    fn defer_request(self: &Rc<Self>, request: Rc<Request>) {
        if request.start.borrow().is_none()
            && request
                .target
                .try_lock()
                .is_ok_and(|target| target.is_none())
        {
            return;
        }
        self.retired.borrow_mut().push(request);
        self.changed.notify_waiters();
        match self.factory() {
            Ok(factory) => {
                let endpoint = Rc::downgrade(self);
                factory.spawn_detached(async move {
                    if let Some(endpoint) = endpoint.upgrade() {
                        loop {
                            let changed = endpoint.changed.notified();
                            let retired = std::mem::take(&mut *endpoint.retired.borrow_mut());
                            for request in retired {
                                let idle = if let Ok(mut target) = request.target.try_lock() {
                                    drop(target.take());
                                    true
                                } else {
                                    false
                                };
                                if idle && request.producer_finished.get() {
                                    drop(request.start.borrow_mut().take());
                                } else {
                                    endpoint.retired.borrow_mut().push(request);
                                }
                            }
                            if endpoint.retired.borrow().is_empty() {
                                break;
                            }
                            changed.await;
                        }
                        endpoint
                            .requests
                            .borrow_mut()
                            .retain(|request| request.strong_count() != 0);
                        endpoint.changed.notify_waiters();
                    }
                });
            }
            Err(error) => {
                eprintln!("RPC deferred close retained without a live factory: {error:?}");
            }
        }
    }
}

struct RpcChannel {
    endpoint: Rc<Endpoint>,
    target: Designator,
}

impl Channel for RpcChannel {
    fn start_request(&self, metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        if *self.endpoint.closed.borrow() {
            return Err(retired());
        }
        let factory = self.endpoint.factory()?;
        let start = Rc::new(Start {
            factory: Rc::clone(&factory),
            metadata,
        });
        let owned_start = Rc::clone(&start);
        let endpoint = Rc::clone(&self.endpoint);
        let target = self.target.clone();
        let producer_finished = Rc::new(Cell::new(false));
        let finished = ProducerFinished {
            finished: Rc::clone(&producer_finished),
            endpoint: Rc::downgrade(&self.endpoint),
        };
        let startup = async move {
            let producer_factory = Rc::clone(&owned_start.factory);
            producer_factory
                .spawn(async move {
                    let _finished = finished;
                    let stream = endpoint
                        .connector
                        .connect(&target, &owned_start.metadata)
                        .await?;
                    if *endpoint.closed.borrow() {
                        // This task was deferred to a KJ turn, never called during resolve/JS startup.
                        drop(stream);
                        return Err(retired());
                    }
                    let client = ffi::new_rpc_client(owned_start.factory.raw(), stream);
                    let worker = ffi::rpc_client_into_worker(client, &owned_start.metadata)?;
                    Ok(attach(worker, Rc::clone(&owned_start.factory)))
                })
                .await
        };
        let request = Rc::new(Request {
            target: Mutex::new(Some(PromisedInterface::new(startup))),
            start: RefCell::new(Some(start)),
            producer_finished,
        });
        self.endpoint
            .requests
            .borrow_mut()
            .retain(|request| request.strong_count() != 0);
        self.endpoint
            .requests
            .borrow_mut()
            .push(Rc::downgrade(&request));
        Ok(RpcRequest {
            endpoint: Rc::clone(&self.endpoint),
            request,
        }
        .into_kj())
    }

    fn require_allows_transfer(&self) -> Result<()> {
        Err(crate::services::not_transferable("Unscoped RPC endpoint"))
    }

    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(crate::services::not_transferable("Unscoped RPC endpoint"))
    }
}

struct RpcRequest {
    endpoint: Rc<Endpoint>,
    request: Rc<Request>,
}

impl Drop for RpcRequest {
    fn drop(&mut self) {
        self.endpoint.defer_request(Rc::clone(&self.request));
    }
}

struct NotifyAfterBorrow(Rc<Endpoint>);

impl Drop for NotifyAfterBorrow {
    fn drop(&mut self) {
        self.0.changed.notify_waiters();
    }
}

async fn wait_closed(mut closed: watch::Receiver<bool>) -> crate::Error {
    while !*closed.borrow() {
        if closed.changed().await.is_err() {
            return kj::disconnected!("RPC endpoint retirement signal was lost");
        }
    }
    retired()
}

macro_rules! forward {
    ($self:ident, $method:ident($($arg:expr),* $(,)?)) => {{
        if *$self.endpoint.closed.borrow() {
            return Err(retired());
        }
        let _notify = NotifyAfterBorrow(Rc::clone(&$self.endpoint));
        let mut slot = $self.request.target.lock().await;
        let target = slot.as_mut().ok_or_else(retired)?;
        let cancelled = wait_closed($self.endpoint.closed.subscribe()).boxed_local();
        let event = target.$method($($arg),*).boxed_local();
        let result = match futures::future::select(cancelled, event).await {
            Either::Left((error, _event)) => Err(error),
            Either::Right((result, _cancelled)) => result,
        };
        drop(slot);
        result
    }};
}

#[async_trait::async_trait(?Send)]
impl Service for RpcRequest {
    async fn request<'a>(
        &'a mut self,
        method: Method,
        url: &'a [u8],
        headers: HeadersRef<'a>,
        body: Pin<&'a mut AsyncInputStream>,
        response: ServiceResponse<'a>,
    ) -> Result<()> {
        forward!(self, request(method, url, headers, body, response))
    }

    async fn connect<'a>(
        &'a mut self,
        host: &'a [u8],
        headers: HeadersRef<'a>,
        connection: Pin<&'a mut AsyncIoStream>,
        response: ConnectResponse<'a>,
        settings: ConnectSettings<'a>,
    ) -> Result<()> {
        let use_tls = settings.use_tls;
        let mut tls_starter = settings.tls_starter.into();
        forward!(
            self,
            connect(
                host,
                headers,
                connection,
                response,
                crate::services::reborrow_settings(use_tls, &mut tls_starter),
            )
        )
    }
}

#[async_trait::async_trait(?Send)]
impl Interface for RpcRequest {
    async fn prewarm(&mut self, url: &str) -> Result<()> {
        forward!(self, prewarm(url))
    }

    async fn run_scheduled(&mut self, time: &SystemTime, cron: &str) -> Result<ScheduledResult> {
        forward!(self, run_scheduled(time, cron))
    }

    async fn run_alarm(&mut self, time: &SystemTime, retry: u32) -> Result<AlarmResult> {
        forward!(self, run_alarm(time, retry))
    }

    async fn abandon_alarm(&mut self, time: &SystemTime) -> Result<Option<SystemTime>> {
        forward!(self, abandon_alarm(time))
    }

    async fn test(&mut self) -> Result<bool> {
        if *self.endpoint.closed.borrow() {
            return Err(retired());
        }
        Err(kj::not_implemented!(
            "standard Workerd RPC has no remote test operation"
        ))
    }

    async fn custom_event(&mut self, event: KjOwn<CustomEvent>) -> Result<CustomEventResult> {
        if *self.endpoint.closed.borrow() {
            return PromisedInterface::new(async { Err(retired()) })
                .custom_event(event)
                .await;
        }
        forward!(self, custom_event(event))
    }
}

#[cfg(test)]
#[path = "rpc_endpoint-test.rs"]
mod tests;
