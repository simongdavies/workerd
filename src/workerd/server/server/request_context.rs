// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

//! Opaque, immutable embedder context. Nothing here interprets a product's authority tokens.

use std::any::Any;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;

use kj_rs::KjOwn;
use kj_rs::KjRc;

use crate::Result;
use crate::bridge::ffi;
use crate::channels::Channel;
use crate::channels::Frankenvalue;
use crate::channels::PendingToken;
use crate::channels::Persistent;
use crate::channels::RequestMetadata;
use crate::channels::TokenUsage;
use crate::channels::WorkerEntrypoint;
use crate::channels::WorkerInterface;

pub type Value = Arc<dyn Any + Send + Sync>;

pub struct RequestContextHandle {
    value: Value,
}

impl RequestContextHandle {
    #[expect(clippy::unnecessary_box_returns, reason = "cxx requires a Box")]
    pub(crate) fn context_handle_clone(&self) -> Box<Self> {
        Box::new(Self {
            value: Arc::clone(&self.value),
        })
    }
}

#[must_use]
pub fn new(value: Value) -> KjRc<ffi::RequestContext> {
    ffi::new_request_context(Box::new(RequestContextHandle { value }))
}

pub fn from_metadata(metadata: Pin<&mut RequestMetadata>) -> Result<Option<Value>> {
    let context: Option<KjRc<ffi::RequestContext>> = ffi::request_metadata_context(metadata).into();
    let Some(context) = context else {
        return Ok(None);
    };
    Ok(Some(ffi::request_context_value(&context)?.value))
}

/// Captured provenance, not proof of an independently authenticated caller on a transferred
/// capability. Absent outside its JavaScript scope; never falls back to actor tracing.
pub fn current() -> Result<Option<Value>> {
    let context: Option<KjRc<ffi::RequestContext>> = ffi::current_request_context()?.into();
    let Some(context) = context else {
        return Ok(None);
    };
    Ok(Some(ffi::request_context_value(&context)?.value))
}

/// Attaches a trusted context to every event on this connection's already-resolved channel.
#[must_use]
pub fn channel(inner: Rc<dyn Channel>, context: KjRc<ffi::RequestContext>) -> Rc<dyn Channel> {
    Rc::new(ContextChannel { inner, context })
}

struct ContextChannel {
    inner: Rc<dyn Channel>,
    context: KjRc<ffi::RequestContext>,
}

impl Channel for ContextChannel {
    fn start_request(
        &self,
        mut metadata: KjOwn<RequestMetadata>,
    ) -> Result<KjOwn<WorkerInterface>> {
        ffi::request_metadata_set_context(metadata.pin_mut(), self.context.clone());
        self.inner.start_request(metadata)
    }

    fn require_allows_transfer(&self) -> Result<()> {
        self.inner.require_allows_transfer()
    }

    fn token(&self, usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        self.inner.token(usage)
    }

    fn for_props(
        &self,
        props: KjOwn<Frankenvalue>,
        persistent: Persistent,
    ) -> Result<Rc<dyn Channel>> {
        Ok(channel(
            self.inner.for_props(props, persistent)?,
            self.context.clone(),
        ))
    }

    fn evict_for_test(&self, hibernate: bool) -> futures::future::LocalBoxFuture<'_, Result<()>> {
        self.inner.evict_for_test(hibernate)
    }

    fn worker_entrypoint(&self) -> Option<WorkerEntrypoint<'_>> {
        self.inner.worker_entrypoint()
    }
}
