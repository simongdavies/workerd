// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use workerd_capnp::config;
use workerd_capnp::service;
use workerd_capnp::socket;

/// Selects the Worker behind the sole RPC ingress socket without rewriting its designator.
/// The caller must bind the admitted Config and executable identities to the owner.
pub fn selected_worker_from_socket(config: config::Reader<'_>) -> Result<u32> {
    let sockets = config.get_sockets()?;
    if sockets.len() != 1 {
        bail!("selected Worker ingress requires exactly one configured socket");
    }
    let socket = sockets.get(0);
    let socket::Which::Http(http) = socket.which()? else {
        bail!("selected Worker ingress requires an HTTP socket");
    };
    let http = http?;
    if !http.has_capnp_connect_host() || http.get_capnp_connect_host()?.to_str()?.is_empty() {
        bail!("selected Worker ingress requires an explicit capnpConnectHost");
    }
    let designator = socket.get_service()?;
    let name = designator.get_name()?.to_str()?;
    let mut selected = None;
    for (index, service) in config.get_services()?.iter().enumerate() {
        if service.get_name()?.to_str()? == name {
            if selected.is_some() {
                bail!("selected Worker service name is duplicated");
            }
            if !matches!(service.which(), Ok(service::Which::Worker(_))) {
                bail!("selected ingress service is not a Worker");
            }
            selected = Some(u32::try_from(index)?);
        }
    }
    selected.context("selected ingress Worker is not configured")
}

#[cfg(test)]
#[path = "selection-test.rs"]
mod tests;
