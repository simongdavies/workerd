// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use super::*;

fn message(
    build: impl FnOnce(config::Builder<'_>),
) -> capnp::message::Reader<capnp::serialize::OwnedSegments> {
    let mut message = capnp::message::Builder::new_default();
    build(message.init_root());
    capnp::serialize::read_message(
        capnp::serialize::write_message_to_words(&message).as_slice(),
        capnp::message::ReaderOptions::new(),
    )
    .unwrap()
}

fn rpc_socket(mut root: config::Builder<'_>, name: &str) {
    let mut socket = root.reborrow().init_sockets(1).get(0);
    socket.reborrow().init_http().set_capnp_connect_host("rpc");
    socket.init_service().set_name(name);
}

#[test]
fn selects_exact_config_index_without_rewriting_entrypoint_or_props() {
    let message = message(|mut root| {
        let mut services = root.reborrow().init_services(2);
        services.reborrow().get(0).set_name("other");
        services.reborrow().get(0).init_worker();
        services.reborrow().get(1).set_name("app");
        services.get(1).init_worker();
        rpc_socket(root.reborrow(), "app");
        let mut target = root.get_sockets().unwrap().get(0).get_service().unwrap();
        target.set_entrypoint("Admin");
        target.init_props().set_json("{\"role\":\"reader\"}");
    });
    let root = message.get_root::<config::Reader>().unwrap();
    assert_eq!(selected_worker_from_socket(root).unwrap(), 1);
    let target = root.get_sockets().unwrap().get(0).get_service().unwrap();
    assert_eq!(target.get_name().unwrap().to_str().unwrap(), "app");
    assert_eq!(target.get_entrypoint().unwrap().to_str().unwrap(), "Admin");
    let workerd_capnp::service_designator::props::Which::Json(props) =
        target.get_props().which().unwrap()
    else {
        panic!("props changed");
    };
    assert_eq!(props.unwrap().to_str().unwrap(), "{\"role\":\"reader\"}");
}

#[test]
fn zero_or_multiple_sockets_are_rejected() {
    for count in [0, 2] {
        let message = message(|root| {
            root.init_sockets(count);
        });
        assert!(selected_worker_from_socket(message.get_root().unwrap()).is_err());
    }
}

#[test]
fn tcp_socket_is_not_an_rpc_bootstrap() {
    let message = message(|root| {
        root.init_sockets(1).get(0).init_tcp();
    });
    assert!(selected_worker_from_socket(message.get_root().unwrap()).is_err());
}

#[test]
fn rpc_connect_host_must_be_explicit_and_nonempty() {
    for explicit in [false, true] {
        let message = message(|root| {
            let mut http = root.init_sockets(1).get(0).init_http();
            if explicit {
                http.set_capnp_connect_host("");
            }
        });
        assert!(selected_worker_from_socket(message.get_root().unwrap()).is_err());
    }
}

#[test]
fn missing_or_non_worker_target_is_rejected() {
    for configured in [false, true] {
        let message = message(|mut root| {
            rpc_socket(root.reborrow(), "app");
            if configured {
                let mut service = root.init_services(1).get(0);
                service.set_name("app");
                service.init_network();
            }
        });
        assert!(selected_worker_from_socket(message.get_root().unwrap()).is_err());
    }
}

#[test]
fn external_leaf_cannot_replace_the_selected_worker() {
    let message = message(|mut root| {
        rpc_socket(root.reborrow(), "peer");
        let mut services = root.init_services(2);
        let mut app = services.reborrow().get(0);
        app.set_name("app");
        app.init_worker();
        let mut peer = services.get(1);
        peer.set_name("peer");
        let mut external = peer.init_external();
        external.set_address("192.0.2.80:8080");
        external.init_http();
    });
    let error = selected_worker_from_socket(message.get_root().unwrap()).unwrap_err();
    assert_eq!(
        error.to_string(),
        "selected ingress service is not a Worker"
    );
}

#[test]
fn duplicate_target_names_are_not_resolved_by_first_match() {
    let message = message(|mut root| {
        rpc_socket(root.reborrow(), "app");
        let mut services = root.init_services(2);
        for index in 0..services.len() {
            let mut service = services.reborrow().get(index);
            service.set_name("app");
            service.init_worker();
        }
    });
    assert!(selected_worker_from_socket(message.get_root().unwrap()).is_err());
}
