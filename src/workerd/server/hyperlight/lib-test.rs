use super::*;

struct RejectActorStorage {
    opened: Rc<RefCell<Vec<(String, String)>>>,
}

impl workerd_server::config::ActorStorageProvider for RejectActorStorage {
    fn open_namespace(
        &self,
        class_name: &str,
        unique_key: &str,
    ) -> workerd_server::Result<KjOwn<workerd_server::bridge::ffi::ActorStorageBackend>> {
        self.opened
            .borrow_mut()
            .push((class_name.to_owned(), unique_key.to_owned()));
        Err(kj::failed!("actor namespace not admitted by the host"))
    }
}

#[test]
fn supplied_actor_storage_refusal_cannot_fall_back_to_memory_or_retry() {
    const CHILD: &str = "WORKERD_EXTERNAL_ACTOR_STORAGE_FAILURE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::supplied_actor_storage_refusal_cannot_fall_back_to_memory_or_retry",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("1 passed; 0 failed")
        );
        return;
    }
    let opened = Rc::new(RefCell::new(Vec::new()));
    let provider = Rc::new(RejectActorStorage {
        opened: Rc::clone(&opened),
    });
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<config::Builder>();
    let mut service = root.reborrow().init_services(1).get(0);
    service.set_name("fixture");
    let mut worker = service.init_worker();
    worker.set_compatibility_date("2026-10-19");
    let mut namespace = worker.reborrow().init_durable_object_namespaces(1).get(0);
    namespace.set_class_name("Counter");
    namespace.set_unique_key("admitted-counter-namespace");
    worker
        .reborrow()
        .init_durable_object_storage()
        .set_in_memory(());
    let mut module = worker.init_modules(1).get(0);
    module.set_name("worker.js");
    module.set_es_module(
        "export class Counter { fetch() { return new Response('actor'); } } \
         export default { fetch() { return new Response('worker'); } };",
    );
    let mut socket = root.init_sockets(1).get(0);
    socket.set_name("http");
    socket.reborrow().init_http();
    socket.init_service().set_name("fixture");
    let bytes = capnp::serialize::write_message_to_words(&message);
    let mut executor = Executor::new(HashMap::new())
        .unwrap()
        .with_actor_storage_provider(provider)
        .unwrap();
    let error = executor
        .dispatch(protocol::Call::Configure {
            config: bytes.clone(),
            socket: "http".into(),
            reply_limit: 4096,
        })
        .unwrap_err();
    assert!(error.to_string().contains("actor namespace not admitted"));
    assert_eq!(
        *opened.borrow(),
        vec![(
            "Counter".to_owned(),
            "admitted-counter-namespace".to_owned()
        )]
    );
    assert!(
        executor
            .dispatch(protocol::Call::Configure {
                config: bytes,
                socket: "http".into(),
                reply_limit: 4096,
            })
            .is_err()
    );
    assert_eq!(opened.borrow().len(), 1);
    executor.close().unwrap();
}

#[test]
fn unopened_external_namespace_delete_refuses_without_materializing_storage() {
    const CHILD: &str = "WORKERD_UNOPENED_STAGE_DELETE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::unopened_external_namespace_delete_refuses_without_materializing_storage",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("1 passed; 0 failed")
        );
        return;
    }
    let opened = Rc::new(RefCell::new(Vec::new()));
    let mut executor = Executor::new(HashMap::new())
        .unwrap()
        .with_actor_storage_provider(Rc::new(RejectActorStorage {
            opened: Rc::clone(&opened),
        }))
        .unwrap();
    executor
        .dispatch(protocol::Call::Configure {
            config: config(),
            socket: "http".into(),
            reply_limit: 4096,
        })
        .unwrap();
    let namespace = workerd_server::actor::ActorNamespace::new(
        Rc::clone(&executor.configured.as_ref().unwrap().factory),
        "UnopenedCounter".into(),
        workerd_server::bindings::ActorConfig::Durable {
            unique_key: "existing-external-data".into(),
            evictable: true,
            enable_sql: false,
            workflow: None,
            container: None,
        },
        Rc::new(workerd_server::config::InvalidConfigActorClass),
        false,
    );
    assert!(!namespace.is_linked());
    let error = namespace.delete_all(None).unwrap_err();
    assert!(
        error
            .description()
            .contains("namespace-wide deletion is not supported")
    );
    assert!(!namespace.is_linked());
    assert!(opened.borrow().is_empty());
    drop(namespace);
    executor.close().unwrap();
}

#[test]
fn registered_calls_cannot_override_standard_operations_or_run_unlisted_handlers() {
    for names in [
        vec![""],
        vec!["fetch"],
        vec!["configure"],
        vec!["configure_artifact"],
        vec!["serve_artifact"],
        vec!["queue", "queue"],
    ] {
        assert!(validate_registered_names(&names).is_err());
    }
    validate_registered_names(&["scheduled", "queue"]).unwrap();
    let mut executor = Executor::new(HashMap::new()).unwrap();
    assert!(executor.max_response_bytes().is_err());
    assert!(
        executor
            .run_scheduled(&std::time::UNIX_EPOCH, "* * * * *")
            .is_err()
    );
    assert!(
        executor
            .dispatch_registered_string(&[], 0, &[], |_, _, _| {
                panic!("no registered handler may run")
            })
            .unwrap()
            .is_none()
    );
    assert!(
        executor
            .dispatch_registered_string(&[], 0, &["queue"], |_, _, _| {
                panic!("invalid frame cannot run handler")
            })
            .is_err()
    );
    executor.close().unwrap();
}

fn config() -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    {
        let mut root = message.init_root::<config::Builder>();
        let mut service = root.reborrow().init_services(1).get(0);
        service.set_name("hello");
        let mut worker = service.init_worker();
        worker.set_compatibility_date("2026-10-19");
        let mut module = worker.init_modules(1).get(0);
        module.set_name("main.js");
        module.set_es_module(
            "let hits = 0; export default { async fetch(request) { \
                if (new URL(request.url).pathname === '/large') return new Response('x'.repeat(8192)); \
                if (new URL(request.url).pathname === '/echo') return new Response(await request.arrayBuffer(), \
                    { status: 201, headers: { 'x-executor-echo': 'binary' } }); \
                if (new URL(request.url).pathname === '/sink') return new Response(String((await request.arrayBuffer()).byteLength)); \
                if (new URL(request.url).pathname === '/loop') { while (true) {} } \
                if (new URL(request.url).pathname === '/counter') return new Response(String(++hits)); \
                return new Response('hello:' + new URL(request.url).pathname); \
            } };",
        );
        let mut socket = root.init_sockets(1).get(0);
        socket.set_name("http");
        socket.reborrow().init_http();
        socket.reborrow().init_service().set_name("hello");
    }
    capnp::serialize::write_message_to_words(&message)
}

#[test]
fn native_standard_worker_configuration_http_and_overflow_regressions() {
    let mut executor = Executor::new(HashMap::new()).unwrap();
    assert!(executor.dispatch(protocol::Call::Fetch(vec![])).is_err());
    let bytes = config();
    let mut trailing = bytes.clone();
    trailing.extend_from_slice(&[0; 8]);
    assert!(executor.configure(&trailing, "http", 4096).is_err());
    assert!(
        executor
            .configure(&bytes[..bytes.len() - 1], "http", 4096)
            .is_err()
    );
    assert!(executor.configure(&bytes, "missing", 4096).is_err());
    executor.configure(&bytes, "http", 4096).unwrap();
    assert!(executor.configure(&bytes, "http", 4096).is_err());
    let error = executor
        .dispatch(protocol::Call::ConfigureArtifact {
            length: 8,
            budget: 8,
            digest: [0; 32],
            socket: "http".to_owned(),
            reply_limit: 4096,
        })
        .unwrap_err();
    assert!(error.to_string().contains("only once"));
    let response = executor
        .fetch(b"GET /smoke HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec())
        .unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(b"hello:/smoke"));
    let mut echo =
        b"POST /echo HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4\r\n\r\n".to_vec();
    echo.extend_from_slice(&[0, 255, 0, 128]);
    let response = executor.fetch(echo).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 201 "));
    assert!(response.ends_with(&[0, 255, 0, 128]));
    let response = executor
        .fetch(
            b"POST /sink HTTP/1.1\r\nHost: example.test\r\nContent-Length: 4\r\n\r\ndata".to_vec(),
        )
        .unwrap();
    assert!(response.ends_with(b"4"));
    assert!(executor.fetch(b"GARBAGE\r\n\r\n".to_vec()).is_err());
    let response = executor.fetch(
        b"POST /echo HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\n\r\n4;note=yes\r\ndata\r\n0\r\nX-Trailer: yes\r\n\r\n".to_vec()
    ).unwrap();
    assert!(response.starts_with(b"HTTP/1.1 201 "));
    assert!(response.ends_with(b"data"));
    assert!(
        executor
            .fetch(b"GET /large HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec())
            .is_err()
    );
    assert!(
        executor
            .fetch(b"CONNECT example.test:443 HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec())
            .is_err()
    );
    assert!(executor.fetch(
        b"GET /a HTTP/1.1\r\nHost: example.test\r\n\r\nGET /b HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec()
    ).is_err());
    let counter = b"GET /counter HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec();
    assert!(executor.fetch(counter.clone()).unwrap().ends_with(b"1"));
    assert!(executor.fetch(
        b"GET /counter HTTP/1.1\r\nHost: example.test\r\nConnection: close\r\n\r\nGET /hidden HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec()
    ).is_err());
    assert!(executor.fetch(counter.clone()).unwrap().ends_with(b"2"));
    for (body, count) in [("0;x=\n\r\n\r\n", b"3"), ("0\r\n\n", b"4")] {
        let malformed = format!(
            "POST /counter HTTP/1.1\r\nHost: example.test\r\nTransfer-Encoding: chunked\r\nExpect: 100-continue\r\n\r\n{body}"
        );
        assert!(executor.fetch(malformed.into_bytes()).is_err());
        assert!(executor.fetch(counter.clone()).unwrap().ends_with(count));
    }

    let response = executor
        .fetch(b"GET /lf-head HTTP/1.1\nHost: example.test\n\n".to_vec())
        .unwrap();
    assert!(response.ends_with(b"hello:/lf-head"));
    let response = executor
        .fetch(b"GET /after-errors HTTP/1.1\r\nHost: example.test\r\n\r\n".to_vec())
        .unwrap();
    assert!(response.ends_with(b"hello:/after-errors"));
    if let Some(directory) = std::env::var_os("TEST_UNDECLARED_OUTPUTS_DIR") {
        std::fs::write(
            std::path::Path::new(&directory).join("hello.config.bin"),
            &bytes,
        )
        .unwrap();
    }
    executor.close().unwrap();
}

#[test]
fn finite_configuration_still_refuses_external_leaf_services() {
    let mut message = capnp::message::Builder::new_default();
    let root = message.init_root::<config::Builder>();
    let mut services = root.init_services(2);
    let mut app = services.reborrow().get(0);
    app.set_name("app");
    app.init_worker();
    let mut peer = services.get(1);
    peer.set_name("exact-peer");
    let mut external = peer.init_external();
    external.set_address("192.0.2.80:8080");
    external.init_http();
    let mut executor = Executor::new(HashMap::new()).unwrap();
    let error = executor
        .configure(
            &capnp::serialize::write_message_to_words(&message),
            "http",
            4096,
        )
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "finite guest entry requires worker services and admitted channels"
    );
    executor.close().unwrap();
}

#[test]
fn deferred_factory_sees_validated_config_once_and_cannot_retry_failure() {
    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let mut executor = Executor::with_service_factory(move |config| {
        observed.set(observed.get() + 1);
        let service = config.get_services()?.get(0);
        assert_eq!(service.get_name()?.to_str()?, "hello");
        let service::Which::Worker(worker) = service.which()? else {
            panic!("validated worker service expected");
        };
        assert_eq!(worker?.get_compatibility_date()?.to_str()?, "2026-10-19");
        bail!("deliberate admission refusal before engine bootstrap")
    })
    .unwrap();
    assert_eq!(calls.get(), 0);
    let bytes = config();
    let mut trailing = bytes.clone();
    trailing.extend_from_slice(&[0; 8]);
    assert!(executor.configure(&trailing, "http", 4096).is_err());
    assert!(executor.configure(&bytes, "missing", 4096).is_err());
    assert_eq!(calls.get(), 0);
    let error = executor.configure(&bytes, "http", 4096).unwrap_err();
    assert!(error.to_string().contains("deliberate admission refusal"));
    assert_eq!(calls.get(), 1);
    assert!(executor.configure(&bytes, "http", 4096).is_err());
    assert_eq!(calls.get(), 1);
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        let error = executor.prepare_ingress(&bytes, "http").err().unwrap();
        assert!(error.to_string().contains("unconfigured"));
        assert_eq!(calls.get(), 1);
    }
    assert!(
        executor
            .fetch(b"GET / HTTP/1.1\r\nHost: test\r\n\r\n".to_vec())
            .is_err()
    );
    executor.close().unwrap();
}

#[test]
fn semantic_bootstrap_failure_cannot_fall_back_to_static_configuration() {
    const CHILD: &str = "WORKERD_FACTORY_FAILURE_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::semantic_bootstrap_failure_cannot_fall_back_to_static_configuration",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(
            String::from_utf8(output.stdout)
                .unwrap()
                .contains("1 passed; 0 failed")
        );
        return;
    }
    let original = config();
    let reader = read_config(&original).unwrap();
    let mut message = capnp::message::Builder::new_default();
    message
        .set_root(reader.get_root::<config::Reader>().unwrap())
        .unwrap();
    message
        .get_root::<config::Builder>()
        .unwrap()
        .get_sockets()
        .unwrap()
        .get(0)
        .set_address("127.0.0.1:9876");
    let good = capnp::serialize::write_message_to_words(&message);
    let service::Which::Worker(worker) = message
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
    let mut module = worker.unwrap().init_modules(1).get(0);
    module.set_name("main.js");
    module.set_es_module(
        "throw new Error('semantic bootstrap refusal'); export default { fetch() {} };",
    );
    let bad = capnp::serialize::write_message_to_words(&message);
    let calls = Rc::new(Cell::new(0));
    let observed = Rc::clone(&calls);
    let mut executor = Executor::with_service_factory(move |_| {
        observed.set(observed.get() + 1);
        Ok(HashMap::new())
    })
    .unwrap();
    let error = executor.configure(&bad, "http", 4096).unwrap_err();
    assert!(
        error.to_string().contains("semantic bootstrap refusal"),
        "{error}"
    );
    assert_eq!(calls.get(), 1);
    let error = executor.configure(&good, "http", 4096).unwrap_err();
    assert!(error.to_string().contains("only once"), "{error}");
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        let error = executor.prepare_ingress(&good, "http").err().unwrap();
        assert!(error.to_string().contains("unconfigured"), "{error}");
    }
    assert_eq!(calls.get(), 1);
    executor.close().unwrap();
}
