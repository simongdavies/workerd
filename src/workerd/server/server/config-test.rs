// Copyright (c) 2026 Cloudflare, Inc.
// Licensed under the Apache 2.0 license found in the LICENSE file or at:
//     https://opensource.org/licenses/Apache-2.0

use std::cell::RefCell;

use super::*;

/// A reporter that collects into a shared list.
fn collecting_reporter() -> (Reporter, Rc<RefCell<Vec<String>>>) {
    let errors = Rc::new(RefCell::new(Vec::new()));
    let sink = Rc::clone(&errors);
    let report = Reporter::new(
        Box::new(move |error| sink.borrow_mut().push(error)),
        Box::new(|_warning| {}),
        true,
    );
    (report, errors)
}

/// The message a config `build` fills in, read back.
fn config_message(
    build: impl FnOnce(config::Builder<'_>),
) -> capnp::message::Reader<capnp::serialize::OwnedSegments> {
    let mut message = capnp::message::Builder::new_default();
    build(message.init_root::<config::Builder>());
    let words = capnp::serialize::write_message_to_words(&message);
    capnp::serialize::read_message(words.as_slice(), ReaderOptions::new()).unwrap()
}

fn actor_configs_of(
    build: impl FnOnce(config::Builder<'_>),
    experimental: bool,
) -> (Result<ActorConfigs>, Vec<String>) {
    let message = config_message(build);
    let config = message.get_root::<config::Reader>().unwrap();
    let (report, errors) = collecting_reporter();
    let result = collect_actor_configs(config, experimental, &report).map_err(capnp_error);
    let errors = errors.borrow().clone();
    (result, errors)
}

#[test]
fn actor_configs_read_durable_and_ephemeral_namespaces() {
    let (result, errors) = actor_configs_of(
        |config| {
            let mut service = config.init_services(1).get(0);
            service.set_name("main");
            let mut worker = service.init_worker();
            worker
                .reborrow()
                .get_durable_object_storage()
                .set_in_memory(());
            let mut namespaces = worker.init_durable_object_namespaces(2);
            let mut durable = namespaces.reborrow().get(0);
            durable.set_class_name("Counter");
            durable.set_unique_key("counter-key");
            durable.set_enable_sql(true);
            let mut ephemeral = namespaces.get(1);
            ephemeral.set_class_name("Cache");
            ephemeral.set_ephemeral_local(());
            ephemeral.set_prevent_eviction(true);
        },
        true,
    );
    assert!(errors.is_empty(), "{errors:?}");
    let configs = result.unwrap();
    let main = &configs["main"];
    assert_eq!(
        main.get("Counter"),
        Some(&ActorConfig::Durable {
            unique_key: "counter-key".to_owned(),
            evictable: true,
            enable_sql: true,
            workflow: None,
            container: None,
        })
    );
    assert_eq!(
        main.get("Cache"),
        Some(&ActorConfig::Ephemeral {
            evictable: false,
            enable_sql: false,
        })
    );
    // Config order is kept.
    let names: Vec<&String> = main.keys().collect();
    assert_eq!(names, ["Counter", "Cache"]);
}

#[test]
fn ephemeral_namespaces_need_experimental() {
    let (result, errors) = actor_configs_of(
        |config| {
            let mut service = config.init_services(1).get(0);
            service.set_name("main");
            let worker = service.init_worker();
            let mut ns = worker.init_durable_object_namespaces(1).get(0);
            ns.set_class_name("Cache");
            ns.set_ephemeral_local(());
        },
        false,
    );
    result.unwrap();
    assert_eq!(
        errors,
        [
            "Ephemeral objects (Durable Object namespaces with type 'ephemeralLocal') are an \
                 experimental feature which may change or go away in the future. You must run \
                 workerd with `--experimental` to use this feature."
        ]
    );
}

#[test]
fn durable_classes_need_storage() {
    let (result, errors) = actor_configs_of(
        |config| {
            let mut service = config.init_services(1).get(0);
            service.set_name("main");
            let worker = service.init_worker();
            let mut ns = worker.init_durable_object_namespaces(1).get(0);
            ns.set_class_name("Counter");
            ns.set_unique_key("k");
        },
        false,
    );
    result.unwrap();
    assert_eq!(
        errors,
        [
            "Worker service \"main\" implements durable object classes but has \
                 `durableObjectStorage` set to `none`."
        ]
    );
}

#[test]
fn duplicate_service_names_are_reported() {
    let (result, errors) = actor_configs_of(
        |config| {
            let mut services = config.init_services(2);
            services.reborrow().get(0).set_name("dup");
            services.reborrow().get(0).set_unspecified(());
            services.reborrow().get(1).set_name("dup");
            services.get(1).init_network();
        },
        false,
    );
    let configs = result.unwrap();
    assert_eq!(configs.len(), 1);
    assert_eq!(errors, ["Config defines multiple services named \"dup\"."]);
}

#[test]
fn unique_key_modifier_is_unimplemented() {
    let (result, _) = actor_configs_of(
        |config| {
            let mut service = config.init_services(1).get(0);
            service.set_name("main");
            service
                .init_worker()
                .set_durable_object_unique_key_modifier("mod");
        },
        false,
    );
    let error = result.err().unwrap();
    assert_eq!(
        error.description(),
        "durableObjectUniqueKeyModifier is not implemented yet"
    );
}

struct NullChannel;

struct RecordingEndpoint {
    requests: Rc<RefCell<Vec<Designator>>>,
    channel: Rc<dyn Channel>,
}

impl WorkerEndpoint for RecordingEndpoint {
    fn resolve(&self, designator: &Designator) -> Result<Rc<dyn Channel>> {
        self.requests.borrow_mut().push(designator.clone());
        Ok(Rc::clone(&self.channel))
    }
}

fn endpoint() -> Rc<dyn WorkerEndpoint> {
    Rc::new(RecordingEndpoint {
        requests: Rc::new(RefCell::new(Vec::new())),
        channel: Rc::new(NullChannel),
    })
}

#[test]
fn worker_partition_requires_exact_other_worker_coverage() {
    let message = config_message(|root| {
        let mut services = root.init_services(3);
        for (index, name) in ["local", "remote", "network"].iter().enumerate() {
            let mut service = services.reborrow().get(index as u32);
            service.set_name(*name);
            if index == 2 {
                service.init_network();
            } else {
                service.init_worker();
            }
        }
    });
    let config = message.get_root::<config::Reader>().unwrap();
    assert!(
        WorkerPartition::new(
            config,
            0,
            HashMap::from([("remote".to_owned(), endpoint())]),
        )
        .is_ok()
    );
    for endpoints in [
        HashMap::new(),
        HashMap::from([("local".to_owned(), endpoint())]),
        HashMap::from([
            ("remote".to_owned(), endpoint()),
            ("network".to_owned(), endpoint()),
        ]),
    ] {
        assert!(WorkerPartition::new(config, 0, endpoints).is_err());
    }
    assert!(WorkerPartition::new(config, 2, HashMap::new()).is_err());
    assert!(WorkerPartition::new(config, 3, HashMap::new()).is_err());
}

#[test]
fn worker_partition_refuses_duplicate_or_empty_service_names() {
    for names in [["local", "local"], ["local", ""]] {
        let message = config_message(|root| {
            let mut services = root.init_services(2);
            for (index, name) in names.iter().enumerate() {
                let mut service = services.reborrow().get(index as u32);
                service.set_name(*name);
                service.init_worker();
            }
        });
        assert!(WorkerPartition::new(message.get_root().unwrap(), 0, HashMap::new(),).is_err());
    }
}

#[test]
fn hosted_worker_receives_original_entrypoint_props_and_channel() {
    let channel: Rc<dyn Channel> = Rc::new(NullChannel);
    let requests = Rc::new(RefCell::new(Vec::new()));
    let endpoint: Rc<dyn WorkerEndpoint> = Rc::new(RecordingEndpoint {
        requests: Rc::clone(&requests),
        channel: Rc::clone(&channel),
    });
    let services = LinkedHashMap::from_iter([(
        "remote".to_owned(),
        Service::Endpoint {
            default: Rc::clone(&channel),
            endpoint,
        },
    )]);
    let mut target = designator("remote", Some("Admin"));
    target.props_json = Some("{\"role\":\"reader\"}".to_owned());
    let (report, errors) = collecting_reporter();
    let resolved = lookup_service(&services, &report, &target);
    assert!(Rc::ptr_eq(&resolved, &channel));
    assert_eq!(&*requests.borrow(), &[target]);
    assert!(errors.borrow().is_empty());
}

#[test]
fn hosted_worker_resolution_failure_is_reported_without_local_fallback() {
    struct Refused;
    impl WorkerEndpoint for Refused {
        fn resolve(&self, _: &Designator) -> Result<Rc<dyn Channel>> {
            Err(kj::failed!("hosted endpoint refused"))
        }
    }
    let services = LinkedHashMap::from_iter([(
        "remote".to_owned(),
        Service::Endpoint {
            default: Rc::new(NullChannel),
            endpoint: Rc::new(Refused),
        },
    )]);
    let (report, errors) = collecting_reporter();
    let _channel = lookup_service(&services, &report, &designator("remote", Some("Admin")));
    assert_eq!(errors.borrow().len(), 1);
    assert!(errors.borrow()[0].contains("hosted endpoint refused"));
    assert!(services["remote"].as_worker().is_none());
}

#[test]
fn hosted_workflow_owner_skips_local_initialization_but_keeps_config_validation() {
    for missing_binding in [false, true] {
        let message = config_message(|root| {
            let mut service = root.init_services(1).get(0);
            service.set_name("hosted");
            let worker = service.init_worker();
            let mut engine = worker.init_workflows_engine();
            engine
                .reborrow()
                .init_actor_class()
                .set_name("remote-engine");
            let mut workflow = engine.init_workflows(1).get(0);
            workflow.set_name("workflow");
            workflow.set_class_name("Flow");
            if !missing_binding {
                workflow.init_binding_service().set_name("remote-storage");
            }
        });
        let config = message.get_root::<config::Reader>().unwrap();
        let (report, errors) = collecting_reporter();
        let actors = collect_actor_configs(config, true, &report).unwrap();
        let services = LinkedHashMap::from_iter([(
            "hosted".to_owned(),
            Service::Endpoint {
                default: Rc::new(NullChannel),
                endpoint: endpoint(),
            },
        )]);
        let mut namespaces = HashMap::new();
        init_workflows(config, &services, &actors, &report, &mut namespaces).unwrap();
        assert!(namespaces.is_empty());
        if missing_binding {
            assert_eq!(errors.borrow().len(), 1);
            assert!(errors.borrow()[0].contains("missing bindingService"));
        } else {
            assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
        }
    }
}

#[test]
fn absent_hosted_actor_class_is_reported_without_local_instantiation() {
    let services = LinkedHashMap::from_iter([(
        "remote".to_owned(),
        Service::Endpoint {
            default: Rc::new(NullChannel),
            endpoint: endpoint(),
        },
    )]);
    let (report, errors) = collecting_reporter();
    let _class = lookup_actor_class(&services, &report, &designator("remote", Some("Counter")));
    assert_eq!(errors.borrow().len(), 1);
    assert!(errors.borrow()[0].contains("hosted Worker actor-class route was not supplied"));
    assert!(services["remote"].as_worker().is_none());
}

#[test]
fn hosted_actor_class_receives_the_complete_original_designator() {
    struct Class;
    impl ActorClass for Class {
        fn new_actor(&self, _: NewActor<'_>) -> Result<KjOwn<ffi::ActorHandle>> {
            Err(kj::failed!("fixture must not instantiate a local actor"))
        }

        fn start_request(
            &self,
            _: KjOwn<RequestMetadata>,
            _: &ffi::ActorHandle,
        ) -> Result<KjOwn<WorkerInterface>> {
            Err(kj::failed!("fixture does not execute actors"))
        }

        fn token(&self, _: TokenUsage) -> Result<KjOwn<PendingToken>> {
            Err(kj::failed!("fixture does not issue tokens"))
        }
    }
    struct Endpoint {
        requests: Rc<RefCell<Vec<Designator>>>,
        class: Rc<dyn ActorClass>,
    }
    impl WorkerEndpoint for Endpoint {
        fn resolve(&self, _: &Designator) -> Result<Rc<dyn Channel>> {
            Err(kj::failed!("fixture is an actor class endpoint"))
        }

        fn actor_class(&self, target: &Designator) -> Result<Rc<dyn ActorClass>> {
            self.requests.borrow_mut().push(target.clone());
            Ok(Rc::clone(&self.class))
        }
    }
    let class: Rc<dyn ActorClass> = Rc::new(Class);
    let requests = Rc::new(RefCell::new(Vec::new()));
    let services = LinkedHashMap::from_iter([(
        "remote".to_owned(),
        Service::Endpoint {
            default: Rc::new(NullChannel),
            endpoint: Rc::new(Endpoint {
                requests: Rc::clone(&requests),
                class: Rc::clone(&class),
            }),
        },
    )]);
    let mut target = designator("remote", Some("Counter"));
    target.props_json = Some("{\"region\":\"west\"}".to_owned());
    let (report, errors) = collecting_reporter();
    let resolved = lookup_actor_class(&services, &report, &target);
    assert!(Rc::ptr_eq(&resolved, &class));
    assert_eq!(&*requests.borrow(), &[target]);
    assert!(errors.borrow().is_empty());
}

#[test]
fn hosted_namespace_retains_its_kind_actor_name_and_persistence_flag() {
    struct Namespace {
        requests: Rc<RefCell<Vec<(String, bool)>>>,
        channel: Rc<dyn Channel>,
    }
    impl ActorNamespaceChannel for Namespace {
        fn unique_key(&self) -> Option<&str> {
            None
        }

        fn channel(&self, _: KjOwn<ActorIdHandle>, _: bool) -> Result<Rc<dyn Channel>> {
            Err(kj::failed!("fixture expects an ephemeral actor name"))
        }

        fn channel_by_name(&self, name: &str, persistent: bool) -> Result<Rc<dyn Channel>> {
            self.requests
                .borrow_mut()
                .push((name.to_owned(), persistent));
            Ok(Rc::clone(&self.channel))
        }

        fn evict_all_for_test(&self, _: bool) -> futures::future::LocalBoxFuture<'_, Result<()>> {
            Box::pin(async { Err(kj::failed!("fixture does not evict local actors")) })
        }
    }
    let requests = Rc::new(RefCell::new(Vec::new()));
    let channel: Rc<dyn Channel> = Rc::new(NullChannel);
    let namespace: Rc<dyn ActorNamespaceChannel> = Rc::new(Namespace {
        requests: Rc::clone(&requests),
        channel: Rc::clone(&channel),
    });
    assert_eq!(namespace.unique_key(), None);
    let resolved = namespace
        .channel_by_name("actual:actor/name", false)
        .unwrap();
    assert!(Rc::ptr_eq(&resolved, &channel));
    assert_eq!(
        &*requests.borrow(),
        &[("actual:actor/name".to_owned(), false)]
    );
}

#[test]
fn selected_worker_routes_without_constructing_the_other_configured_worker() {
    selected_worker_fixture(
        "selected_worker_routes_without_constructing_the_other_configured_worker",
        SelectedWorkerCase::Routing,
    );
}

#[test]
fn hosted_dynamic_unlink_can_synchronously_abort_all_named_workers() {
    selected_worker_fixture(
        "hosted_dynamic_unlink_can_synchronously_abort_all_named_workers",
        SelectedWorkerCase::Unlink,
    );
}

#[test]
fn cache_outbound_keeps_interleaved_request_frames_and_unscoped_absence() {
    selected_worker_fixture(
        "cache_outbound_keeps_interleaved_request_frames_and_unscoped_absence",
        SelectedWorkerCase::Cache,
    );
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum SelectedWorkerCase {
    Routing,
    Unlink,
    Cache,
}

struct AbortOnUnlink {
    on_abort: Box<dyn Fn()>,
    unlinks: Rc<std::cell::Cell<usize>>,
}

impl crate::loader::HostedWorker for AbortOnUnlink {
    fn entrypoint(
        &self,
        _name: Option<&str>,
        _props: KjOwn<Frankenvalue>,
    ) -> Result<Rc<dyn Channel>> {
        Err(kj::failed!("jsg.Error: hosted fixture endpoint refused"))
    }

    fn actor_class(
        &self,
        _name: Option<&str>,
        _props: KjOwn<Frankenvalue>,
    ) -> Result<Rc<dyn ActorClass>> {
        Err(kj::failed!("jsg.Error: hosted fixture class refused"))
    }

    fn unlink(&self) {
        self.unlinks.set(self.unlinks.get() + 1);
        (self.on_abort)();
    }
}

struct AbortOnLoad {
    loads: Rc<std::cell::Cell<usize>>,
    unlinks: Rc<std::cell::Cell<usize>>,
}

impl crate::loader::DynamicWorkerLoader for AbortOnLoad {
    fn load(
        &self,
        name: String,
        source: KjOwn<crate::channels::DynamicSource>,
        on_abort: Box<dyn Fn()>,
    ) -> futures::future::LocalBoxFuture<'static, Result<Rc<dyn crate::loader::HostedWorker>>> {
        self.loads.set(self.loads.get() + 1);
        if name.ends_with(":unlink-first") || name.ends_with(":unlink-second") {
            drop(source);
            let hosted: Rc<dyn crate::loader::HostedWorker> = Rc::new(AbortOnUnlink {
                on_abort,
                unlinks: Rc::clone(&self.unlinks),
            });
            return Box::pin(async move { Ok(hosted) });
        }
        on_abort();
        drop(source);
        Box::pin(async { Err(kj::failed!("jsg.Error: synchronous fixture abort")) })
    }
}

fn selected_worker_config() -> capnp::message::Reader<capnp::serialize::OwnedSegments> {
    config_message(|root| {
        let mut services = root.init_services(2);
        let mut local = services.reborrow().get(0);
        local.set_name("local");
        let mut worker = local.init_worker();
        worker.set_compatibility_date("2026-10-19");
        worker
            .reborrow()
            .init_compatibility_flags(1)
            .set(0, "experimental");
        worker.reborrow().init_global_outbound().set_name("remote");
        worker
            .reborrow()
            .init_cache_api_outbound()
            .set_name("remote");
        let mut module = worker.reborrow().init_modules(1).get(0);
        module.set_name("main.js");
        module.set_es_module(
            r"export default {
              async fetch(request, env) {
                if (new URL(request.url).pathname === '/cache') {
                  await caches.default.delete(request.url);
                  const named = await caches.open('named');
                  await named.delete(request.url);
                  return new Response('cache done', {headers: {'content-length': '10'}});
                }
                if (new URL(request.url).pathname === '/unlink') {
                  for (const name of ['unlink-first', 'unlink-second']) {
                    const stub = env.LOADER.get(name, () => {
                      throw new Error('hosted source must not compile locally');
                    });
                    try {
                      await stub.getEntrypoint().fetch('https://child.test');
                      throw new Error('hosted fixture unexpectedly served');
                    } catch (error) {
                      if (!error.message.includes('hosted fixture endpoint refused')) throw error;
                    }
                  }
                  return new Response('unlinks prepared', {headers: {'content-length': '16'}});
                }
                if (new URL(request.url).pathname === '/loader') {
                  for (let i = 0; i < 2; ++i) {
                    const stub = env.LOADER.get('same-name', () => {
                      throw new Error('aborted source must not compile locally');
                    });
                    try {
                      await stub.getEntrypoint().fetch('https://child.test');
                      throw new Error('aborted hosted Worker unexpectedly started');
                    } catch (error) {
                      if (!error.message.includes('synchronous fixture abort')) throw error;
                    }
                  }
                  return new Response('loader aborted twice', {headers: {'content-length': '20'}});
                }
                return env.REMOTE.fetch(request);
              }
            };",
        );
        let mut bindings = worker.init_bindings(2);
        let mut binding = bindings.reborrow().get(0);
        binding.set_name("REMOTE");
        let mut target = binding.init_service();
        target.set_name("remote");
        target.set_entrypoint("Admin");
        target.init_props().set_json("{\"role\":\"reader\"}");
        let mut binding = bindings.get(1);
        binding.set_name("LOADER");
        binding.init_worker_loader();
        let mut remote = services.get(1);
        remote.set_name("remote");
        let mut worker = remote.init_worker();
        worker.set_compatibility_date("2026-10-19");
        let mut module = worker.init_modules(1).get(0);
        module.set_name("remote.js");
        module.set_es_module("throw new Error('remote Worker must not initialize locally');");
    })
}

fn selected_worker_fixture(test_name: &str, case: SelectedWorkerCase) {
    // V8 cannot be disposed and initialized again in the test process.
    const CHILD: &str = "WORKERD_SELECTED_WORKER_TEST_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .arg("--exact")
            .arg(format!("config::tests::{test_name}"))
            .arg("--nocapture")
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        print!("{stdout}");
        eprint!("{}", String::from_utf8_lossy(&output.stderr));
        assert!(output.status.success());
        assert!(stdout.contains("test result: ok. 1 passed;"));
        return;
    }

    let message = selected_worker_config();
    let bytes = capnp::serialize::write_message_segments_to_words(&message.into_segments());
    let words = bytes
        .chunks_exact(8)
        .map(|word| u64::from_ne_bytes(word.try_into().unwrap()))
        .collect();
    let options = ffi::ServeOrTestOptions {
        perfetto_trace_path: KjMaybe::None,
        perfetto_trace_categories: KjMaybe::None,
        async_trace_path: KjMaybe::None,
        async_trace_stacks: KjMaybe::None,
        async_trace_promises: false,
        experimental: true,
        pyodide_package_disk_cache_dir: KjMaybe::None,
        pyodide_bundle_disk_cache_dir: KjMaybe::None,
        python_save_snapshot: false,
        python_save_baseline_snapshot: false,
        python_load_snapshot: KjMaybe::None,
        python_snapshot_dir: KjMaybe::None,
    };
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    let factory = ffi::bootstrap(
        runtime.context(),
        words,
        &options,
        KjMaybe::None,
        KjMaybe::None,
    )
    .unwrap();
    runtime
        .block_on(selected_worker_scenario(factory, case))
        .unwrap();
}

async fn selected_worker_scenario(raw: KjOwn<ffi::WorkerFactory>, case: SelectedWorkerCase) {
    let factory = Rc::new(Factory::new(raw));
    let message = factory.config().unwrap();
    let live = Rc::new(std::cell::Cell::new(0));
    let requests = Rc::new(RefCell::new(Vec::new()));
    let contexts = Rc::new(RefCell::new(Vec::new()));
    let channel: Rc<dyn Channel> = if case == SelectedWorkerCase::Cache {
        Rc::new(CacheContextChannel {
            live: Rc::clone(&live),
            contexts: Rc::clone(&contexts),
            barrier: Rc::new(tokio::sync::Barrier::new(2)),
        })
    } else {
        Rc::new(SuppliedEchoChannel(Rc::clone(&live)))
    };
    let remote: Rc<dyn WorkerEndpoint> = Rc::new(RecordingEndpoint {
        requests: Rc::clone(&requests),
        channel,
    });
    let loads = Rc::new(std::cell::Cell::new(0));
    let unlinks = Rc::new(std::cell::Cell::new(0));
    let partition = WorkerPartition::new(
        message.get_root().unwrap(),
        0,
        HashMap::from([("remote".to_owned(), remote)]),
    )
    .unwrap()
    .with_dynamic_loader(Rc::new(AbortOnLoad {
        loads: Rc::clone(&loads),
        unlinks: Rc::clone(&unlinks),
    }));
    let (report, errors) = collecting_reporter();
    let server = Server::start_with_worker_partition(
        Rc::clone(&factory),
        &Overrides::default(),
        report,
        HashMap::new(),
        HashMap::new(),
        partition,
    )
    .await
    .unwrap();
    assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
    assert_eq!(
        server.workers().map(|(name, _)| name).collect::<Vec<_>>(),
        ["local"]
    );
    let target = requests
        .borrow()
        .iter()
        .find(|target| target.entrypoint.as_deref() == Some("Admin"))
        .cloned()
        .unwrap();
    assert_eq!(target.service, "remote");
    assert_eq!(target.props_json.as_deref(), Some("{\"role\":\"reader\"}"));
    let app = server.lookup(&designator("local", None));
    let response = selected_worker_request(&factory, Rc::clone(&app),
                b"POST https://example.com/data HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\nContent-Length: 4\r\n\r\nbody",
            ).await;
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(b"body"));
    let response = selected_worker_get(&factory, Rc::clone(&app), "/loader").await;
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    assert!(response.ends_with(b"loader aborted twice"));
    assert_eq!(loads.get(), 2, "synchronous abort left a named stub cached");
    if case == SelectedWorkerCase::Unlink {
        let response = selected_worker_get(&factory, Rc::clone(&app), "/unlink").await;
        assert!(response.ends_with(b"unlinks prepared"));
        assert_eq!(loads.get(), 4);
        assert_eq!(unlinks.get(), 0, "hosted workers unlinked before teardown");
    }
    if case == SelectedWorkerCase::Cache {
        selected_worker_cache(&factory, Rc::clone(&app), &contexts).await;
    }
    drop(app);
    let weak_factory = Rc::downgrade(&factory);
    drop(server);
    assert_eq!(
        unlinks.get(),
        if case == SelectedWorkerCase::Unlink {
            2
        } else {
            0
        },
        "every hosted named worker must unlink exactly once",
    );
    crate::entry::shutdown(factory).await.unwrap();
    assert!(
        weak_factory.upgrade().is_none(),
        "worker factory survived shutdown"
    );
    assert_eq!(live.get(), 0);
}

async fn selected_worker_cache(
    factory: &Factory,
    app: Rc<dyn Channel>,
    contexts: &RefCell<Vec<Option<&'static str>>>,
) {
    let first = crate::request_context::channel(
        Rc::clone(&app),
        crate::request_context::new(std::sync::Arc::new("first")),
    );
    let second = crate::request_context::channel(
        Rc::clone(&app),
        crate::request_context::new(std::sync::Arc::new("second")),
    );
    let (first, second) = futures::join!(
        selected_worker_get(factory, first, "/cache"),
        selected_worker_get(factory, second, "/cache"),
    );
    assert!(first.ends_with(b"cache done"));
    assert!(second.ends_with(b"cache done"));
    let (first, second) = futures::join!(
        selected_worker_get(factory, Rc::clone(&app), "/cache"),
        selected_worker_get(factory, Rc::clone(&app), "/cache"),
    );
    assert!(first.ends_with(b"cache done"));
    assert!(second.ends_with(b"cache done"));
    let contexts = contexts.borrow();
    assert_eq!(contexts.len(), 9);
    assert_eq!(contexts[0], None, "ordinary unscoped service request");
    for start in [1, 3] {
        let mut pair = contexts[start..start + 2].to_vec();
        pair.sort_unstable();
        assert_eq!(pair, [Some("first"), Some("second")]);
    }
    assert_eq!(&contexts[5..], &[None; 4], "stale scoped cache provenance");
}

async fn selected_worker_get(factory: &Factory, app: Rc<dyn Channel>, path: &str) -> Vec<u8> {
    let head = format!(
        "GET https://example.com{path} HTTP/1.1\r\nHost: example.com\r\nConnection: close\r\n\r\n"
    );
    selected_worker_request(factory, app, head.as_bytes()).await
}

async fn selected_worker_request(factory: &Factory, app: Rc<dyn Channel>, head: &[u8]) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let (ours, mut peer) = tokio::io::duplex(1024);
    let shutdown = kj_hyper::server::Shutdown::new();
    let served = kj_hyper::server::serve_connection(
        ours,
        Box::pin(std::future::pending()),
        crate::services::header_table(factory),
        Rc::new(kj_hyper::server::ServerSettings::default()),
        Rc::new(SuppliedServiceHandler(app)),
        &shutdown,
    );
    let client = async {
        peer.write_all(head).await.unwrap();
        let mut response = Vec::new();
        peer.read_to_end(&mut response).await.unwrap();
        response
    };
    let (served, response) = futures::join!(served, client);
    served.unwrap();
    assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
    response
}

impl Channel for NullChannel {
    fn start_request(&self, _metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        Err(kj::failed!("null"))
    }

    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(kj::failed!("null"))
    }
}

#[test]
fn supplied_services_are_leaf_capabilities_in_name_order() {
    let first: Rc<dyn Channel> = Rc::new(NullChannel);
    let second: Rc<dyn Channel> = Rc::new(NullChannel);
    let channels = HashMap::from([
        ("z".to_owned(), Rc::clone(&second)),
        ("a".to_owned(), Rc::clone(&first)),
    ]);
    let mut actors = ActorConfigs::new();
    let services = Services::with_channels(&mut actors, channels).unwrap();
    let names: Vec<_> = services.0.keys().map(String::as_str).collect();
    assert_eq!(names, ["a", "z"]);
    assert!(Rc::ptr_eq(&services.0["a"].channel(), &first));
    assert!(Rc::ptr_eq(&services.0["z"].channel(), &second));
    assert!(services.0["a"].as_worker().is_none());
    assert!(actors["a"].is_empty());
    assert!(actors["z"].is_empty());

    let (report, errors) = collecting_reporter();
    let channel = lookup_service(&services.0, &report, &designator("a", None));
    assert!(Rc::ptr_eq(&channel, &first));
    assert!(errors.borrow().is_empty());
}

#[test]
fn supplied_service_conflicts_fail_before_mutating_actor_configs() {
    let mut actors = ActorConfigs::from([
        ("z".to_owned(), ActorConfigMap::new()),
        ("a".to_owned(), ActorConfigMap::new()),
    ]);
    let channel: Rc<dyn Channel> = Rc::new(NullChannel);
    let channels = HashMap::from([
        ("z".to_owned(), Rc::clone(&channel)),
        ("other".to_owned(), Rc::clone(&channel)),
        ("a".to_owned(), channel),
    ]);
    let error = Services::with_channels(&mut actors, channels)
        .err()
        .unwrap();
    assert_eq!(
        error.description(),
        "Services supplied by the embedder conflict with configured services: a, z"
    );
    assert_eq!(actors.len(), 2);
    assert!(!actors.contains_key("other"));
}

#[test]
fn supplied_services_hold_channel_state_until_graph_teardown() {
    let channel: Rc<dyn Channel> = Rc::new(NullChannel);
    {
        let channels = HashMap::from([("outbound".to_owned(), Rc::clone(&channel))]);
        let services = Services::with_channels(&mut ActorConfigs::new(), channels).unwrap();
        assert_eq!(Rc::strong_count(&channel), 2);
        drop(services);
    }
    assert_eq!(Rc::strong_count(&channel), 1);
}

#[test]
fn no_supplied_services_preserves_config_actor_entries() {
    let mut actors = ActorConfigs::from([("app".to_owned(), ActorConfigMap::new())]);
    let services = Services::with_channels(&mut actors, HashMap::new()).unwrap();
    assert!(services.0.is_empty());
    assert_eq!(actors.len(), 1);
    assert!(actors.contains_key("app"));
}

struct SuppliedEcho(Rc<std::cell::Cell<usize>>, Option<Rc<tokio::sync::Barrier>>);

impl Drop for SuppliedEcho {
    fn drop(&mut self) {
        self.0.set(self.0.get() - 1);
    }
}

#[async_trait::async_trait(?Send)]
impl kj::http::Service for SuppliedEcho {
    async fn request<'a>(
        &'a mut self,
        method: kj::http::Method,
        _url: &'a [u8],
        headers: kj::http::HeadersRef<'a>,
        mut request_body: std::pin::Pin<&'a mut kj::io::AsyncInputStream>,
        response: kj::http::ServiceResponse<'a>,
    ) -> Result<()> {
        if method == kj::http::Method::PURGE
            && let Some(barrier) = &self.1
        {
            barrier.wait().await;
        }
        let headers = headers.clone_shallow();
        let length = request_body.as_mut().try_get_length();
        let mut output = response.send(200, "OK", &headers, length)?;
        let mut buffer = [0; 4096];
        loop {
            let amount = request_body.as_mut().try_read(&mut buffer, 1).await?;
            if amount == 0 {
                return Ok(());
            }
            output.write(&buffer[..amount]).await?;
        }
    }

    async fn connect<'a>(
        &'a mut self,
        _host: &'a [u8],
        _headers: kj::http::HeadersRef<'a>,
        _connection: std::pin::Pin<&'a mut kj::io::AsyncIoStream>,
        _response: kj::http::ConnectResponse<'a>,
        _settings: kj::http::ConnectSettings<'a>,
    ) -> Result<()> {
        Err(kj::not_implemented!("test echo does not handle CONNECT"))
    }
}

#[async_trait::async_trait(?Send)]
impl ::worker::Interface for SuppliedEcho {
    async fn run_scheduled(
        &mut self,
        _scheduled_time: &std::time::SystemTime,
        _cron: &str,
    ) -> ::worker::Result<::worker::ScheduledResult> {
        Err(kj::not_implemented!(
            "test echo does not handle scheduled events"
        ))
    }

    async fn run_alarm(
        &mut self,
        _scheduled_time: &std::time::SystemTime,
        _retry_count: u32,
    ) -> ::worker::Result<::worker::AlarmResult> {
        Err(kj::not_implemented!("test echo does not handle alarms"))
    }
}

struct SuppliedEchoChannel(Rc<std::cell::Cell<usize>>);

impl Channel for SuppliedEchoChannel {
    fn start_request(&self, _metadata: KjOwn<RequestMetadata>) -> Result<KjOwn<WorkerInterface>> {
        self.0.set(self.0.get() + 1);
        Ok(::worker::Interface::into_kj(SuppliedEcho(
            Rc::clone(&self.0),
            None,
        )))
    }

    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(crate::services::not_transferable("test echo"))
    }
}

struct CacheContextChannel {
    live: Rc<std::cell::Cell<usize>>,
    contexts: Rc<RefCell<Vec<Option<&'static str>>>>,
    barrier: Rc<tokio::sync::Barrier>,
}

impl Channel for CacheContextChannel {
    fn start_request(
        &self,
        mut metadata: KjOwn<RequestMetadata>,
    ) -> Result<KjOwn<WorkerInterface>> {
        let value = crate::request_context::from_metadata(metadata.pin_mut())?;
        self.contexts.borrow_mut().push(value.map(|value| {
            *value
                .downcast_ref::<&'static str>()
                .expect("fixture context contains a static string")
        }));
        self.live.set(self.live.get() + 1);
        Ok(::worker::Interface::into_kj(SuppliedEcho(
            Rc::clone(&self.live),
            Some(Rc::clone(&self.barrier)),
        )))
    }

    fn token(&self, _usage: TokenUsage) -> Result<KjOwn<PendingToken>> {
        Err(crate::services::not_transferable("test cache"))
    }
}

#[test]
fn supplied_request_owns_its_state_after_service_graph_is_dropped() {
    let live = Rc::new(std::cell::Cell::new(0));
    let channel: Rc<dyn Channel> = Rc::new(SuppliedEchoChannel(Rc::clone(&live)));
    let channels = HashMap::from([("provided".to_owned(), channel)]);
    let services = Services::with_channels(&mut ActorConfigs::new(), channels).unwrap();
    let request = services.0["provided"]
        .channel()
        .start_request(dummy_metadata())
        .unwrap();
    assert_eq!(live.get(), 1);
    drop(services);
    assert_eq!(live.get(), 1);
    drop(request);
    assert_eq!(live.get(), 0);
}

struct SuppliedServiceHandler(Rc<dyn Channel>);

#[async_trait::async_trait(?Send)]
impl kj_hyper::server::Handler for SuppliedServiceHandler {
    async fn request<'a>(
        &'a self,
        method: kj::http::Method,
        url: &'a [u8],
        headers: kj::http::HeadersRef<'a>,
        body: std::pin::Pin<&'a mut kj::io::AsyncInputStream>,
        response: kj::http::ServiceResponse<'a>,
    ) -> Result<()> {
        let mut worker = ::worker::CxxWorkerInterface::new(self.0.start_request(dummy_metadata())?);
        kj::http::Service::request(&mut worker, method, url, headers, body, response).await
    }

    async fn connect<'a>(
        &'a self,
        _host: &'a [u8],
        _headers: kj::http::HeadersRef<'a>,
        _connect: kj_hyper::server::Connect,
    ) -> Result<()> {
        Err(kj::not_implemented!("test handler does not handle CONNECT"))
    }
}

fn supplied_worker_config() -> Vec<u64> {
    let mut message = capnp::message::Builder::new_default();
    {
        let config = message.init_root::<config::Builder>();
        let mut service = config.init_services(1).get(0);
        service.set_name("app");
        let mut worker = service.init_worker();
        worker.set_compatibility_date("2026-10-19");
        worker
            .reborrow()
            .init_global_outbound()
            .set_name("provided");
        let mut module = worker.reborrow().init_modules(1).get(0);
        module.set_name("main.js");
        module.set_es_module(
            "export default { fetch(request, env) { return env.PROVIDER.fetch(request); } };",
        );
        let mut binding = worker.init_bindings(1).get(0);
        binding.set_name("PROVIDER");
        binding.init_service().set_name("provided");
    }
    let bytes = capnp::serialize::write_message_to_words(&message);
    bytes
        .chunks_exact(8)
        .map(|word| u64::from_ne_bytes(word.try_into().unwrap()))
        .collect()
}

#[test]
fn standard_worker_binding_streams_through_a_supplied_channel_without_sockets() {
    let words = supplied_worker_config();
    let options = ffi::ServeOrTestOptions {
        perfetto_trace_path: kj_rs::KjMaybe::None,
        perfetto_trace_categories: kj_rs::KjMaybe::None,
        async_trace_path: kj_rs::KjMaybe::None,
        async_trace_stacks: kj_rs::KjMaybe::None,
        async_trace_promises: false,
        experimental: false,
        pyodide_package_disk_cache_dir: kj_rs::KjMaybe::None,
        pyodide_bundle_disk_cache_dir: kj_rs::KjMaybe::None,
        python_save_snapshot: false,
        python_save_baseline_snapshot: false,
        python_load_snapshot: kj_rs::KjMaybe::None,
        python_snapshot_dir: kj_rs::KjMaybe::None,
    };
    let mut runtime = kj_rs_tokio::Runtime::new().unwrap();
    let factory = ffi::bootstrap(
        runtime.context(),
        words,
        &options,
        kj_rs::KjMaybe::None,
        kj_rs::KjMaybe::None,
    )
    .unwrap();
    runtime
        .block_on(async move {
            let factory = Rc::new(Factory::new(factory));
            let (report, _) = collecting_reporter();
            let conflicting: Rc<dyn Channel> = Rc::new(NullChannel);
            let conflict = Server::start_with_services(
                Rc::clone(&factory),
                &Overrides::default(),
                report,
                HashMap::new(),
                HashMap::from([("app".to_owned(), conflicting)]),
            )
            .await
            .err()
            .unwrap();
            assert_eq!(
                conflict.description(),
                "Services supplied by the embedder conflict with configured services: app"
            );
            let (report, errors) = collecting_reporter();
            let live = Rc::new(std::cell::Cell::new(0));
            let channel: Rc<dyn Channel> = Rc::new(SuppliedEchoChannel(Rc::clone(&live)));
            let server = Server::start_with_services(
                Rc::clone(&factory),
                &Overrides::default(),
                report,
                HashMap::new(),
                HashMap::from([("provided".to_owned(), channel)]),
            )
            .await
            .unwrap();
            assert!(errors.borrow().is_empty(), "{:?}", errors.borrow());
            let app = server.lookup(&designator("app", None));
            let (ours, peer) = tokio::io::duplex(1024);
            let shutdown = kj_hyper::server::Shutdown::new();
            let connection = kj_hyper::server::serve_connection(
                ours,
                Box::pin(std::future::pending()),
                crate::services::header_table(&factory),
                Rc::new(kj_hyper::server::ServerSettings::default()),
                Rc::new(SuppliedServiceHandler(app)),
                &shutdown,
            );
            let payload = vec![b'x'; 128 * 1024];
            let client = supplied_payload_response(peer, &payload);
            let (connected, response) = futures::join!(connection, client);
            connected.unwrap();
            let body = response
                .windows(4)
                .position(|bytes| bytes == b"\r\n\r\n")
                .unwrap()
                + 4;
            assert!(response.starts_with(b"HTTP/1.1 200 OK\r\n"));
            assert_eq!(&response[body..], payload);
            drop(server);
            crate::entry::shutdown(factory).await.unwrap();
            assert_eq!(live.get(), 0);
        })
        .unwrap();
}

async fn supplied_payload_response(mut peer: tokio::io::DuplexStream, payload: &[u8]) -> Vec<u8> {
    use tokio::io::AsyncReadExt;
    use tokio::io::AsyncWriteExt;

    let head = format!(
        "POST https://example.com/data HTTP/1.1\r\nHost: example.com\r\n\
         Connection: close\r\nContent-Length: {}\r\n\r\n",
        payload.len()
    );
    peer.write_all(head.as_bytes()).await.unwrap();
    let (mut read, mut write) = tokio::io::split(peer);
    let send = async { write.write_all(payload).await.unwrap() };
    let receive = async {
        let mut response = Vec::new();
        read.read_to_end(&mut response).await.unwrap();
        response
    };
    let ((), response) = futures::join!(send, receive);
    response
}

fn designator(service: &str, entrypoint: Option<&str>) -> Designator {
    Designator {
        service: service.to_owned(),
        entrypoint: entrypoint.map(str::to_owned),
        props_json: None,
        error_context: "Worker \"w\"'s binding \"b\"".to_owned(),
    }
}

fn leaf_services() -> LinkedHashMap<String, Service> {
    let mut services = LinkedHashMap::new();
    services.insert("net".to_owned(), Service::Leaf(Rc::new(NullChannel)));
    services.insert(
        "bad".to_owned(),
        Service::Leaf(Rc::new(InvalidConfigChannel)),
    );
    services
}

#[test]
fn lookup_of_a_missing_service_is_reported() {
    let services = leaf_services();
    let (report, errors) = collecting_reporter();
    let channel = lookup_service(&services, &report, &designator("nope", None));
    assert!(channel.start_request(dummy_metadata()).is_err());
    assert_eq!(
        *errors.borrow(),
        [
            "Worker \"w\"'s binding \"b\" refers to a service \"nope\", but no such service \
                 is defined."
        ]
    );

    let (report, errors) = collecting_reporter();
    lookup_actor_class(&services, &report, &designator("nope", None));
    assert_eq!(
        *errors.borrow(),
        [
            "Worker \"w\"'s binding \"b\" refers to a service \"nope\", but no such service \
                 is defined."
        ]
    );
}

#[test]
fn lookup_of_a_leaf_with_an_entrypoint_is_reported() {
    let services = leaf_services();
    let (report, errors) = collecting_reporter();
    lookup_service(&services, &report, &designator("net", Some("ep")));
    assert_eq!(
        *errors.borrow(),
        [
            "Worker \"w\"'s binding \"b\" refers to service \"net\" with a named entrypoint \
                 \"ep\", but \"net\" is not a Worker, so does not have any named entrypoints."
        ]
    );

    let (report, errors) = collecting_reporter();
    lookup_service(&services, &report, &designator("net", None));
    assert!(errors.borrow().is_empty());
}

#[test]
fn lookup_of_a_leaf_as_an_actor_class_is_reported() {
    let services = leaf_services();
    let (report, errors) = collecting_reporter();
    lookup_actor_class(&services, &report, &designator("net", Some("C")));
    lookup_actor_class(&services, &report, &designator("net", None));
    assert_eq!(
        *errors.borrow(),
        [
            "Worker \"w\"'s binding \"b\" refers to service \"net\" with a named Durable \
                 Object entrypoint \"C\", but \"net\" is not a Worker, so does not have any named \
                 entrypoints.",
            "Worker \"w\"'s binding \"b\" refers to service \"net\" as a Durable Object \
                 class, but \"net\" is not a Worker, so cannot be used as a class.",
        ]
    );
}

#[test]
fn invalid_services_fail_requests_with_a_js_error() {
    let services = leaf_services();
    let (report, errors) = collecting_reporter();
    let channel = lookup_service(&services, &report, &designator("bad", None));
    assert!(errors.borrow().is_empty());
    let error = channel.start_request(dummy_metadata()).err().unwrap();
    assert_eq!(
        error.description(),
        "jsg.Error: Service cannot handle requests because its config is invalid."
    );
}

fn dummy_metadata() -> KjOwn<RequestMetadata> {
    ffi::new_request_metadata(kj_rs::KjMaybe::None, kj_rs::KjMaybe::None)
}
