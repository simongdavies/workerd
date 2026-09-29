using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    ( name = "coordinator",
      worker = (
        compatibilityDate = "2025-08-01",
        modules = [(name = "coordinator.js", esModule = embed "coordinator.js")],
        bindings = [(name = "GUEST", service = "guest")]
      )
    ),
    ( name = "guest",
      sandboxedWorker = (
        workerId = "smoke-worker",
        version = "v1",
        address = "127.0.0.1:8791",
        capnpConnectHost = "sandbox"
      )
    )
  ],
  sockets = [
    (name = "http", address = "127.0.0.1:8787", http = (), service = "coordinator")
  ]
);
