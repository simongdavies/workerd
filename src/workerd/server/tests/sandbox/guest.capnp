using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    ( name = "guest",
      worker = (
        compatibilityDate = "2025-08-01",
        modules = [(name = "guest.js", esModule = embed "guest.js")]
      )
    )
  ],
  sockets = [
    ( name = "rpc",
      address = "127.0.0.1:8791",
      http = (capnpConnectHost = "sandbox"),
      service = "guest"
    )
  ]
);
