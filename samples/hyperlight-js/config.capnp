# workerd sample: run a JavaScript Worker inside a Hyperlight + QuickJS micro-VM.
#
# This uses the `hyperlightJs` service type. A request to it never enters V8 or
# JSG -- it is marshalled to a JSON event and run by a handler inside a
# hardware-isolated micro-VM, which returns the response.
#
# Requires Linux/x86-64 with access to /dev/kvm. See ./README.md and
# ../../docs/hyperlight-js.md for build + run instructions.
#
#   workerd serve samples/hyperlight-js/config.capnp
#   curl -i localhost:8080/json
#
# Refer to the comments in /src/workerd/server/workerd.capnp for the full schema.
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  # A single hyperlightJs service running the handler embedded from router.js.
  services = [
    ( name = "js",
      hyperlightJs = (
        handler = embed "router.js",
      ),
    ),
  ],

  # Listen on localhost:8080 and dispatch to the "js" service.
  sockets = [
    ( name = "http", address = "*:8080", service = "js" ),
  ],
);
