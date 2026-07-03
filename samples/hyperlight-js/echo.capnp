# workerd sample (Hyperlight micro-VM backend): echo the marshalled request.
#
#   workerd serve samples/hyperlight-js/echo.capnp
#   curl -X POST -H "X-Custom: hi" --data "the body" "localhost:8080/echo?q=1"
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    ( name = "js",
      hyperlightJs = (
        handler = embed "echo.js",
      ),
    ),
  ],
  sockets = [
    ( name = "http", address = "*:8080", service = "js" ),
  ],
);
