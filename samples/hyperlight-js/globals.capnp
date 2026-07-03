# workerd sample (Hyperlight micro-VM backend): the WinterTC globals tour.
#
#   workerd serve samples/hyperlight-js/globals.capnp
#   curl -s "localhost:8080/api/v1/items?page=1&sort=name" | python3 -m json.tool
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    ( name = "js",
      hyperlightJs = (
        handler = embed "globals.js",
      ),
    ),
  ],
  sockets = [
    ( name = "http", address = "*:8080", service = "js" ),
  ],
);
