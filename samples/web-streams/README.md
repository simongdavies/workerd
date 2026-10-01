# Web Streams Example

Demonstrates Web Streams API features including custom ReadableStream, TransformStream,
and byte streams.

## Endpoints

- `/sync` - Deterministic 9,441-byte stream with uppercase transform (synchronous)
- `/async` - Default stream with uppercase transform (with delays)
- `/bytes/sync` - Byte stream, no transform (synchronous)
- `/bytes/async` - Byte stream, no transform (with delays)

The sandbox executor self-test pins the `/sync` size so Azure cold, restored, and prewarmed pool
measurements use the same payload.

## Running

```sh
$ bazel run //src/workerd/server:workerd -- serve $(pwd)/samples/web-streams/config.capnp
```

Or with a local workerd binary:

```sh
$ ./workerd serve config.capnp
```

## Testing

```sh
$ curl http://localhost:8080/sync
$ curl http://localhost:8080/async
$ curl http://localhost:8080/bytes/sync
$ curl http://localhost:8080/bytes/async
```
