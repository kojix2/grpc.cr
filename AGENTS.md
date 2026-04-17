# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Native Crystal gRPC library built directly on **libnghttp2** via FFI. The library source lives in `grpc/` and examples in `grpc/examples/`. Design philosophy: low-level HTTP/2 details are internal only; the public API must be ergonomic Crystal.

## Commands

```bash
# Run tests (from grpc/)
crystal spec

# Run a single spec file
crystal spec spec/grpc_spec.cr

# Build the protoc plugin
crystal build src/protoc-gen-crystal-grpc_main.cr -o bin/protoc-gen-crystal-grpc

# Generate gRPC stubs from a .proto file
protoc --plugin=protoc-gen-crystal-grpc=bin/protoc-gen-crystal-grpc \
       --crystal-grpc_out=OUTPUT_DIR \
       path/to/service.proto

# Run examples
crystal run examples/helloworld/server.cr
crystal run examples/helloworld/client.cr -- localhost 50051 Alice

# Format code
crystal tool format

# Lint
ameba
```

## System Dependencies

- `libnghttp2` — HTTP/2 library (Arch: `pacman -S nghttp2`, Debian: `apt install libnghttp2-dev`, macOS: `brew install nghttp2`)
- Crystal >= 1.19.1

## Architecture

```
Public API (server.cr, channel.cr, stream.cr, interceptor.cr)
    ↓
Service abstraction (service_handler.cr, call_context.cr, metadata.cr, codec.cr)
    ↓
Transport layer (transport/http2_connection.cr, http2_server_connection.cr, http2_client_connection.cr)
    ↓
libnghttp2 FFI (transport/lib_nghttp2.cr)
    ↓
Crystal stdlib TCPSocket
```

### Transport Layer

- **`http2_connection.cr`** — Base class wrapping an nghttp2 session. Sets up C callbacks (on_begin_headers, on_header, on_data_chunk, on_frame_recv, on_stream_close) that delegate to Crystal methods. Manages `StreamData` accumulation and a `SendBuffer` for outgoing bytes.
- **`http2_server_connection.cr`** — Handles inbound connections. On `END_STREAM`, spawns a fiber that routes the request path to the matching `Service`, runs interceptors, encodes the response via `Codec`, and submits HEADERS + DATA + trailer HEADERS frames.
- **`http2_client_connection.cr`** — Manages outbound RPCs via `PendingCall` / `PendingStream`. Submits request HEADERS + DATA, then blocks a Crystal `::Channel` until the response trailers arrive.
- **`lib_nghttp2.cr`** — Thin FFI bindings; declares C structs, enums, and function signatures for libnghttp2.

### Public API Layer

- **`server.cr`** — `GRPC::Server`: registers services, binds TCP, accepts connections (one fiber each), routes to `Http2ServerConnection`.
- **`channel.cr`** — `GRPC::Channel`: creates/reuses `Http2ClientConnection`. Entry point for unary, server-stream, client-stream, and bidi calls. Accepts `tls_context:` for custom TLS settings.
- **`service_handler.cr`** — `GRPC::Service` abstract base class. Generated service stubs inherit this and override `dispatch(method, body, ctx)`.
- **`stream.cr`** — Typed wrappers for streaming RPCs: `ServerStream(T)`, `ClientStream`, `BidiCall`, `ResponseStream`.
- **`codec.cr`** — gRPC message framing: 5-byte header (1 compression flag + 4 length) prefixed to Protobuf bytes.
- **`metadata.cr`** — Case-insensitive, multi-value HTTP header map.
- **`interceptor.cr`** — Middleware chains for all four RPC variants (unary, server-streaming, client-streaming, bidi).  Each variant has its own proc alias and default pass-through method; only override what you need.
- **`call_context.cr`** — `ServerContext` and `ClientContext` carrying peer address, deadline, and metadata.

### protoc-gen-crystal-grpc

`src/protoc-gen-crystal-grpc.cr` is a standalone `protoc` plugin binary.  Given a proto file it emits a `.grpc.cr` file containing an abstract `GRPC::Service` subclass and a typed client stub for every service defined in the file.  All four RPC streaming variants are supported.

The plugin is self-contained — it includes a minimal binary-format protobuf decoder/encoder with no external dependencies.  Key modules:

- **`ProtobufWireDecoder`** — decodes varint-encoded fields from `CodeGeneratorRequest` / `FileDescriptorProto` / etc.
- **`ProtobufWireEncoder`** — encodes `CodeGeneratorResponse` back to binary.
- **`CrystalGrpcCodeGenerator`** — walks descriptors and emits Crystal source.

Generated code conventions:
- Service class: `{ServiceName}Service < GRPC::Service`  (e.g. `GreeterService`)
- Client class: `{ServiceName}Client`  (e.g. `GreeterClient`)
- RPC method names: snake_cased  (`SayHello` → `say_hello`)
- Package wrapping: `helloworld` → `module Helloworld`
- Cross-package types: `.google.protobuf.Empty` → `Google::Protobuf::Empty`

### Known Limitations

- Bidirectional streaming is half-duplex (client sends all, then server streams)
- No message compression support

## nghttp2 Critical Rules

Two hard-won constraints (see memory files for full details):

1. **Never write the HTTP/2 client preface manually.** `nghttp2_session_mem_send` includes it automatically; writing it again corrupts the stream.
2. **`nghttp2_submit_trailer` must be called from inside the data source read callback**, not after `nghttp2_session_send` / `nghttp2_session_mem_send`. Calling it outside kills DATA frames.

## Testing

- `spec/grpc_spec.cr` — Unit tests for `Status`, `Metadata`, `Codec`, context types
- `spec/integration_spec.cr` — Full round-trip integration tests (spins up a real server on a random port); includes TLS round-trip tests using `spec/test.crt` + `spec/test.key` (self-signed)
- `spec/protoc_gen_spec.cr` — Unit tests for the protoc plugin code generator
- `spec/debug_spec.cr` — Low-level debugging helpers
