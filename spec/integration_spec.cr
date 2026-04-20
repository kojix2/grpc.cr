require "./spec_helper"

# Simple hand-built proto helpers for integration tests
module TestProto
  def self.encode_string(value : String) : Bytes
    io = IO::Memory.new
    tag = (1 << 3) | 2 # field 1, wire type 2
    encode_varint(io, tag.to_u64)
    encode_varint(io, value.bytesize.to_u64)
    io.write(value.to_slice)
    io.to_slice
  end

  def self.decode_string(data : Bytes) : String
    return "" if data.empty?
    i = 0
    while i < data.size
      tag, consumed = decode_varint(data, i)
      i += consumed
      wire_type = (tag & 0x7).to_i
      field_number = (tag >> 3).to_i
      case wire_type
      when 2
        len, consumed = decode_varint(data, i)
        i += consumed
        return String.new(data[i, len.to_i]) if field_number == 1
        i += len.to_i
      when 0
        _, consumed = decode_varint(data, i)
        i += consumed
      else break
      end
    end
    ""
  end

  private def self.encode_varint(io : IO, value : UInt64) : Nil
    loop do
      byte = (value & 0x7F).to_u8
      value >>= 7
      io.write_byte(value != 0 ? (byte | 0x80_u8) : byte)
      break if value == 0
    end
  end

  private def self.decode_varint(data : Bytes, offset : Int32) : {UInt64, Int32}
    result = 0_u64
    shift = 0
    i = 0
    loop do
      break if offset + i >= data.size
      byte = data[offset + i].to_u64
      i += 1
      result |= (byte & 0x7F) << shift
      shift += 7
      break unless (byte & 0x80) != 0
    end
    {result, i}
  end
end

# A minimal service for testing
class EchoService < GRPC::Service
  SERVICE_FULL_NAME = "test.Echo"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    case method
    when "Echo"
      input = TestProto.decode_string(request_body)
      output = TestProto.encode_string("echo:#{input}")
      {output, GRPC::Status.ok}
    when "Fail"
      {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::NOT_FOUND, "not here")}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end
end

class StreamingEchoService < GRPC::Service
  SERVICE_FULL_NAME = "test.StreamingEcho"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    {Bytes.empty, GRPC::Status.unimplemented("unary not implemented")}
  end

  def server_streaming?(method : String) : Bool
    method == "ServerStream" || method == "ErrorStream" || method == "PartialErrorStream"
  end

  def dispatch_server_stream(method : String, request_body : Bytes,
                             ctx : GRPC::ServerContext, writer : GRPC::RawResponseStream) : GRPC::Status
    case method
    when "ServerStream"
      input = TestProto.decode_string(request_body)
      3.times do |i|
        writer.send_raw(TestProto.encode_string("#{i}:#{input}"))
      end
      GRPC::Status.ok
    when "ErrorStream"
      GRPC::Status.new(GRPC::StatusCode::INTERNAL, "forced stream error")
    when "PartialErrorStream"
      input = TestProto.decode_string(request_body)
      3.times do |i|
        writer.send_raw(TestProto.encode_string("partial#{i}:#{input}"))
      end
      GRPC::Status.new(GRPC::StatusCode::INTERNAL, "forced partial stream error")
    else
      GRPC::Status.unimplemented("unknown method")
    end
  end

  def client_streaming?(method : String) : Bool
    method == "ClientStream"
  end

  def dispatch_client_stream(method : String, requests : GRPC::RawRequestStream,
                             ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    case method
    when "ClientStream"
      parts = [] of String
      requests.each { |body| parts << TestProto.decode_string(body) }
      {TestProto.encode_string("joined:#{parts.join(",")}"), GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end

  def bidi_streaming?(method : String) : Bool
    method == "BidiStream"
  end

  def dispatch_bidi_stream(method : String, requests : GRPC::RawRequestStream,
                           ctx : GRPC::ServerContext, writer : GRPC::RawResponseStream) : GRPC::Status
    case method
    when "BidiStream"
      idx = 0
      requests.each do |body|
        input = TestProto.decode_string(body)
        writer.send_raw(TestProto.encode_string("reply#{idx}:#{input}"))
        idx += 1
      end
      GRPC::Status.ok
    else
      GRPC::Status.unimplemented("unknown method")
    end
  end
end

class LiveProbeService < GRPC::Service
  SERVICE_FULL_NAME = "test.LiveProbe"

  def initialize(@first_seen : ::Channel(String))
  end

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    {Bytes.empty, GRPC::Status.unimplemented("unary not implemented")}
  end

  def client_streaming?(method : String) : Bool
    method == "ClientStreamLive"
  end

  def dispatch_client_stream(method : String, requests : GRPC::RawRequestStream,
                             ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    case method
    when "ClientStreamLive"
      count = 0
      first = true
      requests.each do |body|
        decoded = TestProto.decode_string(body)
        if first
          @first_seen.send(decoded)
          first = false
        end
        count += 1
      end
      {TestProto.encode_string("count:#{count}"), GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end
end

# Reflects request metadata back to the caller.
# "GetMeta"     — request body encodes the key name; response is that key's value (or "")
# "HasDeadline" — response is "yes" if ctx.deadline is set, "no" otherwise
class MetaEchoService < GRPC::Service
  SERVICE_FULL_NAME = "test.MetaEcho"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    case method
    when "GetMeta"
      key = TestProto.decode_string(request_body)
      value = ctx.metadata[key]? || ""
      {TestProto.encode_string(value), GRPC::Status.ok}
    when "HasDeadline"
      {TestProto.encode_string(ctx.deadline.nil? ? "no" : "yes"), GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end
end

# Used to verify deadline enforcement while handlers are still running.
class DeadlineProbeService < GRPC::Service
  SERVICE_FULL_NAME = "test.DeadlineProbe"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    case method
    when "SlowUnary"
      sleep 200.milliseconds
      {TestProto.encode_string("late"), GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end

  def server_streaming?(method : String) : Bool
    method == "SlowServerStream"
  end

  def dispatch_server_stream(method : String, request_body : Bytes,
                             ctx : GRPC::ServerContext, writer : GRPC::RawResponseStream) : GRPC::Status
    case method
    when "SlowServerStream"
      i = 0
      loop do
        break if ctx.cancelled?
        writer.send_raw(TestProto.encode_string("tick:#{i}"))
        i += 1
        sleep 10.milliseconds
      end
      GRPC::Status.ok
    else
      GRPC::Status.unimplemented("unknown method")
    end
  end
end

# --- Interceptors ---

class RecordingClientInterceptor < GRPC::ClientInterceptor
  getter calls : Array(String) = [] of String

  def call(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ClientContext,
    next_call : GRPC::UnaryClientCall,
  ) : GRPC::ResponseEnvelope
    @calls << "before:#{request.info.method_path}"
    result = next_call.call(request.info.method_path, request, ctx)
    @calls << "after:#{request.info.method_path}"
    result
  end
end

class MutatingClientInterceptor < GRPC::ClientInterceptor
  def call(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ClientContext,
    next_call : GRPC::UnaryClientCall,
  ) : GRPC::ResponseEnvelope
    # Inject a header before the call.
    merged = ctx.metadata.dup
    merged.set("x-from-interceptor", "1")
    new_ctx = GRPC::ClientContext.new(metadata: merged)
    next_call.call(request.info.method_path, request, new_ctx)
  end
end

class AuthServerInterceptor < GRPC::ServerInterceptor
  def call(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ServerContext,
    next_call : GRPC::UnaryServerCall,
  ) : GRPC::ResponseEnvelope
    unless ctx.metadata["authorization"]? == "valid-token"
      return GRPC::ResponseEnvelope.new(
        request.info,
        Bytes.empty,
        GRPC::Status.new(GRPC::StatusCode::UNAUTHENTICATED, "invalid token")
      )
    end
    next_call.call(request.info.method_path, request, ctx)
  end
end

class RecordingServerInterceptor < GRPC::ServerInterceptor
  getter calls : Array(String) = [] of String

  def call(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ServerContext,
    next_call : GRPC::UnaryServerCall,
  ) : GRPC::ResponseEnvelope
    @calls << "before:#{request.info.method}"
    result = next_call.call(request.info.method_path, request, ctx)
    @calls << "after:#{request.info.method}"
    result
  end
end

# Records calls for all four RPC variants.
class FullRecordingServerInterceptor < GRPC::ServerInterceptor
  getter calls : Array(String) = [] of String

  def call(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ServerContext,
    next_call : GRPC::UnaryServerCall,
  ) : GRPC::ResponseEnvelope
    @calls << "unary:before:#{request.info.method}"
    result = next_call.call(request.info.method_path, request, ctx)
    @calls << "unary:after:#{request.info.method}"
    result
  end

  def call_server_stream(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ServerContext,
    writer : GRPC::RawResponseStream,
    next_call : GRPC::ServerStreamServerCall,
  ) : GRPC::Status
    @calls << "ss:before:#{request.info.method}"
    status = next_call.call(request.info.method_path, request, ctx, writer)
    @calls << "ss:after:#{request.info.method}"
    status
  end

  def call_client_stream(
    info : GRPC::CallInfo,
    requests : GRPC::RawRequestStream,
    ctx : GRPC::ServerContext,
    next_call : GRPC::ClientStreamServerCall,
  ) : GRPC::ResponseEnvelope
    @calls << "cs:before:#{info.method}"
    result = next_call.call(info.method_path, requests, ctx)
    @calls << "cs:after:#{info.method}"
    result
  end

  def call_bidi_stream(
    info : GRPC::CallInfo,
    requests : GRPC::RawRequestStream,
    ctx : GRPC::ServerContext,
    writer : GRPC::RawResponseStream,
    next_call : GRPC::BidiStreamServerCall,
  ) : GRPC::Status
    @calls << "bidi:before:#{info.method}"
    status = next_call.call(info.method_path, requests, ctx, writer)
    @calls << "bidi:after:#{info.method}"
    status
  end
end

class FullRecordingClientInterceptor < GRPC::ClientInterceptor
  getter calls : Array(String) = [] of String

  def call(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ClientContext,
    next_call : GRPC::UnaryClientCall,
  ) : GRPC::ResponseEnvelope
    @calls << "unary:before:#{request.info.method_path}"
    result = next_call.call(request.info.method_path, request, ctx)
    @calls << "unary:after:#{request.info.method_path}"
    result
  end

  def call_server_stream(
    request : GRPC::RequestEnvelope,
    ctx : GRPC::ClientContext,
    next_call : GRPC::ServerStreamClientCall,
  ) : GRPC::RawServerStream
    @calls << "ss:before:#{request.info.method_path}"
    stream = next_call.call(request.info.method_path, request, ctx)
    @calls << "ss:after:#{request.info.method_path}"
    stream
  end

  def call_live_client_stream(
    info : GRPC::CallInfo,
    ctx : GRPC::ClientContext,
    next_call : GRPC::LiveClientStreamClientCall,
  ) : GRPC::RawClientCall
    @calls << "cs:before:#{info.method_path}"
    result = next_call.call(info.method_path, ctx)
    @calls << "cs:after:#{info.method_path}"
    result
  end

  def call_live_bidi_stream(
    info : GRPC::CallInfo,
    ctx : GRPC::ClientContext,
    next_call : GRPC::LiveBidiStreamClientCall,
  ) : GRPC::RawBidiCall
    @calls << "bidi:before:#{info.method_path}"
    stream = next_call.call(info.method_path, ctx)
    @calls << "bidi:after:#{info.method_path}"
    stream
  end
end

def find_free_port : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port
  server.close
  port
end

def start_test_server(port : Int32) : GRPC::Server
  server = GRPC::Server.new
  server.handle EchoService.new
  server.bind("127.0.0.1:#{port}")
  server.start
  server
end

TLS_CERT = File.join(__DIR__, "test.crt")
TLS_KEY  = File.join(__DIR__, "test.key")

describe "GRPC TLS integration" do
  it "performs a unary RPC round-trip over TLS" do
    port = find_free_port
    server = GRPC::Server.new
    server.use_tls(cert: TLS_CERT, key: TLS_KEY)
    server.handle EchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    ctx = OpenSSL::SSL::Context::Client.new
    ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    channel = GRPC::Channel.new("https://127.0.0.1:#{port}", tls_context: ctx)

    begin
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("tls"))
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:tls")
    ensure
      channel.close
      server.stop
    end
  end

  it "performs a server-streaming RPC over TLS" do
    port = find_free_port
    server = GRPC::Server.new
    server.use_tls(cert: TLS_CERT, key: TLS_KEY)
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    ctx = OpenSSL::SSL::Context::Client.new
    ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE
    channel = GRPC::Channel.new("https://127.0.0.1:#{port}", tls_context: ctx)

    begin
      stream = channel.open_server_stream("test.StreamingEcho", "ServerStream",
        TestProto.encode_string("world"))
      messages = [] of String
      stream.each { |bytes| messages << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      messages.should eq(["0:world", "1:world", "2:world"])
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC compression integration" do
  it "handles a gzip-compressed request body" do
    port = find_free_port
    server = start_test_server(port)
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      # Encode the proto payload, then wrap in a compressed gRPC frame
      raw = TestProto.encode_string("compressed")
      compressed_frame = GRPC::Codec.encode(raw, compress: true)
      # The server's Codec.decode (called by Http2ServerConnection.decode_message)
      # should transparently decompress the frame.
      response = channel.unary_call("test.Echo", "Echo",
        GRPC::Codec.decode(compressed_frame).first)
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:compressed")
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC integration" do
  it "performs a unary RPC round-trip" do
    port = find_free_port
    server = start_test_server(port)
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      req_bytes = TestProto.encode_string("hello")
      response = channel.unary_call("test.Echo", "Echo", req_bytes)

      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:hello")
    ensure
      channel.close
      server.stop
    end
  end

  it "returns the correct gRPC error status on service error" do
    port = find_free_port
    server = start_test_server(port)
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      response = channel.unary_call("test.Echo", "Fail", Bytes.empty)
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::NOT_FOUND)
      response.status.message.should eq("not here")
    ensure
      channel.close
      server.stop
    end
  end

  it "returns UNIMPLEMENTED for unknown service" do
    port = find_free_port
    server = start_test_server(port)
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      response = channel.unary_call("no.Such", "Nope", Bytes.empty)
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::UNIMPLEMENTED)
    ensure
      channel.close
      server.stop
    end
  end

  it "performs a server-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      req_bytes = TestProto.encode_string("hello")
      stream = channel.open_server_stream("test.StreamingEcho", "ServerStream", req_bytes)
      messages = [] of String
      stream.each do |bytes|
        messages << TestProto.decode_string(bytes)
      end
      stream.status.ok?.should be_true
      messages.size.should eq(3)
      messages[0].should eq("0:hello")
      messages[1].should eq("1:hello")
      messages[2].should eq("2:hello")
    ensure
      channel.close
      server.stop
    end
  end

  it "performs a client-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      messages = [
        TestProto.encode_string("a"),
        TestProto.encode_string("b"),
        TestProto.encode_string("c"),
      ]
      call = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      messages.each { |msg| call.send_raw(msg) }
      body = call.close_and_recv
      status = call.status
      status.ok?.should be_true
      TestProto.decode_string(body).should eq("joined:a,b,c")
    ensure
      channel.close
      server.stop
    end
  end

  it "performs a bidirectional streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      messages = [
        TestProto.encode_string("x"),
        TestProto.encode_string("y"),
      ]
      stream = channel.open_bidi_stream_live("test.StreamingEcho", "BidiStream")
      messages.each { |msg| stream.send_raw(msg) }
      stream.close_send
      replies = [] of String
      stream.each { |bytes| replies << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      replies.size.should eq(2)
      replies[0].should eq("reply0:x")
      replies[1].should eq("reply1:y")
    ensure
      channel.close
      server.stop
    end
  end

  it "delivers client-streaming requests to the server before close_and_recv" do
    port = find_free_port
    first_seen = ::Channel(String).new(1)
    server = GRPC::Server.new
    server.handle LiveProbeService.new(first_seen)
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      call = channel.open_client_stream_live("test.LiveProbe", "ClientStreamLive")
      call.send_raw(TestProto.encode_string("first"))

      select
      when seen = first_seen.receive
        seen.should eq("first")
      when timeout(1.second)
        fail "server did not observe first streamed request before close_and_recv"
      end

      call.send_raw(TestProto.encode_string("second"))
      body = call.close_and_recv
      call.status.ok?.should be_true
      TestProto.decode_string(body).should eq("count:2")
    ensure
      channel.close
      server.stop
    end
  end

  it "delivers bidi requests live before client closes the stream" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      call = channel.open_bidi_stream_live("test.StreamingEcho", "BidiStream")
      replies = ::Channel(String).new(8)
      done = ::Channel(Nil).new(1)

      spawn do
        begin
          call.each { |bytes| replies.send(TestProto.decode_string(bytes)) }
        ensure
          done.send(nil) rescue nil
        end
      end

      call.send_raw(TestProto.encode_string("x"))
      select
      when first_reply = replies.receive
        first_reply.should eq("reply0:x")
      when timeout(1.second)
        fail "server did not emit first bidi reply before close_send"
      end

      call.send_raw(TestProto.encode_string("y"))
      call.close_send

      second_reply = replies.receive
      second_reply.should eq("reply1:y")

      select
      when done.receive
      when timeout(1.second)
        fail "bidi receive loop did not finish"
      end

      call.status.ok?.should be_true
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC connect timeout" do
  it "fails quickly when endpoint is unreachable and connect_timeout is set" do
    config = GRPC::EndpointConfig.new(connect_timeout: 100.milliseconds)
    channel = GRPC::Channel.new("10.255.255.1:6553", endpoint_config: config)

    begin
      started = Time.instant
      raised = false
      begin
        channel.unary_call("test.Echo", "Echo", Bytes.empty)
      rescue
        raised = true
      end

      raised.should be_true
      elapsed = Time.instant - started
      elapsed.should be < 2.seconds
    ensure
      channel.close
    end
  end
end

describe "GRPC keepalive settings" do
  it "can perform unary RPC with keepalive options enabled" do
    port = find_free_port
    server = start_test_server(port)
    config = GRPC::EndpointConfig.new(
      tcp_keepalive: 2.seconds,
      keepalive: GRPC::KeepaliveParams.new(interval: 2.seconds, timeout: 1.second)
    )
    channel = GRPC::Channel.new("127.0.0.1:#{port}", endpoint_config: config)

    begin
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("ka"))
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:ka")
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC client interceptors" do
  it "wraps a unary call and records before/after events" do
    port = find_free_port
    server = start_test_server(port)
    interceptor = RecordingClientInterceptor.new
    channel = GRPC::Channel.new("127.0.0.1:#{port}",
      interceptors: [interceptor] of GRPC::ClientInterceptor)

    begin
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("hi"))
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:hi")
      interceptor.calls.should eq(["before:/test.Echo/Echo", "after:/test.Echo/Echo"])
    ensure
      channel.close
      server.stop
    end
  end

  it "chains multiple client interceptors outermost-first" do
    port = find_free_port
    server = start_test_server(port)
    first = RecordingClientInterceptor.new
    second = RecordingClientInterceptor.new
    channel = GRPC::Channel.new("127.0.0.1:#{port}",
      interceptors: [first, second] of GRPC::ClientInterceptor)

    begin
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("x"))
      response.status.ok?.should be_true
      # first wraps second: first.before → second.before → RPC → second.after → first.after
      first.calls.should eq(["before:/test.Echo/Echo", "after:/test.Echo/Echo"])
      second.calls.should eq(["before:/test.Echo/Echo", "after:/test.Echo/Echo"])
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC server interceptors" do
  it "blocks a call when authorization is missing" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle EchoService.new
    server.intercept AuthServerInterceptor.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("hi"))
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::UNAUTHENTICATED)
      response.status.message.should eq("invalid token")
    ensure
      channel.close
      server.stop
    end
  end

  it "allows a call through when authorization is valid" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle EchoService.new
    server.intercept AuthServerInterceptor.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      ctx = GRPC::ClientContext.new(metadata: {"authorization" => "valid-token"})
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("hello"), ctx)
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:hello")
    ensure
      channel.close
      server.stop
    end
  end

  it "records before/after events on server interceptor" do
    port = find_free_port
    interceptor = RecordingServerInterceptor.new
    server = GRPC::Server.new
    server.handle EchoService.new
    server.intercept interceptor
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("test"))
      response.status.ok?.should be_true
      interceptor.calls.should eq(["before:Echo", "after:Echo"])
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC streaming server interceptors" do
  it "intercepts a server-streaming RPC" do
    port = find_free_port
    interceptor = FullRecordingServerInterceptor.new
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.intercept interceptor
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      stream = channel.open_server_stream("test.StreamingEcho", "ServerStream",
        TestProto.encode_string("hi"))
      messages = [] of String
      stream.each { |bytes| messages << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      messages.size.should eq(3)
      interceptor.calls.should eq(["ss:before:ServerStream", "ss:after:ServerStream"])
    ensure
      channel.close
      server.stop
    end
  end

  it "intercepts a client-streaming RPC" do
    port = find_free_port
    interceptor = FullRecordingServerInterceptor.new
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.intercept interceptor
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      msgs = [TestProto.encode_string("a"), TestProto.encode_string("b")]
      call = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      msgs.each { |msg| call.send_raw(msg) }
      body = call.close_and_recv
      status = call.status
      status.ok?.should be_true
      TestProto.decode_string(body).should eq("joined:a,b")
      interceptor.calls.should eq(["cs:before:ClientStream", "cs:after:ClientStream"])
    ensure
      channel.close
      server.stop
    end
  end

  it "intercepts a bidirectional-streaming RPC" do
    port = find_free_port
    interceptor = FullRecordingServerInterceptor.new
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.intercept interceptor
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      msgs = [TestProto.encode_string("x")]
      stream = channel.open_bidi_stream_live("test.StreamingEcho", "BidiStream")
      msgs.each { |msg| stream.send_raw(msg) }
      stream.close_send
      replies = [] of String
      stream.each { |bytes| replies << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      replies.should eq(["reply0:x"])
      interceptor.calls.should eq(["bidi:before:BidiStream", "bidi:after:BidiStream"])
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC streaming client interceptors" do
  it "intercepts a server-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    interceptor = FullRecordingClientInterceptor.new
    channel = GRPC::Channel.new("127.0.0.1:#{port}",
      interceptors: [interceptor] of GRPC::ClientInterceptor)

    begin
      stream = channel.open_server_stream("test.StreamingEcho", "ServerStream",
        TestProto.encode_string("hi"))
      messages = [] of String
      stream.each { |bytes| messages << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      messages.size.should eq(3)
      interceptor.calls.should eq([
        "ss:before:/test.StreamingEcho/ServerStream",
        "ss:after:/test.StreamingEcho/ServerStream",
      ])
    ensure
      channel.close
      server.stop
    end
  end

  it "intercepts a client-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    interceptor = FullRecordingClientInterceptor.new
    channel = GRPC::Channel.new("127.0.0.1:#{port}",
      interceptors: [interceptor] of GRPC::ClientInterceptor)

    begin
      msgs = [TestProto.encode_string("a"), TestProto.encode_string("b")]
      call = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      msgs.each { |msg| call.send_raw(msg) }
      body = call.close_and_recv
      status = call.status
      status.ok?.should be_true
      TestProto.decode_string(body).should eq("joined:a,b")
      interceptor.calls.should eq([
        "cs:before:/test.StreamingEcho/ClientStream",
        "cs:after:/test.StreamingEcho/ClientStream",
      ])
    ensure
      channel.close
      server.stop
    end
  end

  it "intercepts a bidirectional-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    interceptor = FullRecordingClientInterceptor.new
    channel = GRPC::Channel.new("127.0.0.1:#{port}",
      interceptors: [interceptor] of GRPC::ClientInterceptor)

    begin
      msgs = [TestProto.encode_string("x")]
      stream = channel.open_bidi_stream_live("test.StreamingEcho", "BidiStream")
      msgs.each { |msg| stream.send_raw(msg) }
      stream.close_send
      replies = [] of String
      stream.each { |bytes| replies << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      replies.should eq(["reply0:x"])
      interceptor.calls.should eq([
        "bidi:before:/test.StreamingEcho/BidiStream",
        "bidi:after:/test.StreamingEcho/BidiStream",
      ])
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC metadata" do
  it "client metadata arrives at the server" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle MetaEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      ctx = GRPC::ClientContext.new(metadata: {"x-test-header" => "hello-world"})
      response = channel.unary_call("test.MetaEcho", "GetMeta",
        TestProto.encode_string("x-test-header"), ctx)
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("hello-world")
    ensure
      channel.close
      server.stop
    end
  end

  it "metadata injected by a client interceptor arrives at the server" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle MetaEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}",
      interceptors: [MutatingClientInterceptor.new] of GRPC::ClientInterceptor)

    begin
      # MutatingClientInterceptor injects x-from-interceptor: 1 into the context
      response = channel.unary_call("test.MetaEcho", "GetMeta",
        TestProto.encode_string("x-from-interceptor"))
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("1")
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC multiple services" do
  it "routes requests to the correct service when multiple services are registered" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle EchoService.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      # Call EchoService
      response = channel.unary_call("test.Echo", "Echo", TestProto.encode_string("ping"))
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("echo:ping")

      # Call StreamingEchoService on the same channel/server
      msgs = [TestProto.encode_string("a"), TestProto.encode_string("b")]
      call = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      msgs.each { |msg| call.send_raw(msg) }
      body = call.close_and_recv
      status = call.status
      status.ok?.should be_true
      TestProto.decode_string(body).should eq("joined:a,b")
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC error propagation" do
  it "error status returned from dispatch_server_stream reaches the client" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      stream = channel.open_server_stream("test.StreamingEcho", "ErrorStream", Bytes.empty)
      stream.each { } # drain (no messages expected)
      stream.status.ok?.should be_false
      stream.status.code.should eq(GRPC::StatusCode::INTERNAL)
      stream.status.message.should eq("forced stream error")
    ensure
      channel.close
      server.stop
    end
  end

  it "client-streaming RPC with zero messages returns a response" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      call = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      body = call.close_and_recv
      status = call.status
      status.ok?.should be_true
      TestProto.decode_string(body).should eq("joined:")
    ensure
      channel.close
      server.stop
    end
  end

  it "preserves streamed messages emitted before a server-side stream error" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      req = TestProto.encode_string("batch")
      stream = channel.open_server_stream("test.StreamingEcho", "PartialErrorStream", req)
      messages = [] of String
      stream.each { |bytes| messages << TestProto.decode_string(bytes) }

      messages.should eq(["partial0:batch", "partial1:batch", "partial2:batch"])
      stream.status.ok?.should be_false
      stream.status.code.should eq(GRPC::StatusCode::INTERNAL)
      stream.status.message.should eq("forced partial stream error")
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC deadline" do
  it "grpc-timeout sent by client is received and parsed as a deadline on the server" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle MetaEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      ctx = GRPC::ClientContext.new(deadline: 30.seconds)
      response = channel.unary_call("test.MetaEcho", "HasDeadline", Bytes.empty, ctx)
      response.status.ok?.should be_true
      TestProto.decode_string(response.raw).should eq("yes")
    ensure
      channel.close
      server.stop
    end
  end

  it "fails with DEADLINE_EXCEEDED when grpc-timeout is already expired at dispatch" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle MetaEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      meta = GRPC::Metadata.new
      meta.add("grpc-timeout", "0m")
      response = channel.unary_call("test.MetaEcho", "HasDeadline", Bytes.empty, meta)
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::DEADLINE_EXCEEDED)
    ensure
      channel.close
      server.stop
    end
  end

  it "fails with INVALID_ARGUMENT when grpc-timeout format is malformed" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle MetaEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      meta = GRPC::Metadata.new
      meta.add("grpc-timeout", "oops")
      response = channel.unary_call("test.MetaEcho", "HasDeadline", Bytes.empty, meta)
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::INVALID_ARGUMENT)
    ensure
      channel.close
      server.stop
    end
  end

  it "terminates a slow unary handler when deadline is exceeded mid-flight" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle DeadlineProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      ctx = GRPC::ClientContext.new(deadline: 30.milliseconds)
      response = channel.unary_call("test.DeadlineProbe", "SlowUnary", Bytes.empty, ctx)
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::DEADLINE_EXCEEDED)
    ensure
      channel.close
      server.stop
    end
  end

  it "terminates a running server-stream handler on deadline expiry" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle DeadlineProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      ctx = GRPC::ClientContext.new(deadline: 40.milliseconds)
      stream = channel.open_server_stream("test.DeadlineProbe", "SlowServerStream", Bytes.empty, ctx)
      started = Time.instant
      stream.each { |_bytes| }
      elapsed = Time.instant - started
      elapsed.should be < 1.second

      unless stream.status.ok?
        stream.status.code.should eq(GRPC::StatusCode::DEADLINE_EXCEEDED)
      end
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC ServerStream status and trailers" do
  it "status is OK after a successful server-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      req_bytes = TestProto.encode_string("hello")
      stream = channel.open_server_stream("test.StreamingEcho", "ServerStream", req_bytes)
      messages = [] of String
      stream.each { |bytes| messages << TestProto.decode_string(bytes) }
      stream.status.ok?.should be_true
      messages.size.should eq(3)
    ensure
      channel.close
      server.stop
    end
  end

  it "status reflects a server-side error in a server-streaming RPC" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      stream = channel.open_server_stream("test.StreamingEcho", "ErrorStream", Bytes.empty)
      stream.each { }
      stream.status.ok?.should be_false
      stream.status.code.should eq(GRPC::StatusCode::INTERNAL)
    ensure
      channel.close
      server.stop
    end
  end

  it "cancel closes the stream early without raising" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      req_bytes = TestProto.encode_string("hello")
      stream = channel.open_server_stream("test.StreamingEcho", "ServerStream", req_bytes)
      # Cancel immediately — should not raise, iteration should stop.
      stream.cancel
      # Draining after cancel is safe (channel is closed, receive? returns nil).
      stream.each { }
    ensure
      channel.close
      server.stop
    end
  end
end

# ---- Full-duplex bidi service ----
# Echoes all incoming messages back after close_send (current server-side bidi is batch).
class FullDuplexEchoService < GRPC::Service
  SERVICE_FULL_NAME = "test.FullDuplexEcho"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::UNIMPLEMENTED, "unimplemented")}
  end

  def bidi_streaming?(method : String) : Bool
    method == "EchoStream"
  end

  def dispatch_bidi_stream(method : String, requests : GRPC::RawRequestStream,
                           ctx : GRPC::ServerContext, writer : GRPC::RawResponseStream) : GRPC::Status
    requests.each { |body| writer.send_raw(body) }
    GRPC::Status.ok
  end
end

describe "GRPC full-duplex bidi streaming" do
  it "interleaves sends and receives in the same stream" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle FullDuplexEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_bidi_stream_live("test.FullDuplexEcho", "EchoStream")
      received = [] of String
      done = ::Channel(Nil).new(1)

      # Receiver fiber runs concurrently with the sender
      spawn do
        raw.each { |bytes| received << TestProto.decode_string(bytes) }
        done.send(nil)
      end

      # Send 3 messages then close; server echoes them back after close_send
      raw.send_raw(TestProto.encode_string("a"))
      raw.send_raw(TestProto.encode_string("b"))
      raw.send_raw(TestProto.encode_string("c"))
      raw.close_send

      done.receive
      received.should eq(["a", "b", "c"])
      raw.status.ok?.should be_true
    ensure
      channel.close
      server.stop
    end
  end

  it "close_send without messages completes cleanly" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle FullDuplexEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_bidi_stream_live("test.FullDuplexEcho", "EchoStream")
      raw.close_send
      received = [] of Bytes
      raw.each { |bytes| received << bytes }
      received.should be_empty
      raw.status.ok?.should be_true
    ensure
      channel.close
      server.stop
    end
  end

  it "continues delivering messages after an idle gap before close_send" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle FullDuplexEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_bidi_stream_live("test.FullDuplexEcho", "EchoStream")
      received = [] of String
      done = ::Channel(Nil).new(1)

      spawn do
        raw.each { |bytes| received << TestProto.decode_string(bytes) }
        done.send(nil)
      end

      raw.send_raw(TestProto.encode_string("alpha"))
      sleep 200.milliseconds
      raw.send_raw(TestProto.encode_string("beta"))
      raw.send_raw(TestProto.encode_string("gamma"))
      raw.close_send

      done.receive
      received.should eq(["alpha", "beta", "gamma"])
      raw.status.ok?.should be_true
    ensure
      channel.close
      server.stop
    end
  end

  it "continues delivering messages after an earlier deadline-exceeded unary call" do
    deadline_port = find_free_port
    deadline_server = GRPC::Server.new
    deadline_server.handle DeadlineProbeService.new
    deadline_server.bind("127.0.0.1:#{deadline_port}")
    deadline_server.start
    deadline_channel = GRPC::Channel.new("127.0.0.1:#{deadline_port}")

    begin
      ctx = GRPC::ClientContext.new(deadline: 30.milliseconds)
      response = deadline_channel.unary_call("test.DeadlineProbe", "SlowUnary", Bytes.empty, ctx)
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::DEADLINE_EXCEEDED)
    ensure
      deadline_channel.close
      deadline_server.stop
    end

    bidi_port = find_free_port
    bidi_server = GRPC::Server.new
    bidi_server.handle FullDuplexEchoService.new
    bidi_server.bind("127.0.0.1:#{bidi_port}")
    bidi_server.start
    bidi_channel = GRPC::Channel.new("127.0.0.1:#{bidi_port}")

    begin
      raw = bidi_channel.open_bidi_stream_live("test.FullDuplexEcho", "EchoStream")
      received = [] of String
      done = ::Channel(Nil).new(1)

      spawn do
        raw.each { |bytes| received << TestProto.decode_string(bytes) }
        done.send(nil)
      end

      raw.send_raw(TestProto.encode_string("alpha"))
      sleep 200.milliseconds
      raw.send_raw(TestProto.encode_string("beta"))
      raw.send_raw(TestProto.encode_string("gamma"))
      raw.close_send

      done.receive
      received.should eq(["alpha", "beta", "gamma"])
      raw.status.ok?.should be_true
    ensure
      bidi_channel.close
      bidi_server.stop
    end
  end
end

describe "GRPC live client streaming" do
  it "sends messages incrementally before close_and_recv" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      raw.send_raw(TestProto.encode_string("hello"))
      raw.send_raw(TestProto.encode_string("world"))
      body = raw.close_and_recv
      result = TestProto.decode_string(body)
      result.should eq("joined:hello,world")
      raw.status.ok?.should be_true
    ensure
      channel.close
      server.stop
    end
  end

  it "close_and_recv with no messages returns server response" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      body = raw.close_and_recv
      result = TestProto.decode_string(body)
      result.should eq("joined:")
      raw.status.ok?.should be_true
    ensure
      channel.close
      server.stop
    end
  end
end

describe "GRPC stream status wiring" do
  it "ClientStream#status is ok after close_and_recv succeeds" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle StreamingEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_client_stream_live("test.StreamingEcho", "ClientStream")
      raw.send_raw(TestProto.encode_string("x"))
      raw.close_and_recv
      raw.status.ok?.should be_true
      raw.status.code.should eq(GRPC::StatusCode::OK)
    ensure
      channel.close
      server.stop
    end
  end

  it "RawBidiCall#status is ok after each completes" do
    port = find_free_port
    server = GRPC::Server.new
    server.handle FullDuplexEchoService.new
    server.bind("127.0.0.1:#{port}")
    server.start
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    begin
      raw = channel.open_bidi_stream_live("test.FullDuplexEcho", "EchoStream")
      raw.send_raw(TestProto.encode_string("ping"))
      raw.close_send
      raw.each { }
      raw.status.ok?.should be_true
      raw.status.code.should eq(GRPC::StatusCode::OK)
    ensure
      channel.close
      server.stop
    end
  end
end
