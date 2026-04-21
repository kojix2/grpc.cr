require "socket"
require "./proto_helpers"

MISSING_GRPCURL_MSG = "grpcurl is not installed; skipping e2e tests"

def grpcurl_available? : Bool
  status = Process.run(
    "grpcurl",
    ["-version"],
    output: Process::Redirect::Close,
    error: Process::Redirect::Close,
  )
  status.success?
rescue
  false
end

class E2EProbeService < GRPC::Service
  SERVICE_FULL_NAME = "e2e.Probe"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(
    method : String,
    request_body : Bytes,
    ctx : GRPC::ServerContext,
  ) : {Bytes, GRPC::Status}
    input = E2EProto.decode_string(request_body)

    case method
    when "UnaryEcho"
      token = ctx.metadata["x-e2e-token"]? || "none"
      output = E2EProto.encode_string("echo:#{input};token:#{token}")
      {output, GRPC::Status.ok}
    when "UnaryFail"
      {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::NOT_FOUND, "missing:#{input}")}
    when "SlowUnary"
      sleep 300.milliseconds
      {E2EProto.encode_string("slow:#{input}"), GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end

  def server_streaming?(method : String) : Bool
    method == "ServerStream"
  end

  def dispatch_server_stream(
    method : String,
    request_body : Bytes,
    ctx : GRPC::ServerContext,
    writer : GRPC::RawResponseStream,
  ) : GRPC::Status
    case method
    when "ServerStream"
      input = E2EProto.decode_string(request_body)
      3.times do |i|
        writer.send_raw(E2EProto.encode_string("stream:#{i}:#{input}"))
      end
      GRPC::Status.ok
    else
      GRPC::Status.unimplemented("unknown method")
    end
  end

  def client_streaming?(method : String) : Bool
    method == "ClientStream"
  end

  def dispatch_client_stream(
    method : String,
    requests : GRPC::RawRequestStream,
    ctx : GRPC::ServerContext,
  ) : {Bytes, GRPC::Status}
    case method
    when "ClientStream"
      parts = [] of String
      requests.each { |body| parts << E2EProto.decode_string(body) }
      body = E2EProto.encode_string("joined:#{parts.join(",")}")
      {body, GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end

  def bidi_streaming?(method : String) : Bool
    method == "BidiStream"
  end

  def dispatch_bidi_stream(
    method : String,
    requests : GRPC::RawRequestStream,
    ctx : GRPC::ServerContext,
    writer : GRPC::RawResponseStream,
  ) : GRPC::Status
    case method
    when "BidiStream"
      idx = 0
      requests.each do |body|
        input = E2EProto.decode_string(body)
        writer.send_raw(E2EProto.encode_string("bidi:#{idx}:#{input}"))
        idx += 1
      end
      GRPC::Status.ok
    else
      GRPC::Status.unimplemented("unknown method")
    end
  end
end

module E2EReflectionWire
  private LABEL_OPTIONAL = 1_u64
  private TYPE_STRING    = 9_u64

  def self.handle_request(request : Bytes) : Bytes
    kind, value = parse_server_reflection_request(request)
    case kind
    when :list_services
      encode_server_reflection_response(request, 6, encode_list_services_response)
    when :file_by_filename
      if value == "e2e.proto"
        encode_server_reflection_response(request, 4, encode_file_descriptor_response)
      else
        encode_server_reflection_response(request, 7, encode_error_response(5, "file not found: #{value}"))
      end
    when :file_containing_symbol
      if e2e_symbol?(value)
        encode_server_reflection_response(request, 4, encode_file_descriptor_response)
      else
        encode_server_reflection_response(request, 7, encode_error_response(5, "symbol not found: #{value}"))
      end
    else
      encode_server_reflection_response(request, 7, encode_error_response(12, "unsupported reflection request"))
    end
  end

  def self.e2e_symbol?(value : String) : Bool
    value.starts_with?("e2e.")
  end

  def self.encode_server_reflection_response(original_request : Bytes, message_field : Int32, message_payload : Bytes) : Bytes
    io = IO::Memory.new
    write_embedded_raw(io, 2, original_request)
    write_embedded_raw(io, message_field, message_payload)
    io.to_slice
  end

  def self.encode_list_services_response : Bytes
    io = IO::Memory.new
    service_names = [
      "e2e.Probe",
      "grpc.reflection.v1alpha.ServerReflection",
    ]
    service_names.each do |service_name|
      svc = IO::Memory.new
      write_string(svc, 1, service_name)
      write_embedded_raw(io, 1, svc.to_slice)
    end
    io.to_slice
  end

  def self.encode_file_descriptor_response : Bytes
    io = IO::Memory.new
    write_bytes(io, 1, build_e2e_file_descriptor_proto)
    io.to_slice
  end

  def self.encode_error_response(code : Int32, message : String) : Bytes
    io = IO::Memory.new
    write_varint_field(io, 1, code.to_u64)
    write_string(io, 2, message)
    io.to_slice
  end

  def self.build_e2e_file_descriptor_proto : Bytes
    io = IO::Memory.new
    write_string(io, 1, "e2e.proto")
    write_string(io, 2, "e2e")

    ["EchoRequest", "EchoReply", "StringMessage"].each do |name|
      write_embedded_raw(io, 4, build_string_message_descriptor(name))
    end

    write_embedded_raw(io, 6, build_probe_service_descriptor)
    io.to_slice
  end

  def self.build_string_message_descriptor(name : String) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_embedded_raw(io, 2, build_string_field_descriptor("message", 1))
    io.to_slice
  end

  def self.build_string_field_descriptor(name : String, number : Int32) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 3, number.to_u64)
    write_varint_field(io, 4, LABEL_OPTIONAL)
    write_varint_field(io, 5, TYPE_STRING)
    io.to_slice
  end

  def self.build_probe_service_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "Probe")

    methods = [
      {"UnaryEcho", false, false},
      {"UnaryFail", false, false},
      {"SlowUnary", false, false},
      {"ServerStream", false, true},
      {"ClientStream", true, false},
      {"BidiStream", true, true},
    ]

    methods.each do |name, client_streaming, server_streaming|
      write_embedded_raw(io, 2, build_method_descriptor(name, client_streaming, server_streaming))
    end

    io.to_slice
  end

  def self.build_method_descriptor(name : String, client_streaming : Bool, server_streaming : Bool) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_string(io, 2, ".e2e.EchoRequest")
    write_string(io, 3, ".e2e.EchoReply")
    write_varint_field(io, 5, 1_u64) if client_streaming
    write_varint_field(io, 6, 1_u64) if server_streaming
    io.to_slice
  end

  def self.parse_server_reflection_request(data : Bytes) : {Symbol, String}
    i = 0
    selected_kind = :unknown
    selected_value = ""

    while i < data.size
      tag, consumed = decode_varint(data, i)
      i += consumed
      wire_type = (tag & 0x7).to_i
      field_number = (tag >> 3).to_i

      case wire_type
      when 0
        _, consumed = decode_varint(data, i)
        i += consumed
      when 2
        len, consumed = decode_varint(data, i)
        i += consumed
        break if i + len.to_i > data.size
        value = String.new(data[i, len.to_i])
        i += len.to_i
        case field_number
        when 3
          selected_kind = :file_by_filename
          selected_value = value
        when 4
          selected_kind = :file_containing_symbol
          selected_value = value
        when 7
          selected_kind = :list_services
          selected_value = value
        end
      else
        break
      end
    end

    {selected_kind, selected_value}
  end

  private def self.write_varint(io : IO, value : UInt64) : Nil
    loop do
      byte = (value & 0x7F).to_u8
      value >>= 7
      io.write_byte(value == 0 ? byte : (byte | 0x80_u8))
      break if value == 0
    end
  end

  private def self.decode_varint(data : Bytes, offset : Int32) : {UInt64, Int32}
    result = 0_u64
    shift = 0
    consumed = 0
    loop do
      break if offset + consumed >= data.size
      byte = data[offset + consumed].to_u64
      consumed += 1
      result |= (byte & 0x7F) << shift
      shift += 7
      break unless (byte & 0x80) != 0
    end
    {result, consumed}
  end

  private def self.write_key(io : IO, field : Int32, wire_type : Int32) : Nil
    write_varint(io, ((field << 3) | wire_type).to_u64)
  end

  private def self.write_varint_field(io : IO, field : Int32, value : UInt64) : Nil
    write_key(io, field, 0)
    write_varint(io, value)
  end

  private def self.write_string(io : IO, field : Int32, value : String) : Nil
    write_key(io, field, 2)
    bytes = value.to_slice
    write_varint(io, bytes.size.to_u64)
    io.write(bytes)
  end

  private def self.write_bytes(io : IO, field : Int32, value : Bytes) : Nil
    write_key(io, field, 2)
    write_varint(io, value.size.to_u64)
    io.write(value)
  end

  private def self.write_embedded_raw(io : IO, field : Int32, value : Bytes) : Nil
    write_bytes(io, field, value)
  end
end

class E2EReflectionService < GRPC::Service
  SERVICE_FULL_NAME = "grpc.reflection.v1alpha.ServerReflection"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(
    method : String,
    request_body : Bytes,
    ctx : GRPC::ServerContext,
  ) : {Bytes, GRPC::Status}
    {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
  end

  def bidi_streaming?(method : String) : Bool
    method == "ServerReflectionInfo"
  end

  def dispatch_bidi_stream(
    method : String,
    requests : GRPC::RawRequestStream,
    ctx : GRPC::ServerContext,
    writer : GRPC::RawResponseStream,
  ) : GRPC::Status
    return GRPC::Status.unimplemented("unknown method") unless method == "ServerReflectionInfo"

    requests.each do |request|
      writer.send_raw(E2EReflectionWire.handle_request(request))
    end
    GRPC::Status.ok
  end
end

def find_free_port : Int32
  server = TCPServer.new("127.0.0.1", 0)
  port = server.local_address.port
  server.close
  port
end

def wait_for_server(port : Int32, timeout : Time::Span = 2.seconds) : Nil
  deadline = Time.instant + timeout
  loop do
    begin
      socket = TCPSocket.new("127.0.0.1", port)
      socket.close
      return
    rescue
      raise "server did not start listening in time" if Time.instant >= deadline
      sleep 50.milliseconds
    end
  end
end

def run_grpcurl(
  args : Array(String),
) : {Process::Status, String, String}
  stdout = IO::Memory.new
  stderr = IO::Memory.new
  status = Process.run("grpcurl", args, output: stdout, error: stderr)
  {status, stdout.to_s, stderr.to_s}
end

def grpcurl_base_flags : Array(String)
  proto_dir = File.expand_path("../fixtures/grpcurl", __DIR__)
  [
    "-plaintext",
    "-import-path", proto_dir,
    "-proto", "e2e.proto",
  ]
end

def grpcurl_call_args(
  port : Int32,
  method : String,
  flags : Array(String) = [] of String,
  include_proto : Bool = true,
) : Array(String)
  args = [] of String
  args.concat(grpcurl_base_flags) if include_proto
  args.concat(flags)
  args << "127.0.0.1:#{port}"
  args << method
  args
end

def grpcurl_reflection_args(
  port : Int32,
  command : String,
  subject : String? = nil,
  flags : Array(String) = [] of String,
) : Array(String)
  args = ["-plaintext"]
  args.concat(flags)
  args << "127.0.0.1:#{port}"
  args << command
  if value = subject
    args << value
  end
  args
end

describe "grpcurl e2e baseline" do
  it "invokes unary success and returns expected payload" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        ["-d", "{\"message\":\"hello\"}"],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      status.success?.should be_true
      err.should eq("")
      out.should contain("echo:hello")
    ensure
      server.stop
    end
  end

  it "passes rpc-header metadata to server logic" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        [
          "-rpc-header", "x-e2e-token: abc123",
          "-d", "{\"message\":\"meta\"}",
        ],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      status.success?.should be_true
      out.should contain("token:abc123")
    ensure
      server.stop
    end
  end

  it "returns grpc status for unary failure" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryFail",
        ["-d", "{\"message\":\"lost\"}"],
      )
      status, _out, err = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?
      status.success?.should be_false
      err.downcase.should contain("error")
      err.should contain("missing:lost")
    ensure
      server.stop
    end
  end

  it "enforces max-time timeout for slow unary" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/SlowUnary",
        [
          "-max-time", "0.1",
          "-d", "{\"message\":\"sleep\"}",
        ],
      )
      status, _out, err = run_grpcurl(args)
      raise "expected timeout but got success" if status.success?
      status.success?.should be_false
      lower = err.downcase
      is_timeout = lower.includes?("deadline") ||
                   lower.includes?("timeout") ||
                   lower.includes?("timed out")
      is_timeout.should be_true
    ensure
      server.stop
    end
  end

  it "receives multiple messages from server-streaming call" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/ServerStream",
        ["-d", "{\"message\":\"stream\"}"],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("stream:0:stream")
      out.should contain("stream:1:stream")
      out.should contain("stream:2:stream")
    ensure
      server.stop
    end
  end

  it "sends multiple messages to client-streaming call" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      payload = %({"message":"a"}\n{"message":"b"}\n{"message":"c"})
      args = grpcurl_call_args(
        port,
        "e2e.Probe/ClientStream",
        ["-d", payload],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("joined:a,b,c")
    ensure
      server.stop
    end
  end

  it "exchanges messages in bidirectional streaming call" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      payload = %({"message":"x"}\n{"message":"y"})
      args = grpcurl_call_args(
        port,
        "e2e.Probe/BidiStream",
        ["-d", payload],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("bidi:0:x")
      out.should contain("bidi:1:y")
    ensure
      server.stop
    end
  end

  it "fails list when reflection is disabled" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "list")
      status, out, err = run_grpcurl(args)
      raise "expected reflection list to fail" if status.success?
      status.success?.should be_false
      detail = "#{out}\n#{err}".downcase
      has_expected_hint = detail.includes?("reflection") ||
                          detail.includes?("server does not support") ||
                          detail.includes?("failed to list services")
      has_expected_hint.should be_true
    ensure
      server.stop
    end
  end

  it "fails describe when reflection is disabled" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "describe", "e2e.Probe")
      status, out, err = run_grpcurl(args)
      raise "expected reflection describe to fail" if status.success?
      status.success?.should be_false
      detail = "#{out}\n#{err}".downcase
      has_expected_hint = detail.includes?("reflection") ||
                          detail.includes?("server does not support") ||
                          detail.includes?("failed to resolve symbol")
      has_expected_hint.should be_true
    ensure
      server.stop
    end
  end

  it "lists services via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.handle E2EReflectionService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "list")
      status, out, err = run_grpcurl(args)
      raise "grpcurl list failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("e2e.Probe")
    ensure
      server.stop
    end
  end

  it "lists methods for e2e.Probe via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.handle E2EReflectionService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "list", "e2e.Probe")
      status, out, err = run_grpcurl(args)
      raise "grpcurl list service failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("e2e.Probe.UnaryEcho")
      out.should contain("e2e.Probe.ServerStream")
    ensure
      server.stop
    end
  end

  it "describes e2e.Probe via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.handle E2EReflectionService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "describe", "e2e.Probe")
      status, out, err = run_grpcurl(args)
      raise "grpcurl describe failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("service Probe")
      out.should contain("rpc UnaryEcho")
    ensure
      server.stop
    end
  end

  it "describes e2e.StringMessage via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.handle E2EReflectionService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "describe", "e2e.StringMessage")
      status, out, err = run_grpcurl(args)
      raise "grpcurl describe message failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("message StringMessage")
      out.should contain("string message")
    ensure
      server.stop
    end
  end

  it "invokes unary with reflection only" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.handle E2EReflectionService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        ["-d", "{\"message\":\"hello\"}"],
        include_proto: false,
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl reflection invoke failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("echo:hello")
    ensure
      server.stop
    end
  end

  it "exports protoset from reflection and invokes with protoset" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.handle E2EReflectionService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    protoset_path = "/tmp/grpc-e2e-#{Time.utc.to_unix_ms}-#{Random.rand(1_000_000)}.protoset"

    begin
      wait_for_server(port)

      export_args = grpcurl_reflection_args(
        port,
        "describe",
        "e2e.Probe",
        ["-protoset-out", protoset_path],
      )
      export_status, export_out, export_err = run_grpcurl(export_args)
      raise "grpcurl protoset export failed: #{export_err}\nstdout: #{export_out}" unless export_status.success?
      File.exists?(protoset_path).should be_true
      File.size(protoset_path).should be > 0

      invoke_args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        [
          "-protoset", protoset_path,
          "-d", "{\"message\":\"hello\"}",
        ],
        include_proto: false,
      )
      status, out, err = run_grpcurl(invoke_args)
      raise "grpcurl protoset invoke failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("echo:hello")
    ensure
      File.delete(protoset_path) if File.exists?(protoset_path)
      server.stop
    end
  end
end
