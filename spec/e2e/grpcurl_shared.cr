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

class E2EProbeObservation
  getter method : String
  getter input : String
  getter token : String?
  getter? deadline_set : Bool
  getter? timed_out : Bool
  getter? cancelled : Bool

  def initialize(
    @method : String,
    @input : String,
    @token : String?,
    @deadline_set : Bool,
    @timed_out : Bool,
    @cancelled : Bool,
  )
  end
end

class E2EProbeObserver
  @mutex = Mutex.new
  @last : E2EProbeObservation?

  def record(method : String, input : String, ctx : GRPC::ServerContext) : Nil
    observation = E2EProbeObservation.new(
      method,
      input,
      ctx.metadata["x-e2e-token"]?,
      !ctx.deadline.nil?,
      ctx.timed_out?,
      ctx.cancelled?,
    )
    @mutex.synchronize { @last = observation }
  end

  def last : E2EProbeObservation?
    @mutex.synchronize { @last }
  end

  def reset : Nil
    @mutex.synchronize { @last = nil }
  end
end

class E2EProbeService < GRPC::Service
  SERVICE_FULL_NAME = "e2e.Probe"

  def initialize(@observer : E2EProbeObserver? = nil)
  end

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
      observe(method, input, ctx)
      output = E2EProto.encode_string("echo:#{input};token:#{token}")
      {output, GRPC::Status.ok}
    when "UnaryFail"
      observe(method, input, ctx)
      {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::NOT_FOUND, "missing:#{input}")}
    when "SlowUnary"
      sleep 300.milliseconds
      observe(method, input, ctx)
      {E2EProto.encode_string("slow:#{input}"), GRPC::Status.ok}
    else
      observe(method, input, ctx)
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
      observe(method, input, ctx)
      3.times do |i|
        writer.send_raw(E2EProto.encode_string("stream:#{i}:#{input}"))
      end
      GRPC::Status.ok
    else
      observe(method, "", ctx)
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
      observe(method, parts.join(","), ctx)
      body = E2EProto.encode_string("joined:#{parts.join(",")}")
      {body, GRPC::Status.ok}
    else
      observe(method, "", ctx)
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
      parts = [] of String
      requests.each do |body|
        input = E2EProto.decode_string(body)
        parts << input
        writer.send_raw(E2EProto.encode_string("bidi:#{idx}:#{input}"))
        idx += 1
      end
      observe(method, parts.join(","), ctx)
      GRPC::Status.ok
    else
      observe(method, "", ctx)
      GRPC::Status.unimplemented("unknown method")
    end
  end

  private def observe(method : String, input : String, ctx : GRPC::ServerContext) : Nil
    @observer.try &.record(method, input, ctx)
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

def enable_e2e_reflection(server : GRPC::Server) : Nil
  server.enable_reflection.add_file_descriptor(E2EReflectionWire.build_e2e_file_descriptor_proto)
end

def wait_for_probe_observation(
  observer : E2EProbeObserver,
  timeout : Time::Span = 1.second,
) : E2EProbeObservation?
  deadline = Time.instant + timeout
  loop do
    if observed = observer.last
      return observed
    end
    return nil if Time.instant >= deadline
    sleep 10.milliseconds
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

def with_default_grpcurl_timeout(
  flags : Array(String),
  timeout_seconds : String = "5",
) : Array(String)
  return flags if flags.each_cons_pair.any? { |left, _right| left == "-max-time" }

  args = flags.dup
  args.unshift(timeout_seconds)
  args.unshift("-max-time")
  args
end

def grpcurl_call_args(
  port : Int32,
  method : String,
  flags : Array(String) = [] of String,
  include_proto : Bool = true,
) : Array(String)
  args = ["-plaintext"]
  if include_proto
    args << "-import-path"
    args << File.expand_path("../fixtures/grpcurl", __DIR__)
    args << "-proto"
    args << "e2e.proto"
  end
  args.concat(with_default_grpcurl_timeout(flags))
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
  args.concat(with_default_grpcurl_timeout(flags))
  args << "127.0.0.1:#{port}"
  args << command
  if value = subject
    args << value
  end
  args
end
