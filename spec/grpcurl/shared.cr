require "openssl"
require "socket"
require "../support/proto_helpers"

MISSING_GRPCURL_MSG      = "grpcurl is not installed; skipping e2e tests"
TLS_FIXTURE_DIR          = File.expand_path("../fixtures/tls", __DIR__)
TLS_GENERATE_SCRIPT      = File.join(TLS_FIXTURE_DIR, "generate.sh")
TLS_CA_CERT              = File.join(TLS_FIXTURE_DIR, "ca.crt")
TLS_SERVER_CERT          = File.join(TLS_FIXTURE_DIR, "server.crt")
TLS_SERVER_KEY           = File.join(TLS_FIXTURE_DIR, "server.key")
TLS_CLIENT_CERT          = File.join(TLS_FIXTURE_DIR, "client.crt")
TLS_CLIENT_KEY           = File.join(TLS_FIXTURE_DIR, "client.key")
TLS_FIXTURE_MUTEX        = Mutex.new
HEALTH_PROTO_IMPORT_PATH = File.expand_path("../../vendor/grpc-proto", __DIR__)
HEALTH_PROTO_FILE        = "grpc/health/v1/health.proto"

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

def ensure_e2e_tls_fixtures! : Nil
  return if e2e_tls_fixtures_exist?

  TLS_FIXTURE_MUTEX.synchronize do
    return if e2e_tls_fixtures_exist?
    generate_e2e_tls_fixtures!
  end
end

private def e2e_tls_fixtures_exist? : Bool
  File.exists?(TLS_CA_CERT) &&
    File.exists?(TLS_SERVER_CERT) &&
    File.exists?(TLS_SERVER_KEY) &&
    File.exists?(TLS_CLIENT_CERT) &&
    File.exists?(TLS_CLIENT_KEY)
end

private def generate_e2e_tls_fixtures! : Nil
  stdout = IO::Memory.new
  stderr = IO::Memory.new
  status = Process.run("sh", [TLS_GENERATE_SCRIPT], output: stdout, error: stderr)
  return if status.success?

  stderr_text = stderr.to_s
  output = stderr_text.empty? ? stdout.to_s : stderr_text
  raise "failed to generate TLS fixtures: #{output}"
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
    when "UnaryFailedPrecondition"
      observe(method, input, ctx)
      {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::FAILED_PRECONDITION, "state blocked:#{input}")}
    when "UnaryDataLoss"
      observe(method, input, ctx)
      {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::DATA_LOSS, "corrupted:#{input}")}
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
    method == "ServerStream" || method == "ServerStreamFailAfterTwo"
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
    when "ServerStreamFailAfterTwo"
      input = E2EProto.decode_string(request_body)
      observe(method, input, ctx)
      2.times do |i|
        writer.send_raw(E2EProto.encode_string("stream-fail:#{i}:#{input}"))
      end
      ctx.trailing_metadata.set("x-e2e-trailer", "server-stream-failed")
      ctx.trailing_metadata.set("x-e2e-count", "2")
      GRPC::Status.new(GRPC::StatusCode::INTERNAL, "stream exploded:#{input}")
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
    method == "BidiStream" || method == "BidiStreamFailAfterTwo"
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
    when "BidiStreamFailAfterTwo"
      idx = 0
      parts = [] of String
      requests.each do |body|
        input = E2EProto.decode_string(body)
        parts << input
        writer.send_raw(E2EProto.encode_string("bidi-fail:#{idx}:#{input}"))
        idx += 1
        break if idx >= 2
      end
      observe(method, parts.join(","), ctx)
      ctx.trailing_metadata.set("x-e2e-trailer", "bidi-failed")
      ctx.trailing_metadata.set("x-e2e-count", idx.to_s)
      GRPC::Status.new(GRPC::StatusCode::RESOURCE_EXHAUSTED, "bidi limit reached")
    else
      observe(method, "", ctx)
      GRPC::Status.unimplemented("unknown method")
    end
  end

  private def observe(method : String, input : String, ctx : GRPC::ServerContext) : Nil
    @observer.try &.record(method, input, ctx)
  end
end

module E2EComplexProto
  RFC3339_TIME = "2024-01-02T03:04:05Z"

  def self.encode_reply : Bytes
    io = IO::Memory.new
    write_embedded_field(io, 1, encode_labels_entry("role", "admin"))
    write_string_field(io, 2, "picked-name")
    write_embedded_field(io, 4, encode_nested)
    write_embedded_field(io, 5, encode_imported)
    write_embedded_field(io, 6, encode_timestamp)
    io.to_slice
  end

  private def self.encode_labels_entry(key : String, value : String) : Bytes
    io = IO::Memory.new
    write_string_field(io, 1, key)
    write_string_field(io, 2, value)
    io.to_slice
  end

  private def self.encode_nested : Bytes
    io = IO::Memory.new
    write_varint_field(io, 1, 1_u64)
    io.to_slice
  end

  private def self.encode_imported : Bytes
    io = IO::Memory.new
    write_string_field(io, 1, "from-import")
    write_varint_field(io, 2, 7_u64)
    io.to_slice
  end

  private def self.encode_timestamp : Bytes
    io = IO::Memory.new
    write_varint_field(io, 1, Time.parse_rfc3339(RFC3339_TIME).to_unix.to_u64)
    write_varint_field(io, 2, 0_u64)
    io.to_slice
  end

  private def self.write_key(io : IO, field : Int32, wire_type : Int32) : Nil
    write_varint(io, ((field << 3) | wire_type).to_u64)
  end

  private def self.write_varint_field(io : IO, field : Int32, value : UInt64) : Nil
    write_key(io, field, 0)
    write_varint(io, value)
  end

  private def self.write_string_field(io : IO, field : Int32, value : String) : Nil
    write_key(io, field, 2)
    bytes = value.to_slice
    write_varint(io, bytes.size.to_u64)
    io.write(bytes)
  end

  private def self.write_embedded_field(io : IO, field : Int32, value : Bytes) : Nil
    write_key(io, field, 2)
    write_varint(io, value.size.to_u64)
    io.write(value)
  end

  private def self.write_varint(io : IO, value : UInt64) : Nil
    loop do
      byte = (value & 0x7F).to_u8
      value >>= 7
      io.write_byte(value == 0 ? byte : (byte | 0x80_u8))
      break if value == 0
    end
  end
end

class E2EComplexService < GRPC::Service
  SERVICE_FULL_NAME = "e2e.ComplexProbe"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(
    method : String,
    request_body : Bytes,
    ctx : GRPC::ServerContext,
  ) : {Bytes, GRPC::Status}
    _ = request_body
    _ = ctx

    case method
    when "GetComplex"
      {E2EComplexProto.encode_reply, GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
    end
  end
end

module E2EReflectionWire
  private LABEL_OPTIONAL =  1_u64
  private LABEL_REPEATED =  3_u64
  private TYPE_STRING    =  9_u64
  private TYPE_MESSAGE   = 11_u64
  private TYPE_INT32     =  5_u64
  private TYPE_INT64     =  3_u64
  private TYPE_ENUM      = 14_u64

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
    write_string(io, 12, "proto3")

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
      {"UnaryFailedPrecondition", false, false},
      {"UnaryDataLoss", false, false},
      {"SlowUnary", false, false},
      {"ServerStream", false, true},
      {"ServerStreamFailAfterTwo", false, true},
      {"ClientStream", true, false},
      {"BidiStream", true, true},
      {"BidiStreamFailAfterTwo", true, true},
    ]

    methods.each do |name, client_streaming, server_streaming|
      write_embedded_raw(io, 2, build_method_descriptor(name, client_streaming, server_streaming))
    end

    io.to_slice
  end

  def self.build_common_file_descriptor_proto : Bytes
    io = IO::Memory.new
    write_string(io, 1, "common.proto")
    write_string(io, 2, "e2e")
    write_string(io, 12, "proto3")
    write_embedded_raw(io, 4, build_imported_message_descriptor)
    io.to_slice
  end

  def self.build_google_empty_file_descriptor_proto : Bytes
    io = IO::Memory.new
    write_string(io, 1, "google/protobuf/empty.proto")
    write_string(io, 2, "google.protobuf")
    write_string(io, 12, "proto3")

    msg = IO::Memory.new
    write_string(msg, 1, "Empty")
    write_embedded_raw(io, 4, msg.to_slice)
    io.to_slice
  end

  def self.build_google_timestamp_file_descriptor_proto : Bytes
    io = IO::Memory.new
    write_string(io, 1, "google/protobuf/timestamp.proto")
    write_string(io, 2, "google.protobuf")
    write_string(io, 12, "proto3")

    msg = IO::Memory.new
    write_string(msg, 1, "Timestamp")
    write_embedded_raw(msg, 2, build_scalar_field_descriptor("seconds", 1, TYPE_INT64))
    write_embedded_raw(msg, 2, build_scalar_field_descriptor("nanos", 2, TYPE_INT32))
    write_embedded_raw(io, 4, msg.to_slice)
    io.to_slice
  end

  def self.build_complex_file_descriptor_proto : Bytes
    io = IO::Memory.new
    write_string(io, 1, "complex.proto")
    write_string(io, 2, "e2e")
    write_string(io, 3, "common.proto")
    write_string(io, 3, "google/protobuf/empty.proto")
    write_string(io, 3, "google/protobuf/timestamp.proto")
    write_string(io, 12, "proto3")
    write_embedded_raw(io, 4, build_complex_reply_descriptor)
    write_embedded_raw(io, 6, build_complex_service_descriptor)
    io.to_slice
  end

  def self.build_imported_message_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "ImportedMessage")
    write_embedded_raw(io, 2, build_scalar_field_descriptor("note", 1, TYPE_STRING))
    write_embedded_raw(io, 2, build_scalar_field_descriptor("count", 2, TYPE_INT32))
    io.to_slice
  end

  def self.build_complex_reply_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "ComplexReply")
    write_embedded_raw(io, 3, build_labels_entry_descriptor)
    write_embedded_raw(io, 3, build_nested_descriptor)
    write_embedded_raw(io, 8, build_oneof_descriptor("choice"))
    write_embedded_raw(io, 2, build_repeated_message_field_descriptor("labels", 1, ".e2e.ComplexReply.LabelsEntry"))
    write_embedded_raw(io, 2, build_oneof_field_descriptor("name", 2, TYPE_STRING, 0))
    write_embedded_raw(io, 2, build_oneof_field_descriptor("code", 3, TYPE_INT32, 0))
    write_embedded_raw(io, 2, build_message_field_descriptor("nested", 4, ".e2e.ComplexReply.Nested"))
    write_embedded_raw(io, 2, build_message_field_descriptor("imported", 5, ".e2e.ImportedMessage"))
    write_embedded_raw(io, 2, build_message_field_descriptor("created_at", 6, ".google.protobuf.Timestamp"))
    io.to_slice
  end

  def self.build_labels_entry_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "LabelsEntry")
    write_embedded_raw(io, 2, build_scalar_field_descriptor("key", 1, TYPE_STRING))
    write_embedded_raw(io, 2, build_scalar_field_descriptor("value", 2, TYPE_STRING))
    write_embedded_raw(io, 7, build_map_entry_options)
    io.to_slice
  end

  def self.build_nested_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "Nested")
    write_embedded_raw(io, 4, build_mode_enum_descriptor)
    write_embedded_raw(io, 2, build_enum_field_descriptor("mode", 1, ".e2e.ComplexReply.Nested.Mode"))
    io.to_slice
  end

  def self.build_mode_enum_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "Mode")
    write_embedded_raw(io, 2, build_enum_value_descriptor("MODE_UNSPECIFIED", 0))
    write_embedded_raw(io, 2, build_enum_value_descriptor("MODE_ACTIVE", 1))
    io.to_slice
  end

  def self.build_enum_value_descriptor(name : String, number : Int32) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 2, number.to_u64)
    io.to_slice
  end

  def self.build_complex_service_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "ComplexProbe")
    write_embedded_raw(io, 2, build_complex_method_descriptor)
    io.to_slice
  end

  def self.build_complex_method_descriptor : Bytes
    io = IO::Memory.new
    write_string(io, 1, "GetComplex")
    write_string(io, 2, ".google.protobuf.Empty")
    write_string(io, 3, ".e2e.ComplexReply")
    io.to_slice
  end

  def self.build_scalar_field_descriptor(name : String, number : Int32, type : UInt64) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 3, number.to_u64)
    write_varint_field(io, 4, LABEL_OPTIONAL)
    write_varint_field(io, 5, type)
    io.to_slice
  end

  def self.build_message_field_descriptor(name : String, number : Int32, type_name : String) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 3, number.to_u64)
    write_varint_field(io, 4, LABEL_OPTIONAL)
    write_varint_field(io, 5, TYPE_MESSAGE)
    write_string(io, 6, type_name)
    io.to_slice
  end

  def self.build_repeated_message_field_descriptor(name : String, number : Int32, type_name : String) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 3, number.to_u64)
    write_varint_field(io, 4, LABEL_REPEATED)
    write_varint_field(io, 5, TYPE_MESSAGE)
    write_string(io, 6, type_name)
    io.to_slice
  end

  def self.build_enum_field_descriptor(name : String, number : Int32, type_name : String) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 3, number.to_u64)
    write_varint_field(io, 4, LABEL_OPTIONAL)
    write_varint_field(io, 5, TYPE_ENUM)
    write_string(io, 6, type_name)
    io.to_slice
  end

  def self.build_oneof_field_descriptor(name : String, number : Int32, type : UInt64, oneof_index : Int32) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    write_varint_field(io, 3, number.to_u64)
    write_varint_field(io, 4, LABEL_OPTIONAL)
    write_varint_field(io, 5, type)
    write_varint_field(io, 9, oneof_index.to_u64)
    io.to_slice
  end

  def self.build_oneof_descriptor(name : String) : Bytes
    io = IO::Memory.new
    write_string(io, 1, name)
    io.to_slice
  end

  def self.build_map_entry_options : Bytes
    io = IO::Memory.new
    write_varint_field(io, 7, 1_u64)
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

def build_e2e_tls_server_context(require_client_cert : Bool = false) : OpenSSL::SSL::Context::Server
  ensure_e2e_tls_fixtures!
  ctx = OpenSSL::SSL::Context::Server.new
  ctx.certificate_chain = TLS_SERVER_CERT
  ctx.private_key = TLS_SERVER_KEY
  ctx.alpn_protocol = "h2"
  if require_client_cert
    ctx.ca_certificates = TLS_CA_CERT
    ctx.verify_mode = OpenSSL::SSL::VerifyMode::PEER | OpenSSL::SSL::VerifyMode::FAIL_IF_NO_PEER_CERT
  end
  ctx
end

def configure_e2e_tls_server(server : GRPC::Server, require_client_cert : Bool = false) : Nil
  server.use_tls(build_e2e_tls_server_context(require_client_cert))
end

def enable_e2e_reflection(server : GRPC::Server) : Nil
  reflection = server.enable_reflection
  reflection.add_file_descriptor(E2EReflectionWire.build_e2e_file_descriptor_proto)
  reflection.add_file_descriptor(E2EReflectionWire.build_common_file_descriptor_proto)
  reflection.add_file_descriptor(E2EReflectionWire.build_google_empty_file_descriptor_proto)
  reflection.add_file_descriptor(E2EReflectionWire.build_google_timestamp_file_descriptor_proto)
  reflection.add_file_descriptor(E2EReflectionWire.build_complex_file_descriptor_proto)
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
    return if Time.instant >= deadline
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

def e2e_tls_grpcurl_flags(
  include_ca : Bool = true,
  include_client_cert : Bool = false,
) : Array(String)
  ensure_e2e_tls_fixtures!
  args = [] of String
  if include_ca
    args << "-cacert"
    args << TLS_CA_CERT
  end
  if include_client_cert
    args << "-cert"
    args << TLS_CLIENT_CERT
    args << "-key"
    args << TLS_CLIENT_KEY
  end
  args
end

def grpcurl_call_args(
  port : Int32,
  method : String,
  flags : Array(String) = [] of String,
  include_proto : Bool = true,
  import_path : String = File.expand_path("../fixtures/grpcurl", __DIR__),
  proto : String = "e2e.proto",
) : Array(String)
  args = ["-plaintext"]
  if include_proto
    args << "-import-path"
    args << import_path
    args << "-proto"
    args << proto
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

def grpcurl_tls_call_args(
  host : String,
  port : Int32,
  method : String,
  flags : Array(String) = [] of String,
  include_proto : Bool = true,
  import_path : String = File.expand_path("../fixtures/grpcurl", __DIR__),
  proto : String = "e2e.proto",
) : Array(String)
  args = [] of String
  if include_proto
    args << "-import-path"
    args << import_path
    args << "-proto"
    args << proto
  end
  args.concat(with_default_grpcurl_timeout(flags))
  args << "#{host}:#{port}"
  args << method
  args
end

def grpcurl_tls_reflection_args(
  host : String,
  port : Int32,
  command : String,
  subject : String? = nil,
  flags : Array(String) = [] of String,
) : Array(String)
  args = [] of String
  args.concat(with_default_grpcurl_timeout(flags))
  args << "#{host}:#{port}"
  args << command
  if value = subject
    args << value
  end
  args
end
