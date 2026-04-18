require "./spec_helper"
require "../src/protoc-gen-crystal-grpc"

# ---------------------------------------------------------------------------
# Helpers to build a synthetic CodeGeneratorRequest in protobuf binary format
# ---------------------------------------------------------------------------

module TestProtoEncode
  def self.varint(io : IO, v : UInt64) : Nil
    loop do
      b = (v & 0x7F).to_u8
      v >>= 7
      io.write_byte(v != 0 ? b | 0x80_u8 : b)
      break if v == 0
    end
  end

  def self.string(io : IO, field : Int32, value : String) : Nil
    return if value.empty?
    bytes = value.to_slice
    varint(io, ((field << 3) | 2).to_u64)
    varint(io, bytes.size.to_u64)
    io.write(bytes)
  end

  def self.bool(io : IO, field : Int32, value : Bool) : Nil
    return unless value
    varint(io, ((field << 3) | 0).to_u64)
    varint(io, 1_u64)
  end

  def self.embedded(io : IO, field : Int32, & : IO ->) : Nil
    tmp = IO::Memory.new
    yield tmp
    data = tmp.to_slice
    return if data.empty?
    varint(io, ((field << 3) | 2).to_u64)
    varint(io, data.size.to_u64)
    io.write(data)
  end

  # Build a MethodDescriptorProto
  def self.method_proto(name : String, input_type : String, output_type : String,
                        client_streaming : Bool = false, server_streaming : Bool = false) : Bytes
    io = IO::Memory.new
    string(io, 1, name)
    string(io, 2, input_type)
    string(io, 3, output_type)
    bool(io, 5, client_streaming)
    bool(io, 6, server_streaming)
    io.to_slice
  end

  # Build a ServiceDescriptorProto
  def self.service_proto(name : String, methods : Array(Bytes)) : Bytes
    io = IO::Memory.new
    string(io, 1, name)
    methods.each do |method_bytes|
      varint(io, ((2 << 3) | 2).to_u64)
      varint(io, method_bytes.size.to_u64)
      io.write(method_bytes)
    end
    io.to_slice
  end

  # Build a FileDescriptorProto
  def self.file_proto(name : String, package : String, services : Array(Bytes)) : Bytes
    io = IO::Memory.new
    string(io, 1, name)
    string(io, 2, package)
    services.each do |svc_bytes|
      varint(io, ((6 << 3) | 2).to_u64)
      varint(io, svc_bytes.size.to_u64)
      io.write(svc_bytes)
    end
    io.to_slice
  end

  # Build a CodeGeneratorRequest
  def self.request(files_to_generate : Array(String), proto_files : Array(Bytes), parameter : String = "") : Bytes
    io = IO::Memory.new
    files_to_generate.each { |fname| string(io, 1, fname) }
    string(io, 2, parameter)
    proto_files.each do |file_bytes|
      varint(io, ((15 << 3) | 2).to_u64)
      varint(io, file_bytes.size.to_u64)
      io.write(file_bytes)
    end
    io.to_slice
  end
end

# ---------------------------------------------------------------------------
# Spec
# ---------------------------------------------------------------------------

describe CrystalGrpcCodeGenerator do
  it "generates a unary service stub" do
    method = TestProtoEncode.method_proto(
      "SayHello",
      ".helloworld.HelloRequest",
      ".helloworld.HelloReply"
    )
    svc = TestProtoEncode.service_proto("Greeter", [method])
    file = TestProtoEncode.file_proto("helloworld.proto", "helloworld", [svc])
    req_bytes = TestProtoEncode.request(["helloworld.proto"], [file])

    request = parse_request(req_bytes)
    generator = CrystalGrpcCodeGenerator.new
    response_bytes = generator.run(request)

    # Parse the response to extract the generated file content
    c = extract_file_content(response_bytes, "helloworld.grpc.cr") ||
        fail("Expected helloworld.grpc.cr to be generated")

    c.should contain("module Greeter")
    c.should contain("FULL_NAME = \"helloworld.Greeter\"")
    c.should contain("abstract class Service < GRPC::Service")
    c.should contain(%("helloworld.Greeter"))
    c.should contain("abstract def say_hello(request : HelloRequest, ctx : GRPC::ServerContext) : HelloReply")
    c.should contain("class Client")
    c.should contain("def say_hello(request : HelloRequest, ctx : GRPC::ClientContext")
    c.should contain("@channel.unary_call")
    c.should contain("protected def marshaller_for(type : T.class) : GRPC::Marshaller(T) forall T")
    c.should contain("res_marshaller = marshaller_for(HelloReply)")
    c.should contain("res_marshaller.decode(response.raw)")
  end

  it "generates a server-streaming stub" do
    method = TestProtoEncode.method_proto(
      "Range",
      ".numbers.Number",
      ".numbers.Number",
      server_streaming: true
    )
    svc = TestProtoEncode.service_proto("Numbers", [method])
    file = TestProtoEncode.file_proto("numbers.proto", "numbers", [svc])
    req_bytes = TestProtoEncode.request(["numbers.proto"], [file])

    content = run_generator(req_bytes, "numbers.grpc.cr")
    content.should contain("def server_streaming?(method : String) : Bool")
    content.should contain(%("Range"))
    content.should contain("dispatch_server_stream")
    content.should contain("GRPC::ServerStream(Number)")
    content.should contain("open_server_stream")
    content.should contain("req_marshaller = marshaller_for(Number)")
    content.should contain("res_marshaller = marshaller_for(Number)")
    content.should contain("stream.push(res_marshaller.decode(bytes))")
  end

  it "generates a client-streaming stub" do
    method = TestProtoEncode.method_proto(
      "Sum",
      ".numbers.Number",
      ".numbers.Number",
      client_streaming: true
    )
    svc = TestProtoEncode.service_proto("Numbers", [method])
    file = TestProtoEncode.file_proto("numbers.proto", "numbers", [svc])

    content = run_generator(
      TestProtoEncode.request(["numbers.proto"], [file]),
      "numbers.grpc.cr"
    )
    content.should contain("def client_streaming?(method : String) : Bool")
    content.should contain("dispatch_client_stream")
    content.should contain("open_client_stream_live")
    content.should contain("GRPC::ClientStream(Number, Number)")
    content.should contain("GRPC::RequestStream(Number)")
    content.should contain("req_marshaller = marshaller_for(Number)")
    content.should contain("res_marshaller = marshaller_for(Number)")
    content.should contain("raw.send_raw(req_marshaller.encode(msg))")
    content.should contain("result_chan.send(res_marshaller.decode(body))")
  end

  it "generates a bidirectional-streaming stub" do
    method = TestProtoEncode.method_proto(
      "Transform",
      ".numbers.Number",
      ".numbers.Number",
      client_streaming: true,
      server_streaming: true
    )
    svc = TestProtoEncode.service_proto("Numbers", [method])
    file = TestProtoEncode.file_proto("numbers.proto", "numbers", [svc])

    content = run_generator(
      TestProtoEncode.request(["numbers.proto"], [file]),
      "numbers.grpc.cr"
    )
    content.should contain("def bidi_streaming?(method : String) : Bool")
    content.should contain("dispatch_bidi_stream")
    content.should contain("open_bidi_stream")
    content.should contain("req_marshaller = marshaller_for(Number)")
    content.should contain("res_marshaller = marshaller_for(Number)")
    content.should contain("recv_chan.send(res_marshaller.decode(bytes))")
    content.should contain("raw.send_raw(req_marshaller.encode(msg))")
  end

  it "wraps generated code in a module for a non-empty package" do
    method = TestProtoEncode.method_proto(
      "SayHello",
      ".helloworld.HelloRequest",
      ".helloworld.HelloReply"
    )
    svc = TestProtoEncode.service_proto("Greeter", [method])
    file = TestProtoEncode.file_proto("helloworld.proto", "helloworld", [svc])

    content = run_generator(
      TestProtoEncode.request(["helloworld.proto"], [file]),
      "helloworld.grpc.cr"
    )
    content.should contain("module Helloworld")
    content.should contain("end\n")
  end

  it "emits no module wrapper for files without a package" do
    method = TestProtoEncode.method_proto("Ping", ".Echo", ".Echo")
    svc = TestProtoEncode.service_proto("EchoService", [method])
    file = TestProtoEncode.file_proto("echo.proto", "", [svc])

    content = run_generator(
      TestProtoEncode.request(["echo.proto"], [file]),
      "echo.grpc.cr"
    )
    content.should contain("module EchoService")
    content.should contain("abstract class Service < GRPC::Service")
  end

  it "snake_cases RPC method names" do
    method = TestProtoEncode.method_proto("GetUserById", ".myapp.Foo", ".myapp.Bar")
    svc = TestProtoEncode.service_proto("UserService", [method])
    file = TestProtoEncode.file_proto("user.proto", "myapp", [svc])

    content = run_generator(
      TestProtoEncode.request(["user.proto"], [file]),
      "user.grpc.cr"
    )
    content.should contain("def get_user_by_id(")
  end

  it "skips files with no services" do
    file = TestProtoEncode.file_proto("empty.proto", "empty", [] of Bytes)
    req_bytes = TestProtoEncode.request(["empty.proto"], [file])

    request = parse_request(req_bytes)
    generator = CrystalGrpcCodeGenerator.new
    response_bytes = generator.run(request)

    content = extract_file_content(response_bytes, "empty.grpc.cr")
    content.should be_nil
  end

  it "handles cross-package type references" do
    method = TestProtoEncode.method_proto(
      "Ping",
      ".google.protobuf.Empty",
      ".mypkg.Reply"
    )
    svc = TestProtoEncode.service_proto("Svc", [method])
    file = TestProtoEncode.file_proto("svc.proto", "mypkg", [svc])

    content = run_generator(
      TestProtoEncode.request(["svc.proto"], [file]),
      "svc.grpc.cr"
    )
    # .google.protobuf.Empty → Google::Protobuf::Empty (different package)
    content.should contain("Google::Protobuf::Empty")
    # .mypkg.Reply in the mypkg package → just Reply
    content.should contain(": Reply")
  end

  it "prefers type_map mappings when provided" do
    method = TestProtoEncode.method_proto(
      "Ping",
      ".google.protobuf.Empty",
      ".mypkg.Reply"
    )
    svc = TestProtoEncode.service_proto("Svc", [method])
    file = TestProtoEncode.file_proto("svc.proto", "mypkg", [svc])
    req = TestProtoEncode.request(
      ["svc.proto"],
      [file],
      "type_map=.google.protobuf.Empty=My::Custom::Empty;.mypkg.Reply=Reply"
    )

    content = run_generator(req, "svc.grpc.cr")
    content.should contain("My::Custom::Empty")
    content.should_not contain("Google::Protobuf::Empty")
  end

  it "parses type_map parameters with surrounding whitespace" do
    method = TestProtoEncode.method_proto(
      "Ping",
      ".google.protobuf.Empty",
      ".mypkg.Reply"
    )
    svc = TestProtoEncode.service_proto("Svc", [method])
    file = TestProtoEncode.file_proto("svc.proto", "mypkg", [svc])
    req = TestProtoEncode.request(
      ["svc.proto"],
      [file],
      " type_map = .google.protobuf.Empty = My::Custom::Empty ; .mypkg.Reply = Reply "
    )

    content = run_generator(req, "svc.grpc.cr")
    content.should contain("My::Custom::Empty")
    content.should_not contain("Google::Protobuf::Empty")
  end

  it "resolves underscore+nested+import-style types via type_map" do
    method = TestProtoEncode.method_proto(
      "Query",
      ".acme_ml.v1.RequestEnvelope.Payload",
      ".third_party.shared.v2.Result.Container"
    )
    svc = TestProtoEncode.service_proto("Gateway", [method])
    file = TestProtoEncode.file_proto("gateway.proto", "acme_ml.v1", [svc])
    req = TestProtoEncode.request(
      ["gateway.proto"],
      [file],
      "type_map=.acme_ml.v1.RequestEnvelope.Payload=AcmeML::V1::RequestEnvelope::Payload;.third_party.shared.v2.Result.Container=Shared::V2::Result::Container"
    )

    content = run_generator(req, "gateway.grpc.cr")
    content.should contain("AcmeML::V1::RequestEnvelope::Payload")
    content.should contain("Shared::V2::Result::Container")
    content.should contain("module AcmeMl::V1")
  end

  it "uses canonical resolution when type_map is not provided" do
    method = TestProtoEncode.method_proto(
      "Query",
      ".acme_ml.v1.RequestEnvelope.Payload",
      ".third_party.shared.v2.Result.Container"
    )
    svc = TestProtoEncode.service_proto("Gateway", [method])
    file = TestProtoEncode.file_proto("gateway.proto", "acme_ml.v1", [svc])

    content = run_generator(
      TestProtoEncode.request(["gateway.proto"], [file]),
      "gateway.grpc.cr"
    )
    content.should contain("RequestEnvelope::Payload")
    content.should contain("ThirdParty::Shared::V2::Result::Container")
  end

  it "uses strict type_map mode when mapping is incomplete" do
    method = TestProtoEncode.method_proto(
      "Query",
      ".acme_ml.v1.RequestEnvelope.Payload",
      ".third_party.shared.v2.Result.Container"
    )
    svc = TestProtoEncode.service_proto("Gateway", [method])
    file = TestProtoEncode.file_proto("gateway.proto", "acme_ml.v1", [svc])
    req = TestProtoEncode.request(
      ["gateway.proto"],
      [file],
      "type_map=.acme_ml.v1.RequestEnvelope.Payload=AcmeML::V1::RequestEnvelope::Payload"
    )

    request = parse_request(req)
    generator = CrystalGrpcCodeGenerator.new
    expect_raises(ArgumentError, /Missing type_map entry/) do
      generator.run(request)
    end
  end

  it "generates service stubs for all 4 RPC types in one service" do
    unary = TestProtoEncode.method_proto("Unary", ".echo.Req", ".echo.Res")
    ss = TestProtoEncode.method_proto("ServerStream", ".echo.Req", ".echo.Res", server_streaming: true)
    cs = TestProtoEncode.method_proto("ClientStream", ".echo.Req", ".echo.Res", client_streaming: true)
    bidi = TestProtoEncode.method_proto("Bidi", ".echo.Req", ".echo.Res", client_streaming: true, server_streaming: true)
    svc = TestProtoEncode.service_proto("Echo", [unary, ss, cs, bidi])
    file = TestProtoEncode.file_proto("echo.proto", "echo", [svc])

    content = run_generator(
      TestProtoEncode.request(["echo.proto"], [file]),
      "echo.grpc.cr"
    )
    content.should contain("module Echo")
    # All abstract methods present
    content.should contain("abstract def unary(")
    content.should contain("abstract def server_stream(")
    content.should contain("abstract def client_stream(")
    content.should contain("abstract def bidi(")
    # All dispatch methods present
    content.should contain("dispatch_server_stream")
    content.should contain("dispatch_client_stream")
    content.should contain("dispatch_bidi_stream")
    # Client stub methods present
    content.should contain("def unary(request")
    content.should contain("def server_stream(request")
    content.should contain("def client_stream(requests")
    content.should contain("def bidi(requests")
  end
end

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

private def run_generator(request_bytes : Bytes, output_file : String) : String
  request = parse_request(request_bytes)
  generator = CrystalGrpcCodeGenerator.new
  response_bytes = generator.run(request)
  extract_file_content(response_bytes, output_file) ||
    fail("Expected #{output_file} to be generated")
end

private def parse_request(data : Bytes) : PluginCodeGeneratorRequest
  PluginCodeGeneratorRequest.read_proto(IO::Memory.new(data))
end

# Parse the CodeGeneratorResponse binary and return the content of the named
# file, or nil if not found.
private def extract_file_content(response : Bytes, filename : String) : String?
  parsed = Proto::Bootstrap::CodeGeneratorResponse.decode(IO::Memory.new(response))
  parsed.file.each do |response_file|
    return response_file.content if response_file.name == filename
  end
  nil
end
