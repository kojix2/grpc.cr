#!/usr/bin/env crystal
# protoc-gen-crystal-grpc — protoc plugin that generates Crystal gRPC service stubs.
#
# For each .proto file passed by protoc the plugin emits a .grpc.cr file with:
#   * An abstract GRPC::Service subclass for the server side
#   * A typed client stub class
#
# All four RPC streaming variants are supported:
#   unary, server-streaming, client-streaming, bidirectional-streaming
#
# Usage:
#   protoc --crystal-grpc_out=OUTPUT_DIR \
#          --plugin=protoc-gen-crystal-grpc \
#          path/to/service.proto
#
# The plugin expects message types to already be available in the generated
# output (e.g. from protoc-gen-crystal).  It only emits service infrastructure.

require "./grpc_generator/naming_adapter"

# ---------------------------------------------------------------------------
# Minimal protobuf binary decoder
# ---------------------------------------------------------------------------

# ProtoField holds a single decoded field from a protobuf message.
struct ProtoField
  getter field_number : Int32
  getter wire_type : Int32
  getter raw : Bytes | UInt64

  def initialize(@field_number : Int32, @wire_type : Int32, @raw : Bytes | UInt64)
  end

  def bytes? : Bytes?
    r = @raw
    r.is_a?(Bytes) ? r : nil
  end

  def as_string : String
    b = bytes?
    b ? String.new(b) : ""
  end

  def as_uint : UInt64
    r = @raw
    r.is_a?(UInt64) ? r : 0_u64
  end

  def as_bool : Bool
    as_uint != 0
  end
end

module ProtobufWireDecoder
  # Decode a base-128 varint from *data* starting at *offset*.
  # Returns {value, bytes_consumed}.
  def self.varint(data : Bytes, offset : Int32) : {UInt64, Int32}
    result = 0_u64
    shift = 0
    consumed = 0
    while offset + consumed < data.size
      byte = data[offset + consumed].to_u64
      consumed += 1
      result |= (byte & 0x7F) << shift
      shift += 7
      break unless (byte & 0x80) != 0
    end
    {result, consumed}
  end

  # Decode all top-level fields in a binary protobuf message.
  def self.fields(data : Bytes) : Array(ProtoField)
    result = [] of ProtoField
    i = 0
    while i < data.size
      tag, c = varint(data, i)
      i += c
      field_num = (tag >> 3).to_i32
      wire_type = (tag & 0x7).to_i32
      case wire_type
      when 0 # varint
        val, c = varint(data, i)
        i += c
        result << ProtoField.new(field_num, 0, val)
      when 1 # 64-bit little-endian
        break if i + 8 > data.size
        val = IO::ByteFormat::LittleEndian.decode(UInt64, data[i, 8])
        i += 8
        result << ProtoField.new(field_num, 1, val)
      when 2 # length-delimited
        len, c = varint(data, i)
        i += c
        len_i = len.to_i32
        break if i + len_i > data.size
        # .dup keeps the bytes alive after *data* is reclaimed
        result << ProtoField.new(field_num, 2, data[i, len_i].dup)
        i += len_i
      when 5 # 32-bit little-endian
        break if i + 4 > data.size
        val = IO::ByteFormat::LittleEndian.decode(UInt32, data[i, 4]).to_u64
        i += 4
        result << ProtoField.new(field_num, 5, val)
      else
        break # unknown wire type — stop
      end
    end
    result
  end

  def self.string(fs : Array(ProtoField), num : Int32) : String
    fs.each { |field| return field.as_string if field.field_number == num && field.wire_type == 2 }
    ""
  end

  def self.strings(fs : Array(ProtoField), num : Int32) : Array(String)
    fs.select { |field| field.field_number == num && field.wire_type == 2 }.map(&.as_string)
  end

  def self.messages(fs : Array(ProtoField), num : Int32) : Array(Bytes)
    fs.compact_map { |field| field.bytes? if field.field_number == num && field.wire_type == 2 }
  end

  def self.bool(fs : Array(ProtoField), num : Int32) : Bool
    fs.each { |field| return field.as_bool if field.field_number == num && field.wire_type == 0 }
    false
  end
end

# ---------------------------------------------------------------------------
# Minimal protobuf binary encoder (for CodeGeneratorResponse)
# ---------------------------------------------------------------------------

module ProtobufWireEncoder
  def self.varint(io : IO, value : UInt64) : Nil
    loop do
      byte = (value & 0x7F).to_u8
      value >>= 7
      io.write_byte(value != 0 ? byte | 0x80_u8 : byte)
      break if value == 0
    end
  end

  def self.string(io : IO, field_num : Int32, value : String) : Nil
    return if value.empty?
    bytes = value.to_slice
    varint(io, ((field_num << 3) | 2).to_u64)
    varint(io, bytes.size.to_u64)
    io.write(bytes)
  end

  def self.message(io : IO, field_num : Int32, & : IO ->) : Nil
    tmp = IO::Memory.new
    yield tmp
    data = tmp.to_slice
    return if data.empty?
    varint(io, ((field_num << 3) | 2).to_u64)
    varint(io, data.size.to_u64)
    io.write(data)
  end
end

# ---------------------------------------------------------------------------
# Descriptor types parsed from the binary CodeGeneratorRequest
# ---------------------------------------------------------------------------

# MethodDescriptor mirrors google.protobuf.MethodDescriptorProto
struct MethodDescriptor
  getter name : String
  getter input_type : String
  getter output_type : String
  getter? client_streaming : Bool
  getter? server_streaming : Bool

  def initialize(@name, @input_type, @output_type, @client_streaming, @server_streaming)
  end

  def self.parse(data : Bytes) : MethodDescriptor
    fs = ProtobufWireDecoder.fields(data)
    new(
      ProtobufWireDecoder.string(fs, 1),
      ProtobufWireDecoder.string(fs, 2),
      ProtobufWireDecoder.string(fs, 3),
      ProtobufWireDecoder.bool(fs, 5),
      ProtobufWireDecoder.bool(fs, 6),
    )
  end

  # Returns one of :unary, :server_streaming, :client_streaming, :bidi
  def rpc_type : Symbol
    case {@client_streaming, @server_streaming}
    when {false, false} then :unary
    when {false, true}  then :server_streaming
    when {true, false}  then :client_streaming
    else                     :bidi
    end
  end
end

# ServiceDescriptor mirrors google.protobuf.ServiceDescriptorProto
struct ServiceDescriptor
  getter name : String
  getter methods : Array(MethodDescriptor)

  def initialize(@name, @methods)
  end

  def self.parse(data : Bytes) : ServiceDescriptor
    fs = ProtobufWireDecoder.fields(data)
    new(
      ProtobufWireDecoder.string(fs, 1),
      ProtobufWireDecoder.messages(fs, 2).map { |method_bytes| MethodDescriptor.parse(method_bytes) },
    )
  end
end

# ProtoFileDescriptor mirrors google.protobuf.FileDescriptorProto (only the fields we need)
struct ProtoFileDescriptor
  getter name : String
  getter package : String
  getter services : Array(ServiceDescriptor)

  def initialize(@name, @package, @services)
  end

  def self.parse(data : Bytes) : ProtoFileDescriptor
    fs = ProtobufWireDecoder.fields(data)
    new(
      ProtobufWireDecoder.string(fs, 1),
      ProtobufWireDecoder.string(fs, 2),
      ProtobufWireDecoder.messages(fs, 6).map { |svc_bytes| ServiceDescriptor.parse(svc_bytes) },
    )
  end
end

# PluginCodeGeneratorRequest mirrors google.protobuf.compiler.CodeGeneratorRequest
struct PluginCodeGeneratorRequest
  getter files_to_generate : Array(String)
  getter parameter : String
  getter proto_files : Array(ProtoFileDescriptor)

  def initialize(@files_to_generate, @parameter, @proto_files)
  end

  def self.parse(data : Bytes) : PluginCodeGeneratorRequest
    fs = ProtobufWireDecoder.fields(data)
    new(
      ProtobufWireDecoder.strings(fs, 1),
      ProtobufWireDecoder.string(fs, 2),
      ProtobufWireDecoder.messages(fs, 15).map { |file_bytes| ProtoFileDescriptor.parse(file_bytes) },
    )
  end
end

# ---------------------------------------------------------------------------
# Crystal gRPC code generator
# ---------------------------------------------------------------------------

class CrystalGrpcCodeGenerator
  @resolver : GRPC::Generator::TypeNameResolver

  def initialize
    @resolver = GRPC::Generator::CanonicalTypeNameResolver.new
  end

  # Entry point: process the request and return an encoded CodeGeneratorResponse.
  def run(request : PluginCodeGeneratorRequest) : Bytes
    @resolver = build_type_name_resolver(request.parameter)

    file_index = {} of String => ProtoFileDescriptor
    request.proto_files.each { |file_desc| file_index[file_desc.name] = file_desc }

    generated = [] of {String, String}
    request.files_to_generate.each do |proto_name|
      fd = file_index[proto_name]?
      next unless fd
      next if fd.services.empty?
      content = generate_file(fd)
      output_name = proto_name.sub(/\.proto$/, ".grpc.cr")
      generated << {output_name, content}
    end

    io = IO::Memory.new
    encode_response(io, generated)
    io.to_slice
  end

  # ---- File-level generation ----

  private def generate_file(fd : ProtoFileDescriptor) : String
    String.build do |str|
      str << "# Code generated by protoc-gen-crystal-grpc. DO NOT EDIT.\n"
      str << "# Source: #{fd.name}\n"
      str << "#\n"
      str << "# Requires message types to be available (e.g. from a .pb.cr file).\n"
      str << "# Add `require \"grpc\"` and the corresponding message file before\n"
      str << "# requiring this file.\n"
      str << "\n"

      mod = package_to_module(fd.package)
      if mod
        str << "module #{mod}\n"
        fd.services.each { |svc| generate_service_pair(str, svc, fd, "  ") }
        str << "end\n"
      else
        fd.services.each { |svc| generate_service_pair(str, svc, fd, "") }
      end
    end
  end

  # Emit both the abstract service class and the client stub for one service.
  private def generate_service_pair(str : String::Builder, svc : ServiceDescriptor,
                                    fd : ProtoFileDescriptor, indent : String) : Nil
    generate_service_class(str, svc, fd, indent)
    str << "\n"
    generate_client_class(str, svc, fd, indent)
  end

  # ---- Abstract service (server side) ----

  private def generate_service_class(str : String::Builder, svc : ServiceDescriptor,
                                     fd : ProtoFileDescriptor, indent : String) : Nil
    full_name = full_service_name(fd, svc)

    str << "\n"
    str << "#{indent}# #{svc.name}Service is the generated abstract base class for server implementations.\n"
    str << "#{indent}# Subclass it and implement each RPC method, then register with GRPC::Server#handle.\n"
    str << "#{indent}abstract class #{svc.name}Service < GRPC::Service\n"
    str << "#{indent}  SERVICE_NAME = #{full_name.inspect}\n"
    str << "\n"
    str << "#{indent}  def service_name : String\n"
    str << "#{indent}    SERVICE_NAME\n"
    str << "#{indent}  end\n"
    str << "\n"
    str << "#{indent}  # Override this hook to swap marshaller implementations.\n"
    str << "#{indent}  protected def marshaller_for(type : T.class) : GRPC::Marshaller(T) forall T\n"
    str << "#{indent}    GRPC::ProtoMarshaller(T).new\n"
    str << "#{indent}  end\n"

    # Abstract method declarations
    str << "\n"
    svc.methods.each do |meth|
      input = resolve_type(meth.input_type, fd.package)
      output = resolve_type(meth.output_type, fd.package)
      mname = to_snake_case(meth.name)
      case meth.rpc_type
      when :unary
        str << "#{indent}  abstract def #{mname}(request : #{input}, ctx : GRPC::ServerContext) : #{output}\n"
      when :server_streaming
        str << "#{indent}  abstract def #{mname}(request : #{input}, writer : GRPC::ResponseStream(#{output}), ctx : GRPC::ServerContext) : GRPC::Status\n"
      when :client_streaming
        str << "#{indent}  abstract def #{mname}(requests : GRPC::RequestStream(#{input}), ctx : GRPC::ServerContext) : #{output}\n"
      when :bidi
        str << "#{indent}  abstract def #{mname}(requests : GRPC::RequestStream(#{input}), writer : GRPC::ResponseStream(#{output}), ctx : GRPC::ServerContext) : GRPC::Status\n"
      end
    end

    # dispatch (unary)
    unary = svc.methods.select { |meth| meth.rpc_type == :unary }
    str << "\n"
    str << "#{indent}  def dispatch(method : String, body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}\n"
    if unary.empty?
      str << "#{indent}    {Bytes.empty, GRPC::Status.unimplemented(\"method \#{method} not found\")}\n"
    else
      str << "#{indent}    case method\n"
      unary.each do |meth|
        input = resolve_type(meth.input_type, fd.package)
        output = resolve_type(meth.output_type, fd.package)
        mname = to_snake_case(meth.name)
        str << "#{indent}    when #{meth.name.inspect}\n"
        str << "#{indent}      req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}      res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}      req  = req_marshaller.load(body)\n"
        str << "#{indent}      resp = #{mname}(req, ctx)\n"
        str << "#{indent}      {res_marshaller.dump(resp), GRPC::Status.ok}\n"
      end
      str << "#{indent}    else\n"
      str << "#{indent}      {Bytes.empty, GRPC::Status.unimplemented(\"method \#{method} not found\")}\n"
      str << "#{indent}    end\n"
    end
    str << "#{indent}  rescue ex : GRPC::StatusError\n"
    str << "#{indent}    {Bytes.empty, ex.status}\n"
    str << "#{indent}  rescue ex\n"
    str << "#{indent}    {Bytes.empty, GRPC::Status.internal(ex.message || \"internal error\")}\n"
    str << "#{indent}  end\n"

    # server_streaming? + dispatch_server_stream
    ss = svc.methods.select { |meth| meth.rpc_type == :server_streaming }
    unless ss.empty?
      str << "\n"
      emit_streaming_predicate(str, "server_streaming?", ss, indent)
      str << "\n"
      str << "#{indent}  def dispatch_server_stream(method : String, body : Bytes,\n"
      str << "#{indent}                             ctx : GRPC::ServerContext,\n"
      str << "#{indent}                             writer : GRPC::RawResponseStream) : GRPC::Status\n"
      str << "#{indent}    case method\n"
      ss.each do |meth|
        input = resolve_type(meth.input_type, fd.package)
        output = resolve_type(meth.output_type, fd.package)
        mname = to_snake_case(meth.name)
        str << "#{indent}    when #{meth.name.inspect}\n"
        str << "#{indent}      req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}      res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}      typed_writer = GRPC::ResponseStream(#{output}).new(writer, res_marshaller)\n"
        str << "#{indent}      #{mname}(req_marshaller.load(body), typed_writer, ctx)\n"
      end
      str << "#{indent}    else\n"
      str << "#{indent}      GRPC::Status.unimplemented(\"method \#{method} not found\")\n"
      str << "#{indent}    end\n"
      str << "#{indent}  rescue ex : GRPC::StatusError\n"
      str << "#{indent}    ex.status\n"
      str << "#{indent}  rescue ex\n"
      str << "#{indent}    GRPC::Status.internal(ex.message || \"internal error\")\n"
      str << "#{indent}  end\n"
    end

    # client_streaming? + dispatch_client_stream
    cs = svc.methods.select { |meth| meth.rpc_type == :client_streaming }
    unless cs.empty?
      emit_streaming_predicate(str, "client_streaming?", cs, indent)
      str << "\n"
      str << "#{indent}  def dispatch_client_stream(method : String, requests : GRPC::RawRequestStream,\n"
      str << "#{indent}                             ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}\n"
      str << "#{indent}    case method\n"
      cs.each do |meth|
        input = resolve_type(meth.input_type, fd.package)
        output = resolve_type(meth.output_type, fd.package)
        mname = to_snake_case(meth.name)
        str << "#{indent}    when #{meth.name.inspect}\n"
        str << "#{indent}      req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}      res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}      reqs = GRPC::RequestStream(#{input}).new(requests.ch, req_marshaller)\n"
        str << "#{indent}      resp = #{mname}(reqs, ctx)\n"
        str << "#{indent}      {res_marshaller.dump(resp), GRPC::Status.ok}\n"
      end
      str << "#{indent}    else\n"
      str << "#{indent}      {Bytes.empty, GRPC::Status.unimplemented(\"method \#{method} not found\")}\n"
      str << "#{indent}    end\n"
      str << "#{indent}  rescue ex : GRPC::StatusError\n"
      str << "#{indent}    {Bytes.empty, ex.status}\n"
      str << "#{indent}  rescue ex\n"
      str << "#{indent}    {Bytes.empty, GRPC::Status.internal(ex.message || \"internal error\")}\n"
      str << "#{indent}  end\n"
    end

    # bidi_streaming? + dispatch_bidi_stream
    bidi = svc.methods.select { |meth| meth.rpc_type == :bidi }
    unless bidi.empty?
      emit_streaming_predicate(str, "bidi_streaming?", bidi, indent)
      str << "\n"
      str << "#{indent}  def dispatch_bidi_stream(method : String, requests : GRPC::RawRequestStream,\n"
      str << "#{indent}                           ctx : GRPC::ServerContext,\n"
      str << "#{indent}                           writer : GRPC::RawResponseStream) : GRPC::Status\n"
      str << "#{indent}    case method\n"
      bidi.each do |meth|
        input = resolve_type(meth.input_type, fd.package)
        output = resolve_type(meth.output_type, fd.package)
        mname = to_snake_case(meth.name)
        str << "#{indent}    when #{meth.name.inspect}\n"
        str << "#{indent}      req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}      res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}      reqs = GRPC::RequestStream(#{input}).new(requests.ch, req_marshaller)\n"
        str << "#{indent}      typed_writer = GRPC::ResponseStream(#{output}).new(writer, res_marshaller)\n"
        str << "#{indent}      #{mname}(reqs, typed_writer, ctx)\n"
      end
      str << "#{indent}    else\n"
      str << "#{indent}      GRPC::Status.unimplemented(\"method \#{method} not found\")\n"
      str << "#{indent}    end\n"
      str << "#{indent}  rescue ex : GRPC::StatusError\n"
      str << "#{indent}    ex.status\n"
      str << "#{indent}  rescue ex\n"
      str << "#{indent}    GRPC::Status.internal(ex.message || \"internal error\")\n"
      str << "#{indent}  end\n"
    end

    str << "#{indent}end\n"
  end

  # ---- Client stub ----

  private def generate_client_class(str : String::Builder, svc : ServiceDescriptor,
                                    fd : ProtoFileDescriptor, indent : String) : Nil
    str << "#{indent}# #{svc.name}Client is the generated type-safe client stub.\n"
    str << "#{indent}# Create one per GRPC::Channel and reuse across calls.\n"
    str << "#{indent}class #{svc.name}Client\n"
    str << "#{indent}  def initialize(@channel : GRPC::Channel)\n"
    str << "#{indent}  end\n"
    str << "\n"
    str << "#{indent}  # Override this hook to swap marshaller implementations.\n"
    str << "#{indent}  protected def marshaller_for(type : T.class) : GRPC::Marshaller(T) forall T\n"
    str << "#{indent}    GRPC::ProtoMarshaller(T).new\n"
    str << "#{indent}  end\n"

    svc.methods.each do |meth|
      input = resolve_type(meth.input_type, fd.package)
      output = resolve_type(meth.output_type, fd.package)
      mname = to_snake_case(meth.name)
      sname = "#{svc.name}Service::SERVICE_NAME"
      str << "\n"
      case meth.rpc_type
      when :unary
        str << "#{indent}  def #{mname}(request : #{input}, ctx : GRPC::ClientContext = GRPC::ClientContext.new) : #{output}\n"
        str << "#{indent}    req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}    res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}    body, status = @channel.unary_call(#{sname}, #{meth.name.inspect}, req_marshaller.dump(request), ctx)\n"
        str << "#{indent}    raise GRPC::StatusError.new(status) unless status.ok?\n"
        str << "#{indent}    res_marshaller.load(body)\n"
        str << "#{indent}  end\n"
      when :server_streaming
        str << "#{indent}  def #{mname}(request : #{input}, ctx : GRPC::ClientContext = GRPC::ClientContext.new) : GRPC::ServerStream(#{output})\n"
        str << "#{indent}    req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}    res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}    raw    = @channel.open_server_stream(#{sname}, #{meth.name.inspect}, req_marshaller.dump(request), ctx)\n"
        str << "#{indent}    stream = GRPC::ServerStream(#{output}).new(-> { raw.status }, -> { raw.trailers }, -> { raw.cancel })\n"
        str << "#{indent}    spawn do\n"
        str << "#{indent}      begin\n"
        str << "#{indent}        raw.each { |bytes| stream.push(res_marshaller.load(bytes)) }\n"
        str << "#{indent}        stream.finish\n"
        str << "#{indent}      rescue ex\n"
        str << "#{indent}        stream.finish(GRPC::Status.internal(ex.message || \"error\"))\n"
        str << "#{indent}      end\n"
        str << "#{indent}    end\n"
        str << "#{indent}    stream\n"
        str << "#{indent}  end\n"
      when :client_streaming
        str << "#{indent}  # #{mname} opens a client-streaming RPC.\n"
        str << "#{indent}  # Send messages via the returned ClientStream, then call close_and_recv\n"
        str << "#{indent}  # to flush and receive the server's single response.\n"
        str << "#{indent}  def #{mname}(ctx : GRPC::ClientContext = GRPC::ClientContext.new) : GRPC::ClientStream(#{input}, #{output})\n"
        str << "#{indent}    raw = @channel.open_client_stream_live(#{sname}, #{meth.name.inspect}, ctx)\n"
        str << "#{indent}    req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}    res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}    result_chan = ::Channel(#{output} | Exception).new(1)\n"
        str << "#{indent}    send_proc = Proc(#{input}, Nil).new { |msg| raw.send_raw(req_marshaller.dump(msg)) }\n"
        str << "#{indent}    close_proc = Proc(Nil).new {\n"
        str << "#{indent}      spawn do\n"
        str << "#{indent}        begin\n"
        str << "#{indent}          body = raw.close_and_recv\n"
        str << "#{indent}          st = raw.status\n"
        str << "#{indent}          if st.ok?\n"
        str << "#{indent}            result_chan.send(res_marshaller.load(body)) rescue nil\n"
        str << "#{indent}          else\n"
        str << "#{indent}            result_chan.send(GRPC::StatusError.new(st)) rescue nil\n"
        str << "#{indent}          end\n"
        str << "#{indent}        rescue ex\n"
        str << "#{indent}          result_chan.send(ex) rescue nil\n"
        str << "#{indent}        end\n"
        str << "#{indent}      end\n"
        str << "#{indent}    }\n"
        str << "#{indent}    GRPC::ClientStream(#{input}, #{output}).new(send_proc, close_proc, result_chan, -> { raw.status }, -> { raw.trailers }, -> { raw.cancel })\n"
        str << "#{indent}  end\n"
      when :bidi
        str << "#{indent}  # #{mname} opens a bidirectional-streaming RPC.\n"
        str << "#{indent}  # Send messages via the returned BidiCall, then call close_send to\n"
        str << "#{indent}  # signal end-of-input and iterate the replies with #each.\n"
        str << "#{indent}  def #{mname}(ctx : GRPC::ClientContext = GRPC::ClientContext.new) : GRPC::BidiCall(#{input}, #{output})\n"
        str << "#{indent}    raw = @channel.open_bidi_stream_live(#{sname}, #{meth.name.inspect}, ctx)\n"
        str << "#{indent}    req_marshaller = marshaller_for(#{input})\n"
        str << "#{indent}    res_marshaller = marshaller_for(#{output})\n"
        str << "#{indent}    recv_chan = ::Channel(#{output} | Exception).new(128)\n"
        str << "#{indent}    spawn do\n"
        str << "#{indent}      begin\n"
        str << "#{indent}        raw.each { |bytes| recv_chan.send(res_marshaller.load(bytes)) rescue nil }\n"
        str << "#{indent}        recv_chan.close\n"
        str << "#{indent}      rescue ex\n"
        str << "#{indent}        recv_chan.send(ex) rescue nil\n"
        str << "#{indent}        recv_chan.close rescue nil\n"
        str << "#{indent}      end\n"
        str << "#{indent}    end\n"
        str << "#{indent}    send_proc = Proc(#{input}, Nil).new { |msg| raw.send_raw(req_marshaller.dump(msg)) }\n"
        str << "#{indent}    close_proc = Proc(Nil).new { raw.close_send }\n"
        str << "#{indent}    GRPC::BidiCall(#{input}, #{output}).new(send_proc, close_proc, recv_chan, -> { raw.status }, -> { raw.trailers }, -> { raw.cancel })\n"
        str << "#{indent}  end\n"
      end
    end

    str << "#{indent}end\n"
  end

  # ---- CodeGeneratorResponse encoder ----

  private def encode_response(io : IO, files : Array({String, String})) : Nil
    # supported_features = 1 (FEATURE_PROTO3_OPTIONAL)
    ProtobufWireEncoder.varint(io, (2_u64 << 3) | 0)
    ProtobufWireEncoder.varint(io, 1_u64)

    files.each do |name, content|
      # field 15 (repeated File)
      ProtobufWireEncoder.message(io, 15) do |msg_io|
        ProtobufWireEncoder.string(msg_io, 1, name)     # File.name
        ProtobufWireEncoder.string(msg_io, 15, content) # File.content
      end
    end
  end

  # ---- Helpers ----

  # Parse plugin parameters and build a type-name resolver.
  #
  # Accepted forms:
  #   type_map=.foo.bar.HelloRequest=Foo::Bar::HelloRequest
  #   type_map=.a.A=A::A;.b.B=B::B
  #
  # Behavior:
  # - type_map unspecified: use grpc default resolver
  # - type_map specified: use strict map-backed resolver; unmapped entries are errors
  #
  # Multiple parameter tokens are separated by comma as per protoc plugin
  # conventions, while the type_map payload itself can contain ';' separators.
  private def build_type_name_resolver(parameter : String) : GRPC::Generator::TypeNameResolver
    return GRPC::Generator::CanonicalTypeNameResolver.new if parameter.empty?

    map = {} of String => String

    parameter.split(',').each do |token|
      key, value = split_once(token, '=')
      key = key.strip
      value = value.strip
      next if key.empty?

      case key
      when "type_map"
        next if value.empty?
        value.split(';').each do |entry|
          proto_name, crystal_name = split_once(entry, '=')
          proto_name = proto_name.strip
          crystal_name = crystal_name.strip
          next if proto_name.empty? || crystal_name.empty?
          normalized = proto_name.starts_with?('.') ? proto_name : ".#{proto_name}"
          map[normalized] = crystal_name
        end
      end
    end

    return GRPC::Generator::CanonicalTypeNameResolver.new if map.empty?

    GRPC::Generator::TypeMapNameResolver.new(map)
  end

  private def split_once(text : String, delimiter : Char) : {String, String}
    idx = text.index(delimiter)
    return {text, ""} unless idx
    {text[0, idx], text[idx + 1, text.bytesize - idx - 1]}
  end

  # Convert a proto package like "google.protobuf" to "Google::Protobuf".
  private def package_to_module(package : String) : String?
    return if package.empty?
    package.split('.').map(&.split('_').map(&.capitalize).join).join("::")
  end

  # Convert a proto full type name like ".helloworld.HelloRequest" to a
  # Crystal type expression relative to *current_package*.
  private def resolve_type(proto_type : String, current_package : String) : String
    @resolver.resolve(proto_type, current_package)
  end

  # "SayHello" → "say_hello", "GetFooBarBaz" → "get_foo_bar_baz"
  private def to_snake_case(name : String) : String
    name
      .gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
      .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
      .downcase
  end

  # Emit a streaming predicate method that returns true for the listed RPC names.
  private def emit_streaming_predicate(str : String::Builder, method_name : String,
                                       methods : Array(MethodDescriptor), indent : String) : Nil
    str << "\n"
    str << "#{indent}  def #{method_name}(method : String) : Bool\n"
    str << "#{indent}    case method\n"
    methods.each { |meth| str << "#{indent}    when #{meth.name.inspect} then true\n" }
    str << "#{indent}    else                       false\n"
    str << "#{indent}    end\n"
    str << "#{indent}  end\n"
  end

  private def full_service_name(fd : ProtoFileDescriptor, svc : ServiceDescriptor) : String
    fd.package.empty? ? svc.name : "#{fd.package}.#{svc.name}"
  end
end
