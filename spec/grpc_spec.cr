require "./spec_helper"

struct DummyMessage
  getter value : String

  def initialize(@value : String)
  end

  def encode : Bytes
    @value.to_slice
  end

  def self.decode(bytes : Bytes) : self
    new(String.new(bytes))
  end
end

class TestDummyMarshaller < GRPC::Marshaller(DummyMessage)
  def encode(value : DummyMessage) : Bytes
    ("out:" + value.value).to_slice
  end

  def decode(bytes : Bytes) : DummyMessage
    DummyMessage.new("in:" + String.new(bytes))
  end
end

class ReflectionRegistryProbeService < GRPC::Service
  SERVICE_FULL_NAME = "demo.Greeter"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    _ = method
    _ = request_body
    _ = ctx
    {Bytes.empty, GRPC::Status.ok}
  end
end

module ReflectionSpecWire
  LABEL_OPTIONAL = 1_u64
  TYPE_STRING    = 9_u64

  def self.encode_request(field_number : Int32, value : String) : Bytes
    io = IO::Memory.new
    GRPC::Reflection::Wire.write_string(io, field_number, value)
    io.to_slice
  end

  def self.decode_list_services_response(bytes : Bytes) : Array(String)
    decode_message_response(bytes, 6) do |payload|
      names = [] of String
      cursor = 0
      while cursor < payload.size
        tag, cursor = GRPC::Reflection::Wire.read_varint(payload, cursor)
        field_number = (tag >> 3).to_i32
        wire_type = (tag & 0x7_u64).to_i32
        if field_number == 1 && wire_type == 2
          entry, cursor = GRPC::Reflection::Wire.read_bytes(payload, cursor)
          names << decode_service_name(entry)
        else
          cursor = GRPC::Reflection::Wire.skip_field(payload, cursor, wire_type)
        end
      end
      names
    end
  end

  def self.decode_file_descriptor_response(bytes : Bytes) : Array(Bytes)
    decode_message_response(bytes, 4) do |payload|
      descriptors = [] of Bytes
      cursor = 0
      while cursor < payload.size
        tag, cursor = GRPC::Reflection::Wire.read_varint(payload, cursor)
        field_number = (tag >> 3).to_i32
        wire_type = (tag & 0x7_u64).to_i32
        if field_number == 1 && wire_type == 2
          descriptor, cursor = GRPC::Reflection::Wire.read_bytes(payload, cursor)
          descriptors << descriptor
        else
          cursor = GRPC::Reflection::Wire.skip_field(payload, cursor, wire_type)
        end
      end
      descriptors
    end
  end

  def self.build_demo_file_descriptor_proto(request_name : String = "HelloRequest", reply_name : String = "HelloReply") : Bytes
    io = IO::Memory.new
    GRPC::Reflection::Wire.write_string(io, 1, "demo.proto")
    GRPC::Reflection::Wire.write_string(io, 2, "demo")
    GRPC::Reflection::Wire.write_bytes(io, 4, build_string_message_descriptor(request_name))
    GRPC::Reflection::Wire.write_bytes(io, 4, build_string_message_descriptor(reply_name))
    GRPC::Reflection::Wire.write_bytes(io, 6, build_service_descriptor(request_name, reply_name))
    io.to_slice
  end

  def self.decode_error_response(bytes : Bytes) : {Int32, String}
    decode_message_response(bytes, 7) do |payload|
      code = 0
      message = ""
      cursor = 0
      while cursor < payload.size
        tag, cursor = GRPC::Reflection::Wire.read_varint(payload, cursor)
        field_number = (tag >> 3).to_i32
        wire_type = (tag & 0x7_u64).to_i32
        case field_number
        when 1
          if wire_type == 0
            value, cursor = GRPC::Reflection::Wire.read_varint(payload, cursor)
            code = value.to_i32
          else
            cursor = GRPC::Reflection::Wire.skip_field(payload, cursor, wire_type)
          end
        when 2
          if wire_type == 2
            message, cursor = GRPC::Reflection::Wire.read_string(payload, cursor)
          else
            cursor = GRPC::Reflection::Wire.skip_field(payload, cursor, wire_type)
          end
        else
          cursor = GRPC::Reflection::Wire.skip_field(payload, cursor, wire_type)
        end
      end
      {code, message}
    end
  end

  private def self.decode_message_response(bytes : Bytes, response_field_number : Int32, & : Bytes -> T) : T forall T
    cursor = 0

    while cursor < bytes.size
      tag, cursor = GRPC::Reflection::Wire.read_varint(bytes, cursor)
      field_number = (tag >> 3).to_i32
      wire_type = (tag & 0x7_u64).to_i32
      if field_number == response_field_number && wire_type == 2
        payload, cursor = GRPC::Reflection::Wire.read_bytes(bytes, cursor)
        return yield payload
      end
      cursor = GRPC::Reflection::Wire.skip_field(bytes, cursor, wire_type)
    end

    raise "missing reflection response payload"
  end

  private def self.decode_service_name(bytes : Bytes) : String
    cursor = 0
    while cursor < bytes.size
      tag, cursor = GRPC::Reflection::Wire.read_varint(bytes, cursor)
      field_number = (tag >> 3).to_i32
      wire_type = (tag & 0x7_u64).to_i32
      if field_number == 1 && wire_type == 2
        name, cursor = GRPC::Reflection::Wire.read_string(bytes, cursor)
        return name
      end
      cursor = GRPC::Reflection::Wire.skip_field(bytes, cursor, wire_type)
    end
    raise "missing service name"
  end

  private def self.build_string_message_descriptor(name : String) : Bytes
    io = IO::Memory.new
    GRPC::Reflection::Wire.write_string(io, 1, name)
    GRPC::Reflection::Wire.write_bytes(io, 2, build_string_field_descriptor)
    io.to_slice
  end

  private def self.build_string_field_descriptor : Bytes
    io = IO::Memory.new
    GRPC::Reflection::Wire.write_string(io, 1, "message")
    GRPC::Reflection::Wire.write_varint_field(io, 3, 1_u64)
    GRPC::Reflection::Wire.write_varint_field(io, 4, LABEL_OPTIONAL)
    GRPC::Reflection::Wire.write_varint_field(io, 5, TYPE_STRING)
    io.to_slice
  end

  private def self.build_service_descriptor(request_name : String, reply_name : String) : Bytes
    io = IO::Memory.new
    GRPC::Reflection::Wire.write_string(io, 1, "Greeter")
    GRPC::Reflection::Wire.write_bytes(io, 2, build_method_descriptor(request_name, reply_name))
    io.to_slice
  end

  private def self.build_method_descriptor(request_name : String, reply_name : String) : Bytes
    io = IO::Memory.new
    GRPC::Reflection::Wire.write_string(io, 1, "SayHello")
    GRPC::Reflection::Wire.write_string(io, 2, ".demo.#{request_name}")
    GRPC::Reflection::Wire.write_string(io, 3, ".demo.#{reply_name}")
    io.to_slice
  end
end

describe GRPC do
  describe GRPC::StatusCode do
    it "has standard gRPC status codes" do
      GRPC::StatusCode::OK.value.should eq(0)
      GRPC::StatusCode::CANCELLED.value.should eq(1)
      GRPC::StatusCode::UNIMPLEMENTED.value.should eq(12)
    end
  end

  describe GRPC::Status do
    it "creates OK status" do
      s = GRPC::Status.ok
      s.ok?.should be_true
      s.code.should eq(GRPC::StatusCode::OK)
    end

    it "creates error status" do
      s = GRPC::Status.new(GRPC::StatusCode::NOT_FOUND, "resource not found")
      s.ok?.should be_false
      s.message.should eq("resource not found")
    end

    it "can carry optional status details bytes" do
      details = Bytes[0x01, 0x02, 0x03]
      s = GRPC::Status.new(GRPC::StatusCode::INTERNAL, "boom", details)

      s.details.should eq(details)
    end
  end

  describe GRPC::Metadata do
    it "sets and gets values" do
      m = GRPC::Metadata.new
      m.set("x-request-id", "abc123")
      m.get("x-request-id").should eq("abc123")
    end

    it "normalises keys to lowercase" do
      m = GRPC::Metadata.new
      m.set("X-Custom-Header", "value")
      m.get("x-custom-header").should eq("value")
    end

    it "accumulates multiple values" do
      m = GRPC::Metadata.new
      m.add("x-tag", "a")
      m.add("x-tag", "b")
      m.get_all("x-tag").should eq(["a", "b"])
    end

    it "can be constructed from a Hash" do
      m = GRPC::Metadata.new({"x-foo" => "bar"})
      m.get("x-foo").should eq("bar")
    end

    it "stores binary metadata separately from text metadata" do
      m = GRPC::Metadata.new
      m.add_bin("trace-bin", Bytes[1, 2, 3])

      m.get("trace-bin").should be_nil
      m.get_bin("trace-bin").should eq(Bytes[1, 2, 3])
      m.to_h["trace-bin"].should eq("AQID")
    end

    it "decodes wire-format binary metadata" do
      m = GRPC::Metadata.new
      m.add_wire("trace-bin", "AQID")

      m.get_bin("trace-bin").should eq(Bytes[1, 2, 3])
    end

    it "merge! duplicates binary metadata values" do
      source = GRPC::Metadata.new
      bytes = Bytes[1, 2, 3]
      source.add_bin("trace-bin", bytes)

      merged = GRPC::Metadata.new
      merged.merge!(source)
      bytes[0] = 9

      source.get_bin("trace-bin").should eq(Bytes[1, 2, 3])
      merged.get_bin("trace-bin").should eq(Bytes[1, 2, 3])

      source_bin = source.get_bin("trace-bin")
      merged_bin = merged.get_bin("trace-bin")
      if source_bin && merged_bin
        merged_bin.should_not be(source_bin)
      end
    end

    it "dup duplicates binary metadata values" do
      original = GRPC::Metadata.new
      bytes = Bytes[4, 5, 6]
      original.add_bin("trace-bin", bytes)

      copy = original.dup
      bytes[0] = 9

      original.get_bin("trace-bin").should eq(Bytes[4, 5, 6])
      copy.get_bin("trace-bin").should eq(Bytes[4, 5, 6])

      original_bin = original.get_bin("trace-bin")
      copy_bin = copy.get_bin("trace-bin")
      if original_bin && copy_bin
        copy_bin.should_not be(original_bin)
      end
    end

    it "rejects text values for binary metadata keys" do
      m = GRPC::Metadata.new

      expect_raises(ArgumentError) do
        m.add("trace-bin", "AQID")
      end
    end

    it "rejects binary values for text metadata keys" do
      m = GRPC::Metadata.new

      expect_raises(ArgumentError) do
        m.add_bin("trace", Bytes[1, 2, 3])
      end
    end

    it "rejects invalid wire-format binary metadata" do
      m = GRPC::Metadata.new

      expect_raises(ArgumentError) do
        m.add_wire("trace-bin", "!!!")
      end
    end
  end

  describe GRPC::Endpoint do
    it "parses plain host and port" do
      endpoint = GRPC::Endpoint.parse("example.com:1234")
      endpoint.host.should eq("example.com")
      endpoint.port.should eq(1234)
      endpoint.tls?.should be_false
    end

    it "parses scheme-specific default ports" do
      plain = GRPC::Endpoint.parse("http://example.com")
      plain.host.should eq("example.com")
      plain.port.should eq(50051)
      plain.tls?.should be_false

      tls = GRPC::Endpoint.parse("https://example.com")
      tls.host.should eq("example.com")
      tls.port.should eq(443)
      tls.tls?.should be_true
    end

    it "strips trailing path components" do
      endpoint = GRPC::Endpoint.parse("https://example.com:8443/foo/bar")
      endpoint.host.should eq("example.com")
      endpoint.port.should eq(8443)
      endpoint.tls?.should be_true
    end

    it "parses bracketed IPv6 endpoints with and without explicit port" do
      endpoint = GRPC::Endpoint.parse("http://[::1]:8080")
      endpoint.host.should eq("[::1]")
      endpoint.port.should eq(8080)
      endpoint.tls?.should be_false

      tls_endpoint = GRPC::Endpoint.parse("https://[::1]")
      tls_endpoint.host.should eq("[::1]")
      tls_endpoint.port.should eq(443)
      tls_endpoint.tls?.should be_true
    end

    it "renders addresses with the correct scheme" do
      GRPC::Endpoint.new("example.com", 50051).to_address.should eq("http://example.com:50051")
      GRPC::Endpoint.new("example.com", 443, true).to_address.should eq("https://example.com:443")
    end
  end

  describe GRPC::Codec do
    it "round-trips a message" do
      original = "Hello, gRPC!".to_slice
      framed = GRPC::Codec.encode(original)
      framed.size.should eq(original.size + GRPC::Codec::HEADER_SIZE)

      decoded, consumed = GRPC::Codec.decode(framed)
      decoded.should eq(original)
      consumed.should eq(framed.size)
    end

    it "decodes multiple messages" do
      msgs = ["foo", "bar", "baz"].map(&.to_slice)
      data = msgs.reduce(Bytes.empty) { |acc, msg| acc + GRPC::Codec.encode(msg) }
      result = GRPC::Codec.decode_all(data)
      result.size.should eq(3)
      result.map { |bytes| String.new(bytes) }.should eq(["foo", "bar", "baz"])
    end

    it "round-trips a gzip-compressed message" do
      original = "Hello, compressed gRPC!".to_slice
      framed = GRPC::Codec.encode(original, compress: true)
      framed[0].should eq(1_u8) # compression flag set

      decoded, consumed = GRPC::Codec.decode(framed)
      decoded.should eq(original)
      consumed.should eq(framed.size)
    end

    it "transparently decodes mixed compressed/uncompressed frames" do
      plain = GRPC::Codec.encode("hello".to_slice)
      compressed = GRPC::Codec.encode("world".to_slice, compress: true)
      data = plain + compressed
      result = GRPC::Codec.decode_all(data)
      result.size.should eq(2)
      String.new(result[0]).should eq("hello")
      String.new(result[1]).should eq("world")
    end

    it "raises on unknown compression flag" do
      # Flag 0x02 is not a recognised compression algorithm
      bad_frame = Bytes[0x02, 0x00, 0x00, 0x00, 0x00]
      expect_raises(GRPC::StatusError) do
        GRPC::Codec.decode(bad_frame)
      end
    end
  end

  describe GRPC::Marshaller do
    it "decodes request stream messages via a custom marshaller" do
      ch = ::Channel(Bytes?).new(2)
      ch.send("A".to_slice)
      ch.send("B".to_slice)
      ch.close

      stream = GRPC::RequestStream(DummyMessage).new(ch, TestDummyMarshaller.new)
      values = [] of DummyMessage
      stream.each { |v| values << v }

      values.map(&.value).should eq(["in:A", "in:B"])
    end

    it "encodes response stream messages via a custom marshaller" do
      sent = [] of Bytes
      raw = GRPC::RawResponseStream.new(->(bytes : Bytes) { sent << bytes; nil })
      stream = GRPC::ResponseStream(DummyMessage).new(raw, TestDummyMarshaller.new)

      stream.send(DummyMessage.new("hello"))

      body, _consumed = GRPC::Codec.decode(sent[0])
      String.new(body).should eq("out:hello")
    end

    it "decodes envelopes via a custom marshaller" do
      info = GRPC::CallInfo.new("/svc/m", GRPC::RPCKind::Unary)
      req = GRPC::RequestEnvelope.new(info, "xyz".to_slice)
      res = GRPC::ResponseEnvelope.new(info, "xyz".to_slice, GRPC::Status.ok)

      req.decode(DummyMessage, TestDummyMarshaller.new).value.should eq("in:xyz")
      res.decode(DummyMessage, TestDummyMarshaller.new).value.should eq("in:xyz")
    end
  end

  describe GRPC::StatusError do
    it "exposes code and message" do
      ex = GRPC::StatusError.new(GRPC::StatusCode::NOT_FOUND, "missing")
      ex.code.should eq(GRPC::StatusCode::NOT_FOUND)
      ex.message.should eq("missing")
    end

    it "can be constructed from a Status" do
      status = GRPC::Status.new(GRPC::StatusCode::INTERNAL, "boom")
      ex = GRPC::StatusError.new(status)
      ex.status.should eq(status)
    end

    it "retains trailers when provided" do
      trailers = GRPC::Metadata.new
      trailers.add("x-extra", "value")

      ex = GRPC::StatusError.new(GRPC::Status.internal("boom"), trailers)

      ex.trailers.get("x-extra").should eq("value")
    end
  end

  describe GRPC::Transport::LiveSendBuffer do
    it "unbounded mode: push never blocks" do
      buf = GRPC::Transport::LiveSendBuffer.new(0)
      100.times { |i| buf.push(Bytes[i.to_u8]) }
    end

    it "bounded mode: blocks when full, unblocks when drained" do
      buf = GRPC::Transport::LiveSendBuffer.new(2)

      # Fill queue to capacity without blocking
      buf.push(Bytes[1_u8])
      buf.push(Bytes[2_u8])

      unblocked = false

      # Third push should block until read_into drains one entry
      spawn do
        buf.push(Bytes[3_u8])
        unblocked = true
      end

      Fiber.yield

      # The spawned fiber should still be blocked
      unblocked.should be_false

      # Simulate DATA_READ_CB_LIVE draining one chunk via read_into
      raw_buf = Bytes.new(64)
      flags = 0_u32
      buf.read_into(raw_buf.to_unsafe, 64_u64, pointerof(flags))

      Fiber.yield

      # Now the slot is free and the push should have completed
      unblocked.should be_true
    end

    it "bounded mode: close after full push unblocks reads with EOF" do
      buf = GRPC::Transport::LiveSendBuffer.new(1)
      buf.push(Bytes[42_u8])
      buf.close

      raw_buf = Bytes.new(64)
      flags = 0_u32
      bytes_read = buf.read_into(raw_buf.to_unsafe, 64_u64, pointerof(flags))
      bytes_read.should eq(1)

      # Second call: queue empty and closed → EOF
      flags2 = 0_u32
      buf.read_into(raw_buf.to_unsafe, 64_u64, pointerof(flags2))
      (flags2 & LibNghttp2::DATA_FLAG_EOF).should_not eq(0)
    end
  end

  describe GRPC::Transport::GrpcDeframer do
    it "drains only complete frames and preserves remainder" do
      buffer = GRPC::Transport::GrpcDeframer.new
      msg1 = GRPC::Codec.encode("hello".to_slice)
      msg2 = GRPC::Codec.encode("world".to_slice)

      partial = msg2[0, 3]
      rest = msg2[3, msg2.size - 3]

      buffer.append(msg1 + partial)
      drained = buffer.drain_messages
      drained.size.should eq(1)
      String.new(drained[0]).should eq("hello")
      buffer.remainder_size.should eq(3)

      buffer.append(rest)
      drained2 = buffer.drain_messages
      drained2.size.should eq(1)
      String.new(drained2[0]).should eq("world")
      buffer.remainder_size.should eq(0)
    end

    it "parses a frame header split across multiple chunks" do
      buffer = GRPC::Transport::GrpcDeframer.new
      frame = GRPC::Codec.encode("header-boundary".to_slice)

      buffer.append(frame[0, 2])
      buffer.drain_messages.should be_empty
      buffer.append(frame[2, 2])
      buffer.drain_messages.should be_empty
      buffer.append(frame[4, frame.size - 4])

      drained = buffer.drain_messages
      drained.size.should eq(1)
      String.new(drained[0]).should eq("header-boundary")
      buffer.remainder_size.should eq(0)
    end

    it "handles one-byte fragmented delivery without losing message boundaries" do
      buffer = GRPC::Transport::GrpcDeframer.new
      payload = "x" * 16_384
      frame = GRPC::Codec.encode(payload.to_slice)
      drained = [] of Bytes

      frame.each do |byte|
        chunk = Bytes.new(1)
        chunk[0] = byte
        buffer.append(chunk)
        drained.concat(buffer.drain_messages)
      end

      drained.size.should eq(1)
      String.new(drained[0]).should eq(payload)
      buffer.remainder_size.should eq(0)
    end

    it "decodes multiple frames split across uneven chunk sizes" do
      buffer = GRPC::Transport::GrpcDeframer.new
      msg1 = GRPC::Codec.encode("alpha".to_slice)
      msg2 = GRPC::Codec.encode("beta".to_slice)
      msg3 = GRPC::Codec.encode("gamma".to_slice)
      wire = msg1 + msg2 + msg3

      drained = [] of Bytes
      chunk_sizes = [2, 7, 1, 11, 3, 5] of Int32
      offset = 0
      index = 0

      while offset < wire.size
        requested = chunk_sizes[index % chunk_sizes.size]
        size = Math.min(requested, wire.size - offset)
        buffer.append(wire[offset, size])
        drained.concat(buffer.drain_messages)
        offset += size
        index += 1
      end

      drained.map { |bytes| String.new(bytes) }.should eq(["alpha", "beta", "gamma"])
      buffer.remainder_size.should eq(0)
    end

    it "decodes compressed frames split across uneven chunks" do
      buffer = GRPC::Transport::GrpcDeframer.new
      frame = GRPC::Codec.encode("compressed payload".to_slice, compress: true)

      buffer.append(frame[0, 3])
      buffer.drain_messages.should be_empty
      buffer.append(frame[3, 4])
      buffer.drain_messages.should be_empty
      buffer.append(frame[7, frame.size - 7])

      drained = buffer.drain_messages
      drained.size.should eq(1)
      String.new(drained[0]).should eq("compressed payload")
      buffer.remainder_size.should eq(0)
    end
  end

  describe GRPC::Transport::GrpcStatusInterpreter do
    it "prefers trailers and decodes grpc-message" do
      headers = GRPC::Metadata.new
      trailers = GRPC::Metadata.new
      trailers.add("grpc-status", "3")
      trailers.add("grpc-message", "invalid%20input")

      status = GRPC::Transport::GrpcStatusInterpreter.grpc_status(headers, trailers, nil)
      status.code.should eq(GRPC::StatusCode::INVALID_ARGUMENT)
      status.message.should eq("invalid input")
    end

    it "uses override when grpc-status is missing" do
      headers = GRPC::Metadata.new
      trailers = GRPC::Metadata.new
      override = GRPC::Status.internal("transport failure")

      status = GRPC::Transport::GrpcStatusInterpreter.grpc_status(headers, trailers, override)
      status.should eq(override)
    end

    it "prefers override over grpc-status when transport failed" do
      headers = GRPC::Metadata.new
      trailers = GRPC::Metadata.new
      trailers.add("grpc-status", "0")
      override = GRPC::Status.internal("transport failure")

      status = GRPC::Transport::GrpcStatusInterpreter.grpc_status(headers, trailers, override)
      status.should eq(override)
    end
  end

  describe GRPC::Transport::StreamHeaderState do
    it "routes first headers block to headers and next block to trailers" do
      state = GRPC::Transport::StreamHeaderState.new

      state.begin_header_block
      state.add_header(":status", "200")
      state.add_header("content-type", "application/grpc")

      state.begin_header_block
      state.add_header("grpc-status", "0")

      state.headers.get(":status").should eq("200")
      state.trailers.get("grpc-status").should eq("0")
    end
  end

  describe GRPC::Transport::StreamTerminalState do
    it "allows finish/cancel transitions only once" do
      lifecycle = GRPC::Transport::StreamTerminalState.new

      lifecycle.mark_finished.should be_true
      lifecycle.mark_finished.should be_false
      lifecycle.mark_cancelled.should be_true
      lifecycle.mark_cancelled.should be_false
    end
  end

  describe GRPC::RawServerStream do
    it "runs finish hooks once after iteration completes" do
      ch = ::Channel(Bytes?).new(2)
      ch.send(Bytes[1_u8])
      ch.close

      finishes = 0
      stream = GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok }).with_on_finish(-> { finishes += 1; nil })

      received = [] of Bytes
      stream.each { |msg| received << msg }

      received.should eq([Bytes[1_u8]])
      finishes.should eq(1)
    end

    it "runs finish hooks once across repeated cancel calls" do
      ch = ::Channel(Bytes?).new(1)
      cancels = 0
      finishes = 0
      stream = GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok }, -> { GRPC::Metadata.new }, -> { cancels += 1; nil }).with_on_finish(-> { finishes += 1; nil })

      stream.cancel
      stream.cancel

      cancels.should eq(2)
      finishes.should eq(1)
    end
  end

  describe GRPC::RawClientCall do
    it "runs finish hooks once even if cancel follows close_and_recv" do
      finishes = 0
      call = GRPC::RawClientCall.new(
        ->(_b : Bytes) { },
        -> { Bytes[7_u8] },
        -> { GRPC::Metadata.new },
        -> { GRPC::Status.ok },
        -> { GRPC::Metadata.new },
        -> { }
      ).with_on_finish(-> { finishes += 1; nil })

      call.close_and_recv.should eq(Bytes[7_u8])
      call.cancel

      finishes.should eq(1)
    end
  end

  describe GRPC::RawBidiCall do
    it "runs finish hooks once even if cancel follows iteration" do
      ch = ::Channel(Bytes?).new(2)
      ch.send(Bytes[9_u8])
      ch.close

      finishes = 0
      call = GRPC::RawBidiCall.new(
        ->(_b : Bytes) { },
        -> { },
        ch,
        -> { GRPC::Metadata.new },
        -> { GRPC::Status.ok },
        -> { GRPC::Metadata.new },
        -> { }
      ).with_on_finish(-> { finishes += 1; nil })

      received = [] of Bytes
      call.each { |msg| received << msg }
      call.cancel

      received.should eq([Bytes[9_u8]])
      finishes.should eq(1)
    end
  end

  describe GRPC::ServerContext do
    it "exposes peer and metadata" do
      meta = GRPC::Metadata.new({"x-foo" => "bar"})
      ctx = GRPC::ServerContext.new("127.0.0.1:12345", meta)
      ctx.peer.should eq("127.0.0.1:12345")
      ctx.metadata.get("x-foo").should eq("bar")
    end

    it "tracks cancellation" do
      ctx = GRPC::ServerContext.new("peer")
      ctx.cancelled?.should be_false
      ctx.cancel
      ctx.cancelled?.should be_true
    end

    it "detects deadline expiry" do
      ctx = GRPC::ServerContext.new("peer", GRPC::Metadata.new, Time.utc - 1.second)
      ctx.timed_out?.should be_true
    end

    it "reports no timeout when deadline is in the future" do
      ctx = GRPC::ServerContext.new("peer", GRPC::Metadata.new, Time.utc + 60.seconds)
      ctx.timed_out?.should be_false
    end
  end

  describe GRPC::ClientContext do
    it "holds metadata from a hash" do
      ctx = GRPC::ClientContext.new(metadata: {"authorization" => "Bearer token"})
      ctx.metadata.get("authorization").should eq("Bearer token")
    end

    it "converts a span deadline to absolute time" do
      before = Time.utc
      ctx = GRPC::ClientContext.new(deadline: 5.seconds)
      after = Time.utc
      dl = ctx.deadline.as(Time)
      dl.should be >= (before + 5.seconds)
      dl.should be <= (after + 5.seconds)
    end

    it "includes grpc-timeout in effective_metadata when deadline is set" do
      ctx = GRPC::ClientContext.new(deadline: 10.seconds)
      meta = ctx.effective_metadata
      meta.get("grpc-timeout").should_not be_nil
    end

    it "does not add grpc-timeout when no deadline is set" do
      ctx = GRPC::ClientContext.new
      meta = ctx.effective_metadata
      meta.get("grpc-timeout").should be_nil
    end

    it "reports remaining time before deadline" do
      ctx = GRPC::ClientContext.new(deadline: 10.seconds)
      ctx.remaining.should_not be_nil
    end

    it "returns nil remaining when no deadline" do
      ctx = GRPC::ClientContext.new
      ctx.remaining.should be_nil
    end
  end

  describe GRPC::Health::Service do
    it "returns SERVING for empty service by default" do
      service = GRPC::Health::Service.new
      ctx = GRPC::ServerContext.new("peer")
      request = Grpc::Health::V1::HealthCheckRequest.new

      body, status = service.dispatch("Check", request.encode, ctx)
      status.should eq(GRPC::Status.ok)
      response = Grpc::Health::V1::HealthCheckResponse.decode(body)
      GRPC::Health.serving_status_from_wire(response.status).should eq(Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVING)
    end

    it "returns NOT_FOUND when named service has no status" do
      service = GRPC::Health::Service.new
      ctx = GRPC::ServerContext.new("peer")
      req = Grpc::Health::V1::HealthCheckRequest.new
      req.service = "my.service"

      body, status = service.dispatch("Check", req.encode, ctx)
      body.should be_empty
      status.should eq(GRPC::Status.not_found("unknown service my.service"))
    end

    it "returns configured status for named service" do
      service = GRPC::Health::Service.new
      service.reporter.set_status("my.service", Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)
      ctx = GRPC::ServerContext.new("peer")
      req = Grpc::Health::V1::HealthCheckRequest.new
      req.service = "my.service"

      body, status = service.dispatch("Check", req.encode, ctx)
      status.should eq(GRPC::Status.ok)
      response = Grpc::Health::V1::HealthCheckResponse.decode(body)
      GRPC::Health.serving_status_from_wire(response.status).should eq(Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)
    end

    it "treats Watch as server-streaming" do
      service = GRPC::Health::Service.new
      service.server_streaming?("Watch").should be_true
      service.server_streaming?("Check").should be_false
    end

    it "Watch sends initial unknown status, then updates, until cancelled" do
      service = GRPC::Health::Service.new
      ctx = GRPC::ServerContext.new("peer")
      req = Grpc::Health::V1::HealthCheckRequest.new
      req.service = "my.service"
      sent = ::Channel(Bytes).new(2)
      writer = GRPC::RawResponseStream.new(->(bytes : Bytes) { sent.send(bytes); nil })
      done = ::Channel(GRPC::Status).new(1)

      spawn do
        done.send(service.dispatch_server_stream("Watch", req.encode, ctx, writer))
      end

      first_frame = sent.receive
      body, consumed = GRPC::Codec.decode(first_frame)
      consumed.should eq(first_frame.size)
      response = Grpc::Health::V1::HealthCheckResponse.decode(body)
      GRPC::Health.serving_status_from_wire(response.status).should eq(Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVICE_UNKNOWN)

      service.reporter.set_status("my.service", Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)

      second_frame = sent.receive
      body, consumed = GRPC::Codec.decode(second_frame)
      consumed.should eq(second_frame.size)
      response = Grpc::Health::V1::HealthCheckResponse.decode(body)
      GRPC::Health.serving_status_from_wire(response.status).should eq(Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)

      ctx.cancel
      done.receive.code.should eq(GRPC::StatusCode::CANCELLED)
    end

    it "lists all currently known health statuses" do
      service = GRPC::Health::Service.new
      service.reporter.set_status("my.service", Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)
      ctx = GRPC::ServerContext.new("peer")
      request = Grpc::Health::V1::HealthListRequest.new

      body, status = service.dispatch("List", request.encode, ctx)

      status.should eq(GRPC::Status.ok)
      response = Grpc::Health::V1::HealthListResponse.decode(body)
      statuses = response.statuses.transform_values do |value|
        GRPC::Health.serving_status_from_wire(value.status)
      end
      statuses.should eq({
        ""           => Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVING,
        "my.service" => Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING,
      })
    end

    describe GRPC::Reflection::Service do
      it "lists registered services" do
        service = GRPC::Reflection::Service.new
        service.register_service("demo.Greeter")
        sent = [] of Bytes
        writer = GRPC::RawResponseStream.new(->(framed : Bytes) {
          body, _ = GRPC::Codec.decode(framed)
          sent << body
          nil
        })
        requests = ::Channel(Bytes?).new(1)
        requests.send(ReflectionSpecWire.encode_request(7, ""))
        requests.close

        status = service.dispatch_bidi_stream(
          "ServerReflectionInfo",
          GRPC::RawRequestStream.new(requests),
          GRPC::ServerContext.new("peer", GRPC::Metadata.new),
          writer,
        )

        status.ok?.should be_true
        ReflectionSpecWire.decode_list_services_response(sent[0]).should eq([
          "demo.Greeter",
          GRPC::Reflection::Service::SERVICE_FULL_NAME,
        ])
      end

      it "returns the matching file descriptor for symbol lookups" do
        descriptor = ReflectionSpecWire.build_demo_file_descriptor_proto
        service = GRPC::Reflection::Service.new
        service.register_service("demo.Greeter")
        service.add_file_descriptor(descriptor)

        sent = [] of Bytes
        writer = GRPC::RawResponseStream.new(->(framed : Bytes) {
          body, _ = GRPC::Codec.decode(framed)
          sent << body
          nil
        })
        requests = ::Channel(Bytes?).new(1)
        requests.send(ReflectionSpecWire.encode_request(4, "demo.HelloRequest"))
        requests.close

        status = service.dispatch_bidi_stream(
          "ServerReflectionInfo",
          GRPC::RawRequestStream.new(requests),
          GRPC::ServerContext.new("peer", GRPC::Metadata.new),
          writer,
        )

        status.ok?.should be_true
        ReflectionSpecWire.decode_file_descriptor_response(sent[0]).should eq([descriptor])
      end

      it "replaces symbol mappings when a descriptor file is re-registered" do
        original_descriptor = ReflectionSpecWire.build_demo_file_descriptor_proto
        replacement_descriptor = ReflectionSpecWire.build_demo_file_descriptor_proto(
          request_name: "RenamedRequest",
          reply_name: "RenamedReply",
        )
        service = GRPC::Reflection::Service.new
        service.register_service("demo.Greeter")
        service.add_file_descriptor(original_descriptor)
        service.add_file_descriptor(replacement_descriptor)

        sent = [] of Bytes
        writer = GRPC::RawResponseStream.new(->(framed : Bytes) {
          body, _ = GRPC::Codec.decode(framed)
          sent << body
          nil
        })
        requests = ::Channel(Bytes?).new(2)
        requests.send(ReflectionSpecWire.encode_request(4, "demo.HelloRequest"))
        requests.send(ReflectionSpecWire.encode_request(4, "demo.RenamedRequest"))
        requests.close

        status = service.dispatch_bidi_stream(
          "ServerReflectionInfo",
          GRPC::RawRequestStream.new(requests),
          GRPC::ServerContext.new("peer", GRPC::Metadata.new),
          writer,
        )

        status.ok?.should be_true
        ReflectionSpecWire.decode_error_response(sent[0]).should eq({5, "symbol not found: demo.HelloRequest"})
        ReflectionSpecWire.decode_file_descriptor_response(sent[1]).should eq([replacement_descriptor])
      end

      it "server registers existing and later services with reflection" do
        server = GRPC::Server.new
        server.handle(ReflectionRegistryProbeService.new)

        reflection = server.enable_reflection

        reflection.register_service("extra.Debug")

        sent = [] of Bytes
        writer = GRPC::RawResponseStream.new(->(framed : Bytes) {
          body, _ = GRPC::Codec.decode(framed)
          sent << body
          nil
        })
        requests = ::Channel(Bytes?).new(1)
        requests.send(ReflectionSpecWire.encode_request(7, ""))
        requests.close

        status = reflection.dispatch_bidi_stream(
          "ServerReflectionInfo",
          GRPC::RawRequestStream.new(requests),
          GRPC::ServerContext.new("peer", GRPC::Metadata.new),
          writer,
        )

        status.ok?.should be_true
        ReflectionSpecWire.decode_list_services_response(sent[0]).should contain("demo.Greeter")
        ReflectionSpecWire.decode_list_services_response(sent[0]).should contain("extra.Debug")
      end
    end
  end

  describe GRPC::Server do
    it "returns a health reporter" do
      server = GRPC::Server.new

      reporter = server.enable_health_checking
      reporter.should be_a(GRPC::Health::Reporter)
    end

    it "reuses existing health reporter on repeated calls" do
      server = GRPC::Server.new

      first = server.enable_health_checking
      second = server.enable_health_checking(default_status: Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)

      second.should be(first)
    end

    it "pushes NOT_SERVING to active health watchers on stop" do
      server = GRPC::Server.new
      reporter = server.enable_health_checking
      reporter.set_status("my.service", Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVING)
      port_probe = TCPServer.new("127.0.0.1", 0)
      port = port_probe.local_address.as(Socket::IPAddress).port
      port_probe.close
      server.bind("127.0.0.1:#{port}")
      server.start

      channel = GRPC::Channel.new("127.0.0.1:#{port}")
      client = Grpc::Health::V1::Health::Client.new(channel)
      request = Grpc::Health::V1::HealthCheckRequest.new
      request.service = "my.service"
      stream = client.watch(request)

      statuses = ::Channel(Grpc::Health::V1::HealthCheckResponse::ServingStatus).new(2)
      done = ::Channel(Nil).new(1)

      spawn do
        stream.each do |message|
          statuses.send(GRPC::Health.serving_status_from_wire(message.status))
        end
      ensure
        done.send(nil)
      end

      statuses.receive.should eq(Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVING)

      server.stop
      statuses.receive.should eq(Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)
      done.receive
    ensure
      stream.try &.cancel
      channel.try &.close
    end
  end

  describe GRPC::Transport::PendingCall do
    it "decodes grpc-status-details-bin from trailers" do
      call = GRPC::Transport::PendingCall.new
      call.trailers.add("grpc-status", GRPC::StatusCode::INVALID_ARGUMENT.value.to_s)
      call.trailers.add("grpc-message", "bad%20request")
      call.trailers.add_bin("grpc-status-details-bin", Bytes[0x0A, 0x01, 0x7F])

      status = call.grpc_status
      status.code.should eq(GRPC::StatusCode::INVALID_ARGUMENT)
      status.message.should eq("bad request")
      status.details.should eq(Bytes[0x0A, 0x01, 0x7F])
    end

    it "separates initial headers from trailing metadata" do
      call = GRPC::Transport::PendingCall.new
      call.begin_header_block
      call.add_header(":status", "200")
      call.add_header("x-initial", "one")
      call.begin_header_block
      call.add_header("grpc-status", "0")
      call.add_header("x-trailer", "done")

      call.grpc_headers.get("x-initial").should eq("one")
      call.grpc_headers.get("x-trailer").should be_nil
      call.grpc_trailers.get("x-trailer").should eq("done")
    end

    it "treats missing grpc-status as UNKNOWN" do
      call = GRPC::Transport::PendingCall.new

      status = call.grpc_status
      status.code.should eq(GRPC::StatusCode::UNKNOWN)
      status.message.should contain("missing grpc-status")
    end

    it "prefers transport error override over grpc-status when trailers are present" do
      call = GRPC::Transport::PendingCall.new
      call.trailers.add("grpc-status", "0")
      call.transport_error = GRPC::Status.unknown("transport close")

      call.grpc_status.code.should eq(GRPC::StatusCode::UNKNOWN)
      call.grpc_status.message.should eq("transport close")
    end

    it "keeps invalid response metadata as the final status even with grpc-status" do
      call = GRPC::Transport::PendingCall.new
      call.begin_header_block
      call.add_header("trace-bin", "!!!")
      call.begin_header_block
      call.add_header("grpc-status", "0")

      status = call.grpc_status
      status.code.should eq(GRPC::StatusCode::INTERNAL)
      status.message.should contain("invalid wire metadata")
    end

    it "ignores transport errors reported after completion" do
      call = GRPC::Transport::PendingCall.new
      call.trailers.add("grpc-status", "0")

      call.complete
      call.transport_error = GRPC::Status.unknown("late close")

      call.grpc_status.code.should eq(GRPC::StatusCode::OK)
    end

    it "complete is idempotent" do
      call = GRPC::Transport::PendingCall.new

      call.complete
      call.complete

      call.wait
    end
  end

  describe GRPC::UnaryResponse do
    it "raises status errors with trailing metadata from ok!" do
      trailers = GRPC::Metadata.new
      trailers.add("x-extra", "value")
      response = GRPC::UnaryResponse(String).new(nil, GRPC::Metadata.new, trailers, GRPC::Status.internal("boom"))

      ex = expect_raises(GRPC::StatusError) { response.ok! }
      ex.trailers.get("x-extra").should eq("value")
    end
  end

  describe GRPC::Transport::PendingStream do
    it "keeps grpc status trailers out of metadata" do
      stream = GRPC::Transport::PendingStream.new
      stream.trailers.add("grpc-status", "13")
      stream.trailers.add("grpc-message", "boom")
      stream.trailers.add_bin("grpc-status-details-bin", Bytes[0x01])
      stream.trailers.add("x-extra", "ok")
      stream.trailers.add("x-extra", "second")

      trailers = stream.grpc_trailers
      trailers.get("grpc-status").should be_nil
      trailers.get("grpc-message").should be_nil
      trailers.get("grpc-status-details-bin").should be_nil
      trailers.get_all("x-extra").should eq(["ok", "second"])
    end

    it "tracks initial headers separately from trailing metadata" do
      stream = GRPC::Transport::PendingStream.new
      stream.begin_header_block
      stream.add_header(":status", "200")
      stream.add_header("x-initial", "one")
      stream.begin_header_block
      stream.add_header("grpc-status", "0")
      stream.add_header("x-trailer", "done")

      stream.grpc_headers.get("x-initial").should eq("one")
      stream.grpc_headers.get("x-trailer").should be_nil
      stream.grpc_trailers.get("x-trailer").should eq("done")
    end

    it "treats missing grpc-status as UNKNOWN" do
      stream = GRPC::Transport::PendingStream.new

      status = stream.grpc_status
      status.code.should eq(GRPC::StatusCode::UNKNOWN)
      status.message.should contain("missing grpc-status")
    end

    it "prefers transport error override over grpc-status when trailers are present" do
      stream = GRPC::Transport::PendingStream.new
      stream.trailers.add("grpc-status", "0")
      stream.transport_error = GRPC::Status.unknown("transport close")

      stream.grpc_status.code.should eq(GRPC::StatusCode::UNKNOWN)
      stream.grpc_status.message.should eq("transport close")
    end

    it "keeps deframe errors as the final status even with grpc-status" do
      stream = GRPC::Transport::PendingStream.new
      stream.trailers.add("grpc-status", "0")

      stream.receive_data(Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8])

      stream.grpc_status.code.should eq(GRPC::StatusCode::UNIMPLEMENTED)
    end

    it "ignores transport errors reported after finish" do
      stream = GRPC::Transport::PendingStream.new
      stream.trailers.add("grpc-status", "0")

      stream.finish
      stream.transport_error = GRPC::Status.unknown("late close")

      stream.grpc_status.code.should eq(GRPC::StatusCode::OK)
    end

    it "finish is idempotent" do
      stream = GRPC::Transport::PendingStream.new

      stream.finish
      stream.finish

      stream.messages.receive?.should be_nil
    end

    it "uses Codec.decode for streamed frames" do
      stream = GRPC::Transport::PendingStream.new
      # Unsupported compression flag (=2) should become an UNIMPLEMENTED transport error.
      stream.receive_data(Bytes[2_u8, 0_u8, 0_u8, 0_u8, 0_u8])

      stream.messages.receive?.should be_nil
      stream.grpc_status.code.should eq(GRPC::StatusCode::UNIMPLEMENTED)
    end
  end

  describe GRPC::RawServerStream do
    it "exposes initial headers separately from trailers" do
      headers = GRPC::Metadata.new
      headers.add("x-initial", "one")
      trailers = GRPC::Metadata.new
      trailers.add("x-trailer", "done")
      messages = ::Channel(Bytes?).new(1)
      messages.close

      stream = GRPC::RawServerStream.new(messages, -> { GRPC::Status.ok }, -> { trailers }).with_headers(-> { headers })

      stream.headers.get("x-initial").should eq("one")
      stream.trailers.get("x-trailer").should eq("done")
    end
  end
end
