# Proto + gRPC stubs for the Numbers service (single-file example).
#
# In real projects, generate message types and gRPC stubs separately:
#
#   crystal build ../proto.cr/src/protoc-gen-crystal_main.cr -o bin/protoc-gen-crystal
#   crystal build src/protoc-gen-crystal-grpc_main.cr -o bin/protoc-gen-crystal-grpc
#
#   protoc --plugin=protoc-gen-crystal=bin/protoc-gen-crystal \
#          --crystal_out=. numbers.proto
#
#   protoc --plugin=protoc-gen-crystal-grpc=bin/protoc-gen-crystal-grpc \
#          --crystal-grpc_out=. numbers.proto
#
# Proto definition (numbers.proto):
#   syntax = "proto3";
#   package numbers;
#
#   service Numbers {
#     rpc Square    (Number)         returns (Number)        {}  // unary
#     rpc Range     (Number)         returns (stream Number) {}  // server streaming
#     rpc Sum       (stream Number)  returns (Number)        {}  // client streaming
#     rpc Transform (stream Number)  returns (stream Number) {}  // bidirectional
#   }
#
#   message Number { int32 value = 1; }

require "../../src/grpc"

module Numbers
  # --- Minimal protobuf helpers ---

  module Proto
    def self.encode_int32_field(field : Int32, value : Int32) : Bytes
      return Bytes.empty if value == 0
      io = IO::Memory.new
      encode_varint(io, ((field << 3) | 0).to_u64)
      encode_varint(io, value.to_u64)
      io.to_slice
    end

    def self.decode_int32_field(data : Bytes, target_field : Int32) : Int32
      i = 0
      while i < data.size
        tag, consumed = decode_varint(data, i)
        i += consumed
        field_number = (tag >> 3).to_i
        wire_type = (tag & 0x7).to_i
        case wire_type
        when 0
          value, consumed = decode_varint(data, i)
          i += consumed
          return value.to_i32 if field_number == target_field
        when 2
          len, consumed = decode_varint(data, i)
          i += consumed + len.to_i
        else
          break
        end
      end
      0
    end

    def self.encode_varint(io : IO, value : UInt64) : Nil
      loop do
        byte = (value & 0x7F).to_u8
        value >>= 7
        io.write_byte(value != 0 ? (byte | 0x80_u8) : byte)
        break if value == 0
      end
    end

    def self.decode_varint(data : Bytes, offset : Int32) : {UInt64, Int32}
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
  end

  # --- Message type ---

  struct Number
    property value : Int32

    def initialize(@value : Int32 = 0)
    end

    def encode : Bytes
      Proto.encode_int32_field(1, @value)
    end

    def self.decode(data : Bytes) : Number
      new(Proto.decode_int32_field(data, 1))
    end
  end

  # --- Generated service base class and client stub ---

  module Numbers
    FULL_NAME = "numbers.Numbers"

    abstract class Service < GRPC::Service
      def service_full_name : String
        FULL_NAME
      end

      # Unary: returns value^2
      abstract def square(req : Number, ctx : GRPC::ServerContext) : Number

      def dispatch(method : String, body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
        case method
        when "Square"
          req = Number.decode(body)
          {square(req, ctx).encode, GRPC::Status.ok}
        else
          {Bytes.empty, GRPC::Status.unimplemented("method #{method} not found")}
        end
      rescue ex : GRPC::StatusError
        {Bytes.empty, ex.status}
      rescue ex
        {Bytes.empty, GRPC::Status.internal(ex.message || "internal error")}
      end

      # Server streaming: streams integers 1..req.value
      def server_streaming?(method : String) : Bool
        method == "Range"
      end

      abstract def range(req : Number, writer : GRPC::ResponseStream(Number),
                         ctx : GRPC::ServerContext) : GRPC::Status

      def dispatch_server_stream(method : String, body : Bytes,
                                 ctx : GRPC::ServerContext,
                                 writer : GRPC::RawResponseStream) : GRPC::Status
        case method
        when "Range"
          typed_writer = GRPC::ResponseStream(Number).new(writer)
          range(Number.decode(body), typed_writer, ctx)
        else
          GRPC::Status.unimplemented("method #{method} not found")
        end
      rescue ex : GRPC::StatusError
        ex.status
      rescue ex
        GRPC::Status.internal(ex.message || "internal error")
      end

      # Client streaming: returns the sum of all received numbers
      def client_streaming?(method : String) : Bool
        method == "Sum"
      end

      abstract def sum(requests : GRPC::RequestStream(Number), ctx : GRPC::ServerContext) : Number

      def dispatch_client_stream(method : String, requests : GRPC::RawRequestStream,
                                 ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
        case method
        when "Sum"
          typed = GRPC::RequestStream(Number).new(requests.ch)
          {sum(typed, ctx).encode, GRPC::Status.ok}
        else
          {Bytes.empty, GRPC::Status.unimplemented("method #{method} not found")}
        end
      rescue ex : GRPC::StatusError
        {Bytes.empty, ex.status}
      rescue ex
        {Bytes.empty, GRPC::Status.internal(ex.message || "internal error")}
      end

      # Bidirectional streaming: returns the square of each received number
      def bidi_streaming?(method : String) : Bool
        method == "Transform"
      end

      abstract def transform(requests : GRPC::RequestStream(Number), writer : GRPC::ResponseStream(Number),
                             ctx : GRPC::ServerContext) : GRPC::Status

      def dispatch_bidi_stream(method : String, requests : GRPC::RawRequestStream,
                               ctx : GRPC::ServerContext,
                               writer : GRPC::RawResponseStream) : GRPC::Status
        case method
        when "Transform"
          typed = GRPC::RequestStream(Number).new(requests.ch)
          typed_writer = GRPC::ResponseStream(Number).new(writer)
          transform(typed, typed_writer, ctx)
        else
          GRPC::Status.unimplemented("method #{method} not found")
        end
      rescue ex : GRPC::StatusError
        ex.status
      rescue ex
        GRPC::Status.internal(ex.message || "internal error")
      end
    end

    class Client
      def initialize(@channel : GRPC::Channel)
      end

      # Unary call.
      def square(req : Number, ctx : GRPC::ClientContext = GRPC::ClientContext.new) : Number
        response = @channel.unary_call(FULL_NAME, "Square", req.encode, ctx)
        raise GRPC::StatusError.new(response.status, response.trailing_metadata) unless response.status.ok?
        Number.decode(response.raw)
      end

      # Server streaming: returns a typed stream of Number replies.
      def range(req : Number, ctx : GRPC::ClientContext = GRPC::ClientContext.new) : GRPC::ServerStream(Number)
        raw = @channel.open_server_stream(FULL_NAME, "Range", req.encode, ctx)
        stream = GRPC::ServerStream(Number).new(-> { raw.headers }, -> { raw.status }, -> { raw.trailers }, -> { raw.cancel })
        spawn do
          begin
            raw.each { |bytes| stream.push(Number.decode(bytes)) }
            stream.finish
          rescue ex
            stream.finish(GRPC::Status.internal(ex.message || "error"))
          end
        end
        stream
      end

      # Client streaming: opens a streaming call; send numbers via the handle,
      # then call close_and_recv to receive the sum.
      def sum(ctx : GRPC::ClientContext = GRPC::ClientContext.new) : GRPC::ClientStream(Number, Number)
        raw = @channel.open_client_stream_live(FULL_NAME, "Sum", ctx)
        result_chan = ::Channel(Number | Exception).new(1)
        send_proc = Proc(Number, Nil).new { |msg| raw.send_raw(msg.encode) }
        close_proc = Proc(Nil).new do
          spawn do
            begin
              body = raw.close_and_recv
              st = raw.status
              if st.ok?
                result_chan.send(Number.decode(body)) rescue nil
              else
                result_chan.send(GRPC::StatusError.new(st, raw.trailers)) rescue nil
              end
            rescue ex
              result_chan.send(ex) rescue nil
            end
          end
        end
        GRPC::ClientStream(Number, Number).new(send_proc, close_proc, result_chan, -> { raw.headers }, -> { raw.status }, -> { raw.trailers }, -> { raw.cancel })
      end

      # Bidirectional streaming: opens a bidi call; send numbers via the handle,
      # then call close_send and iterate replies with #each.
      def transform(ctx : GRPC::ClientContext = GRPC::ClientContext.new) : GRPC::BidiCall(Number, Number)
        raw = @channel.open_bidi_stream_live(FULL_NAME, "Transform", ctx)
        recv_chan = ::Channel(Number | Exception).new(128)
        spawn do
          begin
            raw.each { |bytes| recv_chan.send(Number.decode(bytes)) rescue nil }
            recv_chan.close
          rescue ex
            recv_chan.send(ex) rescue nil
            recv_chan.close rescue nil
          end
        end
        send_proc = Proc(Number, Nil).new { |msg| raw.send_raw(msg.encode) }
        close_proc = Proc(Nil).new { raw.close_send }
        GRPC::BidiCall(Number, Number).new(send_proc, close_proc, recv_chan, -> { raw.headers }, -> { raw.status }, -> { raw.trailers }, -> { raw.cancel })
      end
    end
  end
end
