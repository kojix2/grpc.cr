module GRPC
  module Health
    enum ServingStatus : Int32
      UNKNOWN         = 0
      SERVING         = 1
      NOT_SERVING     = 2
      SERVICE_UNKNOWN = 3
    end

    struct CheckRequest
      getter service : String

      def initialize(@service : String = "")
      end

      def encode : Bytes
        return Bytes.empty if @service.empty?

        io = IO::Memory.new
        Health.write_varint(io, 10_u64) # field 1, length-delimited
        value = @service.to_slice
        Health.write_varint(io, value.size.to_u64)
        io.write(value)
        io.to_slice
      end

      def self.decode(bytes : Bytes) : self
        cursor = 0
        service = ""

        while cursor < bytes.size
          tag, cursor = Health.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          if field_number == 1 && wire_type == 2
            len, cursor = Health.read_varint(bytes, cursor)
            length = len.to_i
            Health.ensure_length!(bytes, cursor, length)
            service = String.new(bytes[cursor, length])
            cursor += length
          else
            cursor = Health.skip_field(bytes, cursor, wire_type)
          end
        end

        new(service)
      end
    end

    struct CheckResponse
      getter status : ServingStatus

      def initialize(@status : ServingStatus)
      end

      def encode : Bytes
        io = IO::Memory.new
        Health.write_varint(io, 8_u64) # field 1, varint
        Health.write_varint(io, @status.value.to_u64)
        io.to_slice
      end

      def self.decode(bytes : Bytes) : self
        cursor = 0
        status = ServingStatus::UNKNOWN

        while cursor < bytes.size
          tag, cursor = Health.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          if field_number == 1 && wire_type == 0
            raw_value, cursor = Health.read_varint(bytes, cursor)
            value = raw_value.to_i32
            status = ServingStatus.from_value?(value) || ServingStatus::UNKNOWN
          else
            cursor = Health.skip_field(bytes, cursor, wire_type)
          end
        end

        new(status)
      end
    end

    def self.write_varint(io : IO, value : UInt64) : Nil
      current = value
      loop do
        byte = (current & 0x7F_u64).to_u8
        current >>= 7
        if current == 0_u64
          io.write_byte(byte)
          break
        else
          io.write_byte(byte | 0x80_u8)
        end
      end
    end

    def self.read_varint(bytes : Bytes, start : Int32) : {UInt64, Int32}
      result = 0_u64
      shift = 0
      cursor = start

      while cursor < bytes.size
        byte = bytes[cursor]
        cursor += 1
        result |= ((byte & 0x7F_u8).to_u64 << shift)
        return {result, cursor} if (byte & 0x80_u8) == 0_u8
        shift += 7
        raise ArgumentError.new("malformed varint") if shift >= 64
      end

      raise ArgumentError.new("truncated varint")
    end

    def self.skip_field(bytes : Bytes, start : Int32, wire_type : Int32) : Int32
      case wire_type
      when 0
        _, cursor = read_varint(bytes, start)
        cursor
      when 1
        Health.ensure_length!(bytes, start, 8)
        start + 8
      when 2
        len, cursor = Health.read_varint(bytes, start)
        length = len.to_i
        Health.ensure_length!(bytes, cursor, length)
        cursor + length
      when 5
        Health.ensure_length!(bytes, start, 4)
        start + 4
      else
        raise ArgumentError.new("unsupported wire type: #{wire_type}")
      end
    end

    def self.ensure_length!(bytes : Bytes, start : Int32, length : Int32) : Nil
      if length < 0 || start < 0 || start + length > bytes.size
        raise ArgumentError.new("truncated message")
      end
    end

    # Minimal standard health checking service.
    # This step intentionally implements unary Check only (no Watch yet).
    class Service < GRPC::Service
      SERVICE_FULL_NAME = "grpc.health.v1.Health"

      @statuses : Hash(String, ServingStatus)
      @default_status : ServingStatus

      def initialize(default_status : ServingStatus = ServingStatus::SERVING)
        @statuses = {} of String => ServingStatus
        @default_status = default_status
      end

      def service_full_name : String
        SERVICE_FULL_NAME
      end

      def set_status(service : String, status : ServingStatus) : self
        @statuses[service] = status
        self
      end

      def clear_status(service : String) : self
        @statuses.delete(service)
        self
      end

      def server_streaming?(method : String) : Bool
        method == "Watch"
      end

      def dispatch_server_stream(method : String, request_body : Bytes,
                                 ctx : ServerContext, writer : RawResponseStream) : Status
        _ = ctx
        case method
        when "Watch"
          request = CheckRequest.decode(request_body)
          writer.send_raw(CheckResponse.new(resolve_status(request.service)).encode)
          Status.ok
        else
          Status.unimplemented("method #{method} not found")
        end
      rescue ex
        Status.invalid_argument(ex.message || "invalid health watch request")
      end

      def dispatch(method : String, request_body : Bytes, ctx : ServerContext) : {Bytes, Status}
        _ = ctx
        case method
        when "Check"
          request = CheckRequest.decode(request_body)
          {CheckResponse.new(resolve_status(request.service)).encode, Status.ok}
        else
          {Bytes.empty, Status.unimplemented("method #{method} not found")}
        end
      rescue ex
        {Bytes.empty, Status.invalid_argument(ex.message || "invalid health check request")}
      end

      private def resolve_status(service : String) : ServingStatus
        return @default_status if service.empty?
        @statuses[service]? || ServingStatus::SERVICE_UNKNOWN
      end
    end
  end
end
