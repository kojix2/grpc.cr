require "compress/gzip"

module GRPC
  # Codec handles gRPC message framing and optional gzip compression.
  #
  # Each gRPC message is prefixed with a 5-byte header:
  #   Byte 0:    Compressed-Flag (0 = not compressed, 1 = gzip compressed)
  #   Bytes 1-4: Message-Length (big-endian uint32) — length of the (possibly compressed) body
  module Codec
    HEADER_SIZE = 5

    # Encode a protobuf-serialized message into a gRPC DATA frame payload.
    # Pass `compress: true` to gzip-compress the payload.
    def self.encode(message_bytes : Bytes, compress : Bool = false) : Bytes
      if compress
        compressed = compress_gzip(message_bytes)
        io = IO::Memory.new(HEADER_SIZE + compressed.size)
        io.write_byte(1_u8)
        io.write_bytes(compressed.size.to_u32, IO::ByteFormat::BigEndian)
        io.write(compressed)
      else
        io = IO::Memory.new(HEADER_SIZE + message_bytes.size)
        io.write_byte(0_u8)
        io.write_bytes(message_bytes.size.to_u32, IO::ByteFormat::BigEndian)
        io.write(message_bytes)
      end
      io.to_slice
    end

    # Decode reads one gRPC message from the given Bytes slice.
    # Returns {message_bytes, bytes_consumed}.
    # Decompresses gzip-compressed frames automatically.
    # Raises StatusError on incomplete or malformed frames.
    def self.decode(data : Bytes, encoding : String? = nil,
                    validate_encoding : Bool = false) : {Bytes, Int32}
      raise StatusError.new(StatusCode::INTERNAL, "incomplete gRPC frame header") if data.size < HEADER_SIZE

      compressed = data[0]
      length = IO::ByteFormat::BigEndian.decode(UInt32, data[1, 4]).to_i
      total = HEADER_SIZE + length

      raise StatusError.new(StatusCode::INTERNAL, "incomplete gRPC frame body") if data.size < total

      body = data[HEADER_SIZE, length]
      {decode_payload(compressed, body, encoding, validate_encoding), total}
    end

    def self.decode_payload(compressed : UInt8, body : Bytes,
                            encoding : String? = nil,
                            validate_encoding : Bool = false) : Bytes
      # Keep the uncompressed path as a borrowed slice; only gzip requires a
      # fresh allocation for the decompressed payload.
      if compressed == 1
        if validate_encoding && encoding != "gzip"
          raise StatusError.new(StatusCode::UNIMPLEMENTED,
            "compressed gRPC message requires grpc-encoding: gzip")
        end
        decompress_gzip(body)
      elsif compressed == 0
        body
      else
        raise StatusError.new(StatusCode::UNIMPLEMENTED, "unsupported gRPC compression flag: #{compressed}")
      end
    end

    # Decode all gRPC messages from the given Bytes slice.
    def self.decode_all(data : Bytes) : Array(Bytes)
      messages = [] of Bytes
      offset = 0
      while offset < data.size
        msg, consumed = decode(data[offset..])
        messages << msg
        offset += consumed
      end
      messages
    end

    private def self.compress_gzip(data : Bytes) : Bytes
      io = IO::Memory.new
      Compress::Gzip::Writer.open(io, &.write(data))
      io.to_slice
    end

    private def self.decompress_gzip(data : Bytes) : Bytes
      Compress::Gzip::Reader.open(IO::Memory.new(data), &.getb_to_end)
    end
  end
end
