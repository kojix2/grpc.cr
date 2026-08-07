module GRPC
  module Transport
    # GrpcDeframer incrementally accumulates DATA chunks and extracts complete
    # gRPC messages while preserving any trailing partial frame bytes.
    class GrpcDeframer
      record FrameHeader, compressed : UInt8, length : Int32

      @segments : Deque(Bytes)
      @head_offset : Int32
      @remainder_size : Int32
      property encoding : String?

      def initialize(@encoding : String? = nil, @validate_encoding : Bool = false)
        @segments = Deque(Bytes).new
        @head_offset = 0
        @remainder_size = 0
      end

      def append(chunk : Bytes) : Nil
        return if chunk.empty?

        # Incoming slices may be backed by transient transport buffers.
        # Keep one owned copy per chunk and avoid whole-buffer repacking.
        owned = Bytes.new(chunk.size)
        owned.copy_from(chunk.to_unsafe, chunk.size)
        @segments << owned
        @remainder_size += owned.size
      end

      def drain_messages : Array(Bytes)
        messages = [] of Bytes

        loop do
          break if @remainder_size < Codec::HEADER_SIZE

          # Parse the fixed 5-byte header in place so we do not materialize a
          # temporary frame unless the payload itself must be copied out.
          header = read_header
          total = Codec::HEADER_SIZE + header.length
          break if @remainder_size < total

          skip_exact(Codec::HEADER_SIZE)
          message = consume_message_payload(header)
          messages << message
        end

        messages
      end

      def remainder_size : Int32
        @remainder_size
      end

      private def read_header : FrameHeader
        compressed = read_byte_at(0)
        length = 0

        4.times do |index|
          length = (length << 8) | read_byte_at(index + 1)
        end

        FrameHeader.new(compressed.to_u8, length)
      end

      private def consume_message_payload(header : FrameHeader) : Bytes
        payload = consume_payload_exact(header.length)
        Codec.decode_payload(header.compressed, payload, @encoding, @validate_encoding)
      end

      private def skip_exact(n : Int32) : Nil
        return if n <= 0
        raise "internal deframer error: consume beyond remainder" if n > @remainder_size

        # Header bytes are no longer needed after length validation, so drop
        # them by advancing segment cursors instead of copying them anywhere.
        remaining = n

        while remaining > 0
          seg = @segments.first?
          raise "internal deframer error: missing segment" unless seg

          seg_available = seg.size - @head_offset
          take = Math.min(seg_available, remaining)
          remaining -= take
          @head_offset += take

          if @head_offset == seg.size
            @segments.shift
            @head_offset = 0
          end
        end

        @remainder_size -= n
      end

      private def consume_payload_exact(n : Int32) : Bytes
        return Bytes.empty if n <= 0
        raise "internal deframer error: consume beyond remainder" if n > @remainder_size

        # Public stream APIs still hand out owned Bytes, so payload extraction
        # remains the only unavoidable copy on the non-compressed path.
        result = Bytes.new(n)
        out_offset = 0
        remaining = n

        while remaining > 0
          seg = @segments.first?
          raise "internal deframer error: missing segment" unless seg

          seg_available = seg.size - @head_offset
          take = Math.min(seg_available, remaining)

          (result.to_unsafe + out_offset).copy_from(seg.to_unsafe + @head_offset, take)

          out_offset += take
          remaining -= take
          @head_offset += take

          if @head_offset == seg.size
            @segments.shift
            @head_offset = 0
          end
        end

        @remainder_size -= n
        result
      end

      private def read_byte_at(index : Int32) : Int32
        raise "internal deframer error: insufficient bytes while peeking" if index < 0 || index >= @remainder_size

        # This is intentionally scalar and allocation-free because it is only
        # used for the 5-byte header fast path.
        remaining = index
        first_segment = true

        @segments.each do |seg|
          seg_offset = first_segment ? @head_offset : 0
          first_segment = false

          seg_available = seg.size - seg_offset
          next if seg_available <= 0

          if remaining < seg_available
            return seg[seg_offset + remaining].to_i
          end

          remaining -= seg_available
        end

        raise "internal deframer error: insufficient bytes while peeking"
      end
    end
  end
end
