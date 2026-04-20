module GRPC
  module Transport
    # GrpcDeframer incrementally accumulates DATA chunks and extracts complete
    # gRPC messages while preserving any trailing partial frame bytes.
    class GrpcDeframer
      @segments : Deque(Bytes)
      @head_offset : Int32
      @remainder_size : Int32

      def initialize
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

          header = peek_exact(Codec::HEADER_SIZE)
          length = IO::ByteFormat::BigEndian.decode(UInt32, header[1, 4]).to_i
          total = Codec::HEADER_SIZE + length
          break if @remainder_size < total

          frame = consume_exact(total)
          message, _ = Codec.decode(frame)
          messages << message
        end

        messages
      end

      def remainder_size : Int32
        @remainder_size
      end

      private def peek_exact(n : Int32) : Bytes
        return Bytes.empty if n <= 0

        if first = @segments.first?
          available = first.size - @head_offset
          if available >= n
            return first[@head_offset, n]
          end
        end

        header_bytes = Bytes.new(n)
        copy_from_segments(header_bytes.to_unsafe, n)
        header_bytes
      end

      private def consume_exact(n : Int32) : Bytes
        return Bytes.empty if n <= 0
        raise "internal deframer error: consume beyond remainder" if n > @remainder_size

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

      private def copy_from_segments(dst : UInt8*, n : Int32) : Nil
        remaining = n
        dst_offset = 0
        first_segment = true

        @segments.each do |seg|
          seg_offset = first_segment ? @head_offset : 0
          first_segment = false

          seg_available = seg.size - seg_offset
          next if seg_available <= 0

          take = Math.min(seg_available, remaining)
          (dst + dst_offset).copy_from(seg.to_unsafe + seg_offset, take)
          dst_offset += take
          remaining -= take
          break if remaining == 0
        end

        raise "internal deframer error: insufficient bytes while peeking" unless remaining == 0
      end
    end
  end
end
