require "./lib_nghttp2"
require "base64"
require "socket"
require "openssl"

module GRPC
  module Transport
    module TrailerCodec
      extend self

      def percent_decode(s : String) : String
        result = IO::Memory.new
        i = 0
        while i < s.size
          if s[i] == '%' && i + 2 < s.size
            hex = s[i + 1, 2]
            byte = hex.to_u8?(16)
            if byte
              result.write_byte(byte)
              i += 3
              next
            end
          end
          result.write_byte(s.byte_at(i))
          i += 1
        end
        String.new(result.to_slice)
      end

      def encode_bin(data : Bytes?) : String?
        return unless data
        Base64.strict_encode(data)
      end

      def decode_bin(data : String?) : Bytes?
        return if data.nil? || data.empty?
        decoded = Base64.decode(data)
        canonical = Base64.strict_encode(decoded)
        return decoded if data == canonical || data == canonical.gsub(/=+$/, "")
        nil
      rescue
        nil
      end
    end

    # StreamData accumulates headers and body for one HTTP/2 stream.
    class StreamData
      property headers : Metadata
      property body : IO::Memory
      property header_error : Status?
      property? closed : Bool

      def initialize
        @headers = Metadata.new
        @body = IO::Memory.new
        @header_error = nil
        @closed = false
      end

      def body_bytes : Bytes
        @body.to_slice
      end
    end

    # SendBuffer is used as a DataProvider source for outgoing DATA frames.
    class SendBuffer
      property data : Bytes
      property offset : Int32

      def initialize(@data : Bytes)
        @offset = 0
      end

      def remaining : Int32
        @data.size - @offset
      end
    end

    # Http2Connection wraps an nghttp2 session over any IO (plain TCP or TLS).
    # Subclasses implement server-specific or client-specific logic.
    abstract class Http2Connection
      HTTP2_CLIENT_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
      RECV_BUFFER_SIZE     = 65536

      @session : LibNghttp2::Session*
      @socket : IO
      @peer_address : String
      @mutex : Mutex
      @user_data : Void* # GC anchor for Box.box(self)
      @closed : Bool

      def closed? : Bool
        @closed
      end

      def initialize(@socket : IO, @peer_address : String = "unknown")
        @mutex = Mutex.new(:reentrant)
        @closed = false
        @user_data = Pointer(Void).null              # filled in subclass after Box.box(self)
        @session = Pointer(LibNghttp2::Session).null # filled in setup_session
      end

      # Subclasses call this inside their own initialize after all fields are set.
      protected def setup_session(server_side : Bool) : Nil
        @user_data = Box.box(self)

        callbacks = Pointer(LibNghttp2::SessionCallbacks).null
        check LibNghttp2.session_callbacks_new(pointerof(callbacks))

        install_callbacks(callbacks)

        if server_side
          check LibNghttp2.session_server_new(pointerof(@session), callbacks, @user_data)
        else
          check LibNghttp2.session_client_new(pointerof(@session), callbacks, @user_data)
        end
        LibNghttp2.session_callbacks_del(callbacks)

        # Send initial SETTINGS
        settings = StaticArray[
          LibNghttp2::SettingsEntry.new(
            settings_id: LibNghttp2::SETTINGS_MAX_CONCURRENT_STREAMS,
            value: 100_u32
          ),
          LibNghttp2::SettingsEntry.new(
            settings_id: LibNghttp2::SETTINGS_INITIAL_WINDOW_SIZE,
            value: 65535_u32
          ),
        ]
        check LibNghttp2.submit_settings(@session, LibNghttp2::FLAG_NONE,
          settings.to_unsafe, settings.size)
        flush_send
      end

      private def install_callbacks(callbacks : LibNghttp2::SessionCallbacks*) : Nil
        LibNghttp2.session_callbacks_set_on_begin_headers_callback(callbacks,
          LibNghttp2::OnBeginHeadersCallback.new do |_, frame, user_data|
            conn = Box(Http2Connection).unbox(user_data)
            conn.on_begin_headers_cb(frame)
            0
          end
        )
        LibNghttp2.session_callbacks_set_on_header_callback(callbacks,
          LibNghttp2::OnHeaderCallback.new do |_, frame, name, nlen, value, vlen, _, user_data|
            conn = Box(Http2Connection).unbox(user_data)
            conn.on_header_cb(frame, name, nlen, value, vlen)
            0
          end
        )
        LibNghttp2.session_callbacks_set_on_data_chunk_recv_callback(callbacks,
          LibNghttp2::OnDataChunkRecvCallback.new do |_, _, stream_id, data, len, user_data|
            conn = Box(Http2Connection).unbox(user_data)
            conn.on_data_chunk_cb(stream_id, data, len)
            0
          end
        )
        LibNghttp2.session_callbacks_set_on_frame_recv_callback(callbacks,
          LibNghttp2::OnFrameRecvCallback.new do |_, frame, user_data|
            conn = Box(Http2Connection).unbox(user_data)
            conn.on_frame_recv_cb(frame)
            0
          end
        )
        LibNghttp2.session_callbacks_set_on_frame_send_callback(callbacks,
          LibNghttp2::OnFrameSendCallback.new do |_, frame, user_data|
            conn = Box(Http2Connection).unbox(user_data)
            conn.on_frame_send_cb(frame)
            0
          end
        )
        LibNghttp2.session_callbacks_set_on_stream_close_callback(callbacks,
          LibNghttp2::OnStreamCloseCallback.new do |_, stream_id, error_code, user_data|
            conn = Box(Http2Connection).unbox(user_data)
            conn.on_stream_close_cb(stream_id, error_code)
            0
          end
        )
      end

      # --- Callbacks (called from nghttp2, protected by GIL via @mutex) ---

      def on_begin_headers_cb(frame : Void*) : Nil
      end

      def on_header_cb(frame : Void*, name : UInt8*, nlen : LibC::SizeT,
                       value : UInt8*, vlen : LibC::SizeT) : Nil
      end

      def on_data_chunk_cb(stream_id : Int32, data : UInt8*, len : LibC::SizeT) : Nil
      end

      def on_frame_recv_cb(frame : Void*) : Nil
      end

      def on_frame_send_cb(frame : Void*) : Nil
      end

      def on_stream_close_cb(stream_id : Int32, error_code : UInt32) : Nil
      end

      # --- I/O ---

      # flush_send writes all pending nghttp2 outbound data to the socket.
      protected def flush_send : Nil
        return if @closed
        empty_send_count = 0
        loop do
          data_ptr = Pointer(UInt8).null
          n = LibNghttp2.session_mem_send(@session, pointerof(data_ptr))
          raise ConnectionError.new("session_mem_send failed: #{String.new(LibNghttp2.strerror(n.to_i32))} (#{n})") if n < 0
          if n == 0
            break if LibNghttp2.session_want_write(@session) == 0
            empty_send_count += 1
            raise ConnectionError.new("session_mem_send made no progress while write work remained") if empty_send_count > 16
            next
          end
          empty_send_count = 0
          @socket.write(Slice.new(data_ptr, n))
        end
        @socket.flush
      rescue IO::Error
        @closed = true
      end

      # run_recv_loop reads from the socket and feeds data to nghttp2.
      # Runs until the connection closes.
      def run_recv_loop : Nil
        buf = Bytes.new(RECV_BUFFER_SIZE)
        loop do
          n = @socket.read(buf)
          break if n == 0
          @mutex.synchronize do
            result = LibNghttp2.session_mem_recv(@session, buf.to_unsafe, n)
            if result < 0
              @closed = true
              break
            end
            flush_send
          end
        end
      rescue IO::Error
        # connection closed
      ensure
        close
      end

      def close : Nil
        return if @closed
        @closed = true
        LibNghttp2.submit_goaway(@session, LibNghttp2::FLAG_NONE, 0, LibNghttp2::NO_ERROR, nil, 0)
        flush_send rescue nil
        LibNghttp2.session_del(@session)
        @socket.close rescue nil
      end

      # --- Helpers ---

      protected def stream_data_for(stream_id : Int32) : StreamData?
        sd_ptr = LibNghttp2.session_get_stream_user_data(@session, stream_id)
        return if sd_ptr.null?
        Box(StreamData).unbox(sd_ptr)
      end

      protected def frame_stream_id(frame : Void*) : Int32
        frame.as(LibNghttp2::FrameHd*).value.stream_id
      end

      protected def frame_type(frame : Void*) : UInt8
        frame.as(LibNghttp2::FrameHd*).value.type
      end

      protected def frame_flags(frame : Void*) : UInt8
        frame.as(LibNghttp2::FrameHd*).value.flags
      end

      protected def frame_end_stream?(frame : Void*) : Bool
        (frame_flags(frame) & LibNghttp2::FLAG_END_STREAM) != 0
      end

      protected def make_nv(name : String, value : String) : LibNghttp2::Nv
        n = name.to_slice
        v = value.to_slice
        LibNghttp2::Nv.new(
          name: n.to_unsafe,
          value: v.to_unsafe,
          namelen: n.size,
          valuelen: v.size,
          flags: LibNghttp2::NV_FLAG_NONE
        )
      end

      protected def check(rc : Int32, msg : String = "nghttp2 error") : Nil
        return if rc >= 0
        err = String.new(LibNghttp2.strerror(rc))
        raise ConnectionError.new("#{msg}: #{err} (#{rc})")
      end
    end
  end
end
