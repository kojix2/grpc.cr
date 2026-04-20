require "./http2_connection"
require "./grpc_deframer"
require "./grpc_status_interpreter"
require "./interface"
require "./stream_state"
require "../endpoint"

module GRPC
  module Transport
    # PendingStream holds state for an in-flight client server-streaming RPC.
    class PendingStream
      property messages : ::Channel(Bytes?)
      property send_buf : SendBuffer?          # GC anchor for request body (batch bidi)
      property live_send_buf : LiveSendBuffer? # GC anchor for live bidi
      property send_resume_proc : (-> Nil)?    # wakes up deferred DATA for live bidi
      property cancel_proc : (-> Nil)?
      @status_override : Status?
      @header_state : StreamHeaderState
      @terminal_state : StreamTerminalState
      @deframer : GrpcDeframer

      def initialize
        @messages = ::Channel(Bytes?).new(128)
        @send_buf = nil
        @live_send_buf = nil
        @send_resume_proc = nil
        @cancel_proc = nil
        @status_override = nil
        @header_state = StreamHeaderState.new
        @terminal_state = StreamTerminalState.new
        @deframer = GrpcDeframer.new
      end

      def headers : Metadata
        @header_state.headers
      end

      def trailers : Metadata
        @header_state.trailers
      end

      def begin_header_block : Nil
        @header_state.begin_header_block
      end

      def add_header(key : String, value : String) : Nil
        @header_state.add_header(key, value)
      rescue ex : ArgumentError
        self.transport_error = Status.internal(ex.message || "invalid response metadata")
      end

      def receive_data(chunk : Bytes) : Nil
        @deframer.append(chunk)
        @deframer.drain_messages.each { |msg| @messages.send(msg) }
      rescue ex : StatusError
        self.transport_error = ex.status
        finish
      end

      def finish : Nil
        return unless @terminal_state.mark_finished
        @messages.send(nil) rescue nil
      end

      def transport_error=(status : Status) : Nil
        return if @terminal_state.finished?
        @status_override ||= status
      end

      # send_outgoing enqueues a pre-framed gRPC message for live bidi streaming
      # and signals nghttp2 to resume data transmission.
      def send_outgoing(framed_bytes : Bytes) : Nil
        @live_send_buf.try &.push(framed_bytes)
        @send_resume_proc.try &.call
      end

      # close_send signals the end of the client request stream for live bidi.
      def close_send : Nil
        @live_send_buf.try &.close
        @send_resume_proc.try &.call
      end

      # cancel sends an HTTP/2 RST_STREAM with error code CANCEL and closes the
      # message channel so waiting iterators unblock.
      def cancel : Nil
        return unless @terminal_state.mark_cancelled
        @cancel_proc.try &.call
        @messages.close rescue nil
      end

      def cancelled? : Bool
        @terminal_state.cancelled?
      end

      def grpc_headers : Metadata
        GrpcStatusInterpreter.application_headers(@header_state.headers)
      end

      # grpc_trailers returns the raw trailer hash received from the server.
      def grpc_trailers : Metadata
        GrpcStatusInterpreter.application_trailers(@header_state.trailers)
      end

      def grpc_status : Status
        GrpcStatusInterpreter.grpc_status(@header_state.headers, @header_state.trailers, @status_override)
      end
    end

    # PendingCall holds state for an in-flight client RPC.
    class PendingCall
      property response_body : IO::Memory
      property done : ::Channel(Nil)
      property send_buf : SendBuffer? # GC anchor
      @status_override : Status?
      @header_state : StreamHeaderState
      @terminal_state : StreamTerminalState

      def initialize
        @response_body = IO::Memory.new
        @done = ::Channel(Nil).new(1)
        @send_buf = nil
        @status_override = nil
        @header_state = StreamHeaderState.new
        @terminal_state = StreamTerminalState.new
      end

      def response_headers : Metadata
        @header_state.headers
      end

      def trailers : Metadata
        @header_state.trailers
      end

      def begin_header_block : Nil
        @header_state.begin_header_block
      end

      def add_header(key : String, value : String) : Nil
        @header_state.add_header(key, value)
      rescue ex : ArgumentError
        self.transport_error = Status.internal(ex.message || "invalid response metadata")
      end

      def wait : Nil
        @done.receive
      end

      def complete : Nil
        return unless @terminal_state.mark_finished
        @done.send(nil) rescue nil
      end

      def transport_error=(status : Status) : Nil
        return if @terminal_state.finished?
        @status_override ||= status
      end

      def grpc_status : Status
        GrpcStatusInterpreter.grpc_status(@header_state.headers, @header_state.trailers, @status_override)
      end

      def response_bytes : Bytes
        @response_body.to_slice
      end

      def grpc_headers : Metadata
        GrpcStatusInterpreter.application_headers(@header_state.headers)
      end

      def grpc_trailers : Metadata
        GrpcStatusInterpreter.application_trailers(@header_state.trailers)
      end
    end

    # LiveSendBuffer is an on-demand queue for full-duplex bidi streaming.
    # DATA_READ_CB_LIVE returns NGHTTP2_ERR_DEFERRED when the queue is empty,
    # and the caller must call session_resume_data after pushing new data.
    #
    # When *capacity* > 0 the buffer is bounded: push blocks the calling fiber
    # until a slot is available, providing backpressure to the sender.
    # capacity == 0 (default) means unbounded.
    #
    # Lock ordering: callers must NOT hold @lsb_mutex while acquiring the
    # connection @mutex.  read_into is called from inside the connection @mutex,
    # so it acquires @lsb_mutex as an inner lock — which is safe because push/close
    # always release @lsb_mutex before the caller acquires the connection @mutex
    # (via send_resume_proc).
    class LiveSendBuffer
      @deque : Deque(Bytes)
      @current : IO::Memory?
      @closed : Bool
      @lsb_mutex : Mutex
      # Counting semaphore for bounded mode.  Pre-filled with *capacity* permits;
      # push consumes one permit (blocks when 0), read_into returns one on shift.
      @permits : ::Channel(Nil)?

      def initialize(capacity : Int32 = 0)
        @deque = Deque(Bytes).new
        @current = nil
        @closed = false
        @lsb_mutex = Mutex.new
        if capacity > 0
          permits = ::Channel(Nil).new(capacity)
          capacity.times { permits.send(nil) }
          @permits = permits
        else
          @permits = nil
        end
      end

      def push(bytes : Bytes) : Nil
        # Block until a slot is available (no-op when unbounded).
        @permits.try &.receive
        @lsb_mutex.synchronize { @deque.push(bytes) }
      end

      def close : Nil
        @lsb_mutex.synchronize { @closed = true }
      end

      def closed? : Bool
        @lsb_mutex.synchronize { @closed }
      end

      # read_into fills *buf* with up to *length* bytes from the queue.
      # Returns the number of bytes written, sets DATA_FLAG_EOF when done,
      # or returns NGHTTP2_ERR_DEFERRED when no data is available yet.
      # Called from DATA_READ_CB_LIVE while the connection @mutex is held.
      def read_into(buf : UInt8*, length : LibC::SizeT,
                    data_flags : UInt32*) : LibC::SSizeT
        @lsb_mutex.synchronize do
          advance_current_if_exhausted
          if @current.nil?
            if early = load_next_chunk(data_flags)
              next early
            end
          end
          if cur = @current
            copy_from_current(cur, buf, length, data_flags)
          else
            LibNghttp2::ERR_DEFERRED.to_i64
          end
        end
      end

      # Returns nil when the next chunk was loaded into @current (caller should copy).
      # Returns an early-exit value (0 for EOF, ERR_DEFERRED) when no data is available.
      private def load_next_chunk(data_flags : UInt32*) : LibC::SSizeT?
        if @deque.empty?
          if @closed
            data_flags.value |= LibNghttp2::DATA_FLAG_EOF
            return 0_i64
          end
          return LibNghttp2::ERR_DEFERRED.to_i64
        end
        @current = IO::Memory.new(@deque.shift)
        # Return one permit now that a slot has been freed.
        @permits.try { |permits_ch| permits_ch.send(nil) rescue nil }
        nil
      end

      private def advance_current_if_exhausted : Nil
        if (cur = @current) && cur.pos == cur.size
          @current = nil
        end
      end

      private def copy_from_current(cur : IO::Memory, buf : UInt8*, length : LibC::SizeT,
                                    data_flags : UInt32*) : LibC::SSizeT
        to_copy = Math.min(length.to_i, (cur.size - cur.pos).to_i).to_i
        buf.copy_from(cur.to_slice.to_unsafe + cur.pos, to_copy)
        cur.pos += to_copy
        data_flags.value |= LibNghttp2::DATA_FLAG_EOF if cur.pos == cur.size && @deque.empty? && @closed
        to_copy.to_i64
      end
    end

    # Http2ClientConnection manages one HTTP/2 connection from a gRPC client.
    class Http2ClientConnection < Http2Connection
      include ClientTransport
      DATA_READ_CB = LibNghttp2::DataSourceReadCallback.new do |_session, _stream_id, buf, length, data_flags, source, _user_data|
        sb = Box(SendBuffer).unbox(source.value.ptr)
        to_copy = Math.min(length.to_i, sb.remaining).to_i
        if to_copy == 0
          data_flags.value |= LibNghttp2::DATA_FLAG_EOF
          next 0_i64
        end
        buf.copy_from(sb.data.to_unsafe + sb.offset, to_copy)
        sb.offset += to_copy
        data_flags.value |= LibNghttp2::DATA_FLAG_EOF if sb.remaining == 0
        to_copy.to_i64
      end

      DATA_READ_CB_LIVE = LibNghttp2::DataSourceReadCallback.new do |_session, _stream_id, buf, length, data_flags, source, _user_data|
        lsb = Box(LiveSendBuffer).unbox(source.value.ptr)
        lsb.read_into(buf, length, data_flags)
      end

      # stream_id => PendingCall (unary)
      @pending : Hash(Int32, PendingCall)
      # stream_id => PendingStream (server-streaming)
      @pending_streams : Hash(Int32, PendingStream)
      # stream_id => Void* (GC anchor for stream user data)
      @stream_boxes : Hash(Int32, Void*)
      # stream_id => Void* (GC anchor for nghttp2 DataSource.ptr boxes)
      @data_source_boxes : Hash(Int32, Void*)
      # GC anchor for the TLS context and socket (prevents premature collection)
      @tls_context_anchor : OpenSSL::SSL::Context::Client?
      @tls_socket_anchor : OpenSSL::SSL::Socket::Client?
      @use_tls : Bool
      @endpoint_config : EndpointConfig
      @target_authority : String

      def initialize(host : String, port : Int32, use_tls : Bool = false,
                     tls_context : OpenSSL::SSL::Context::Client? = nil,
                     endpoint_config : EndpointConfig = EndpointConfig.new)
        tcp = if connect_timeout = endpoint_config.connect_timeout
                TCPSocket.new(host, port, connect_timeout: connect_timeout)
              else
                TCPSocket.new(host, port)
              end
        tcp.tcp_nodelay = true
        apply_keepalive_options(tcp, endpoint_config)
        peer = tcp.remote_address.to_s rescue "#{host}:#{port}"

        @tls_context_anchor = nil
        @tls_socket_anchor = nil
        @use_tls = use_tls
        @endpoint_config = endpoint_config
        @target_authority = "#{host}:#{port}"

        io : IO
        if use_tls
          ctx = tls_context || begin
            c = OpenSSL::SSL::Context::Client.new
            c.alpn_protocol = "h2"
            c
          end
          ssl_socket = OpenSSL::SSL::Socket::Client.new(tcp, ctx, sync_close: true, hostname: host)
          @tls_context_anchor = ctx
          @tls_socket_anchor = ssl_socket
          io = ssl_socket
        else
          io = tcp
        end

        super(io, peer)
        @pending = {} of Int32 => PendingCall
        @pending_streams = {} of Int32 => PendingStream
        @stream_boxes = {} of Int32 => Void*
        @data_source_boxes = {} of Int32 => Void*

        # nghttp2 client session automatically prepends the HTTP/2 connection preface
        # to the first session_mem_send output, so we must NOT write it manually.
        setup_session(server_side: false)

        # Start recv loop in background fiber
        spawn run_recv_loop
      end

      private def apply_keepalive_options(tcp : TCPSocket, endpoint_config : EndpointConfig) : Nil
        # Enable TCP keepalive when any keepalive-related option is set.
        has_keepalive = !endpoint_config.tcp_keepalive.nil? || !endpoint_config.keepalive.nil?
        return unless has_keepalive

        tcp.keepalive = true
      rescue
        # Keepalive tuning is best-effort and platform-dependent.
      end

      # unary_call sends one gRPC request and blocks until the response arrives.
      def unary_call(service : String, method : String, request_body : Bytes,
                     metadata : Metadata = Metadata.new) : ResponseEnvelope
        submit_unary_request(service, method, Codec.encode(request_body), metadata)
      end

      # open_server_stream sends one gRPC request and returns a RawServerStream that
      # delivers server-pushed messages as they arrive.
      def open_server_stream(service : String, method : String, request_bytes : Bytes,
                             metadata : Metadata = Metadata.new) : RawServerStream
        pending_to_raw_stream(submit_streaming_request(service, method, Codec.encode(request_bytes), metadata))
      end

      # open_bidi_stream_live opens a true full-duplex bidi stream.
      # Returns a RawBidiCall whose send_raw / close_send / each methods let the
      # caller interleave sends and receives freely.
      # *send_queue_size* limits how many outgoing frames may be buffered before
      # send_raw blocks the calling fiber (0 = unbounded).
      def open_bidi_stream_live(service : String, method : String,
                                metadata : Metadata = Metadata.new,
                                send_queue_size : Int32 = 0) : RawBidiCall
        pending_to_raw_bidi_call(submit_bidi_stream_live(service, method, metadata, send_queue_size))
      end

      # open_client_stream_live opens a live client-streaming RPC.
      # Returns a RawClientCall whose send_raw / close_and_recv methods let the
      # caller send messages incrementally; the server's single response arrives
      # after close_and_recv returns.
      # *send_queue_size* limits how many outgoing frames may be buffered before
      # send_raw blocks the calling fiber (0 = unbounded).
      def open_client_stream_live(service : String, method : String,
                                  metadata : Metadata = Metadata.new,
                                  send_queue_size : Int32 = 0) : RawClientCall
        pending_to_raw_client_call(submit_bidi_stream_live(service, method, metadata, send_queue_size))
      end

      # ---- Private submission helpers ----
      private def pending_to_raw_stream(ps : PendingStream) : RawServerStream
        RawServerStream.build(
          ps.messages,
          headers_proc: stream_headers_proc(ps),
          status_proc: stream_status_proc(ps),
          trailers_proc: stream_trailers_proc(ps),
          cancel_proc: stream_cancel_proc(ps)
        )
      end

      private def pending_to_raw_client_call(ps : PendingStream) : RawClientCall
        RawClientCall.new(
          ->(b : Bytes) { ps.send_outgoing(Codec.encode(b)) },
          -> {
            ps.close_send
            # Drain the channel until it is closed (nil sentinel).
            # Client-streaming servers send exactly one response message, but we
            # must consume through the sentinel so the channel is fully drained.
            result = Bytes.empty
            loop do
              msg = ps.messages.receive?
              break if msg.nil?
              result = msg
            end
            result
          },
          stream_headers_proc(ps),
          stream_status_proc(ps),
          stream_trailers_proc(ps),
          stream_cancel_proc(ps)
        )
      end

      private def pending_to_raw_bidi_call(ps : PendingStream) : RawBidiCall
        RawBidiCall.new(
          ->(b : Bytes) { ps.send_outgoing(Codec.encode(b)) },
          stream_close_proc(ps),
          ps.messages,
          stream_headers_proc(ps),
          stream_status_proc(ps),
          stream_trailers_proc(ps),
          stream_cancel_proc(ps)
        )
      end

      private def stream_headers_proc(ps : PendingStream) : -> Metadata
        -> { ps.grpc_headers }
      end

      private def stream_status_proc(ps : PendingStream) : -> Status
        -> { ps.grpc_status }
      end

      private def stream_trailers_proc(ps : PendingStream) : -> Metadata
        -> { ps.grpc_trailers }
      end

      private def stream_cancel_proc(ps : PendingStream) : -> Nil
        -> { ps.cancel }
      end

      private def stream_close_proc(ps : PendingStream) : -> Nil
        -> { ps.close_send }
      end

      # Submits an HTTP/2 request with *framed_body* and blocks until the single
      # unary response arrives.  Shared by unary_call and client_stream_call.
      private def submit_unary_request(service : String, method : String,
                                       framed_body : Bytes,
                                       metadata : Metadata) : ResponseEnvelope
        call = PendingCall.new

        @mutex.synchronize do
          sb = SendBuffer.new(framed_body)
          call.send_buf = sb

          nva_list = build_request_headers(service, method, metadata)
          nva = nva_list.to_unsafe
          nvlen = nva_list.size

          src = LibNghttp2::DataSource.new
          boxed_src = Box.box(sb)
          src.ptr = boxed_src
          dp = LibNghttp2::DataProvider.new(source: src, read_callback: DATA_READ_CB)

          boxed_call = Box.box(call)
          stream_id = LibNghttp2.submit_request(@session, nil, nva, nvlen, pointerof(dp), boxed_call)
          raise ConnectionError.new("submit_request failed: #{stream_id}") if stream_id < 0

          @pending[stream_id] = call
          @stream_boxes[stream_id] = boxed_call
          @data_source_boxes[stream_id] = boxed_src
          flush_send
        end

        call.wait
        status = call.grpc_status
        return ResponseEnvelope.new(
          CallInfo.new("/#{service}/#{method}", RPCKind::Unary),
          Bytes.empty,
          status,
          call.grpc_headers,
          call.grpc_trailers
        ) unless status.ok?
        begin
          body, _ = Codec.decode(call.response_bytes)
          ResponseEnvelope.new(
            CallInfo.new("/#{service}/#{method}", RPCKind::Unary),
            body,
            status,
            call.grpc_headers,
            call.grpc_trailers
          )
        rescue ex : StatusError
          ResponseEnvelope.new(
            CallInfo.new("/#{service}/#{method}", RPCKind::Unary),
            Bytes.empty,
            Status.internal("failed to decode unary response: #{ex.message}"),
            call.grpc_headers,
            call.grpc_trailers
          )
        rescue
          ResponseEnvelope.new(
            CallInfo.new("/#{service}/#{method}", RPCKind::Unary),
            Bytes.empty,
            Status.internal("failed to decode unary response"),
            call.grpc_headers,
            call.grpc_trailers
          )
        end
      end

      # Submits an HTTP/2 request with *framed_body* and returns a PendingStream for
      # the server-streamed response.  Shared by open_server_stream and open_bidi_stream.
      private def submit_streaming_request(service : String, method : String,
                                           framed_body : Bytes,
                                           metadata : Metadata) : PendingStream
        ps = PendingStream.new

        @mutex.synchronize do
          sb = SendBuffer.new(framed_body)
          ps.send_buf = sb

          nva_list = build_request_headers(service, method, metadata)
          nva = nva_list.to_unsafe
          nvlen = nva_list.size

          src = LibNghttp2::DataSource.new
          boxed_src = Box.box(sb)
          src.ptr = boxed_src
          dp = LibNghttp2::DataProvider.new(source: src, read_callback: DATA_READ_CB)

          boxed_ps = Box.box(ps)
          stream_id = LibNghttp2.submit_request(@session, nil, nva, nvlen, pointerof(dp), boxed_ps)
          raise ConnectionError.new("submit_request failed: #{stream_id}") if stream_id < 0

          @pending_streams[stream_id] = ps
          @stream_boxes[stream_id] = boxed_ps
          @data_source_boxes[stream_id] = boxed_src

          # Wire up the cancel proc so PendingStream.cancel can send RST_STREAM.
          ps.cancel_proc = -> {
            @mutex.synchronize do
              LibNghttp2.submit_rst_stream(@session, LibNghttp2::FLAG_NONE, stream_id,
                LibNghttp2::NGHTTP2_CANCEL)
              flush_send rescue nil
            end
          }

          flush_send
        end

        ps
      end

      # Submits an HTTP/2 request for a full-duplex bidi stream.
      # The DATA provider starts in DEFERRED state (no data yet); callers enqueue
      # messages via PendingStream#send_outgoing, which calls session_resume_data.
      private def submit_bidi_stream_live(service : String, method : String,
                                          metadata : Metadata,
                                          send_queue_size : Int32 = 0) : PendingStream
        ps = PendingStream.new

        @mutex.synchronize do
          lsb = LiveSendBuffer.new(send_queue_size)
          ps.live_send_buf = lsb

          nva_list = build_request_headers(service, method, metadata)
          nva = nva_list.to_unsafe
          nvlen = nva_list.size

          src = LibNghttp2::DataSource.new
          boxed_src = Box.box(lsb)
          src.ptr = boxed_src
          dp = LibNghttp2::DataProvider.new(source: src, read_callback: DATA_READ_CB_LIVE)

          boxed_ps = Box.box(ps)
          stream_id = LibNghttp2.submit_request(@session, nil, nva, nvlen, pointerof(dp), boxed_ps)
          raise ConnectionError.new("submit_request failed: #{stream_id}") if stream_id < 0

          @pending_streams[stream_id] = ps
          @stream_boxes[stream_id] = boxed_ps
          @data_source_boxes[stream_id] = boxed_src

          ps.send_resume_proc = -> {
            @mutex.synchronize do
              LibNghttp2.session_resume_data(@session, stream_id)
              flush_send rescue nil
            end
          }

          ps.cancel_proc = -> {
            @mutex.synchronize do
              LibNghttp2.submit_rst_stream(@session, LibNghttp2::FLAG_NONE, stream_id,
                LibNghttp2::NGHTTP2_CANCEL)
              flush_send rescue nil
            end
          }

          # Send HTTP/2 HEADERS frame immediately; DATA will follow on-demand.
          flush_send
        end

        ps
      end

      private def build_request_headers(service : String, method : String,
                                        metadata : Metadata) : Array(LibNghttp2::Nv)
        nva_list = [
          make_nv(":method", "POST"),
          make_nv(":scheme", @use_tls ? "https" : "http"),
          make_nv(":path", "/#{service}/#{method}"),
          make_nv(":authority", @target_authority),
          make_nv("content-type", "application/grpc"),
          make_nv("te", "trailers"),
          make_nv("user-agent", "grpc-crystal/#{GRPC::VERSION}"),
        ] of LibNghttp2::Nv
        metadata.each_wire { |k, v| nva_list << make_nv(k, v) }
        nva_list
      end

      # ---- Callbacks ----

      def on_begin_headers_cb(frame : Void*) : Nil
        return unless frame_type(frame) == LibNghttp2::FRAME_HEADERS
        stream_id = frame_stream_id(frame)

        if call = @pending[stream_id]?
          call.begin_header_block
        elsif ps = @pending_streams[stream_id]?
          ps.begin_header_block
        end
      end

      def on_header_cb(frame : Void*, name : UInt8*, nlen : LibC::SizeT,
                       value : UInt8*, vlen : LibC::SizeT) : Nil
        stream_id = frame_stream_id(frame)
        key = String.new(name, nlen)
        val = String.new(value, vlen)

        if call = @pending[stream_id]?
          call.add_header(key, val)
        elsif ps = @pending_streams[stream_id]?
          ps.add_header(key, val)
        end
      end

      def on_data_chunk_cb(stream_id : Int32, data : UInt8*, len : LibC::SizeT) : Nil
        if call = @pending[stream_id]?
          call.response_body.write(Slice.new(data, len))
        elsif ps = @pending_streams[stream_id]?
          ps.receive_data(Slice.new(data, len))
        end
      end

      def on_frame_recv_cb(frame : Void*) : Nil
        return unless frame_end_stream?(frame)
        # Ignore SETTINGS ACK: FLAG_ACK == FLAG_END_STREAM == 0x01
        return if frame_type(frame) == LibNghttp2::FRAME_SETTINGS
        stream_id = frame_stream_id(frame)
        if call = @pending[stream_id]?
          call.complete
        elsif ps = @pending_streams[stream_id]?
          ps.finish
        end
      end

      def on_stream_close_cb(stream_id : Int32, error_code : UInt32) : Nil
        call = @pending.delete(stream_id)
        if call && error_code != LibNghttp2::NO_ERROR
          call.transport_error = stream_close_status(error_code)
        end
        call.complete if call # wake up waiter on error close
        ps = @pending_streams.delete(stream_id)
        if ps && error_code != LibNghttp2::NO_ERROR
          ps.transport_error = stream_close_status(error_code)
        end
        ps.finish if ps # wake up blocked reader on unexpected close
        @stream_boxes.delete(stream_id)
        @data_source_boxes.delete(stream_id)
      end

      private def stream_close_status(error_code : UInt32) : Status
        case error_code
        when LibNghttp2::NGHTTP2_CANCEL
          Status.cancelled("stream reset by peer")
        else
          Status.unknown("stream closed without valid grpc-status (http2 error code #{error_code})")
        end
      end
    end
  end
end
