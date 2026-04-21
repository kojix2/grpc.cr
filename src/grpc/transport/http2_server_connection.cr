require "log"

require "./http2_connection"
require "./grpc_deframer"
require "./interface"

module GRPC
  module Transport
    # ResponseContext bundles the framed body bytes and trailer strings for one
    # gRPC response.  A pointer to this object is stashed in DataSource.ptr so
    # the DATA read callback can both send body bytes and, when the body is
    # exhausted, submit the trailing HEADERS frame (grpc-status / grpc-message).
    class ResponseContext
      property data : Bytes
      property offset : Int32
      property stream_id : Int32
      # Keep the strings alive so that the Nv pointers inside the callback
      # remain valid for the duration of the submit_trailer call.
      property status_name : String
      property status_value : String
      property message_name : String
      property message_value : String
      property details_name : String?
      property details_value : String?

      def initialize(@data : Bytes, @stream_id : Int32,
                     @status_value : String, @message_value : String,
                     @details_value : String? = nil)
        @offset = 0
        @status_name = "grpc-status"
        @message_name = "grpc-message"
        @details_name = @details_value ? "grpc-status-details-bin" : nil
      end

      def remaining : Int32
        @data.size - @offset
      end
    end

    # Http2ServerConnection handles one accepted TCP connection on the server side.
    # It receives gRPC requests, dispatches to registered services, and sends responses.
    class Http2ServerConnection < Http2Connection
      include ServerTransport

      LOGGER = ::Log.for(self)

      # Per-stream state for live request-streaming RPCs.
      class LiveRequestState
        getter requests : RawRequestStream
        getter kind : Symbol
        getter deframer : GrpcDeframer
        property error_status : Status?

        def initialize(@kind : Symbol)
          @channel = ::Channel(Bytes?).new(256)
          @requests = RawRequestStream.new(@channel)
          @deframer = GrpcDeframer.new
          @error_status = nil
          @closed = Atomic(Bool).new(false)
        end

        def push(message : Bytes) : Nil
          return if @closed.get
          @channel.send(message)
        end

        def close : Nil
          # compare_and_set returns {old_value, success?}; only the winner executes.
          return unless @closed.compare_and_set(false, true)[1]
          @channel.send(nil)
        rescue
          # no-op
        end
      end

      # DATA read callback for nghttp2 when sending a response body.
      # Must be a module-level constant (non-closure) proc.
      #
      # When the body is exhausted the callback also submits the trailing
      # HEADERS frame (grpc-status / grpc-message).  Per nghttp2 documentation,
      # nghttp2_submit_trailer is safe to call from the data source read callback.
      DATA_READ_CB = LibNghttp2::DataSourceReadCallback.new do |session, stream_id, buf, length, data_flags, source, _user_data|
        ctx = Box(ResponseContext).unbox(source.value.ptr)
        to_copy = Math.min(length.to_i, ctx.remaining).to_i
        if to_copy > 0
          buf.copy_from(ctx.data.to_unsafe + ctx.offset, to_copy)
          ctx.offset += to_copy
        end
        if ctx.remaining == 0
          data_flags.value |= LibNghttp2::DATA_FLAG_EOF | LibNghttp2::DATA_FLAG_NO_END_STREAM
          # Submit trailers from inside the callback (safe per nghttp2 docs).
          trailer_nva = [] of LibNghttp2::Nv
          sn = ctx.status_name.to_slice
          sv = ctx.status_value.to_slice
          trailer_nva << LibNghttp2::Nv.new(name: sn.to_unsafe, value: sv.to_unsafe,
            namelen: sn.size, valuelen: sv.size,
            flags: LibNghttp2::NV_FLAG_NONE)
          mn = ctx.message_name.to_slice
          mv = ctx.message_value.to_slice
          trailer_nva << LibNghttp2::Nv.new(name: mn.to_unsafe, value: mv.to_unsafe,
            namelen: mn.size, valuelen: mv.size,
            flags: LibNghttp2::NV_FLAG_NONE)
          if details_name = ctx.details_name
            if details_value = ctx.details_value
              dn = details_name.to_slice
              dv = details_value.to_slice
              trailer_nva << LibNghttp2::Nv.new(name: dn.to_unsafe, value: dv.to_unsafe,
                namelen: dn.size, valuelen: dv.size,
                flags: LibNghttp2::NV_FLAG_NONE)
            end
          end
          rc = LibNghttp2.submit_trailer(session, stream_id, trailer_nva.to_unsafe, trailer_nva.size)
          next LibNghttp2::ERR_CALLBACK_FAILURE.to_i64 if rc < 0
        end
        to_copy.to_i64
      end

      # DATA read callback for server-streaming: sends one message chunk.
      # Sets DATA_FLAG_EOF | DATA_FLAG_NO_END_STREAM when the buffer is exhausted
      # so the stream stays open for subsequent data or trailer submission.
      STREAMING_DATA_READ_CB = LibNghttp2::DataSourceReadCallback.new do |_session, _stream_id, buf, length, data_flags, source, _user_data|
        sb = Box(SendBuffer).unbox(source.value.ptr)
        to_copy = Math.min(length.to_i, sb.remaining).to_i
        if to_copy > 0
          buf.copy_from(sb.data.to_unsafe + sb.offset, to_copy)
          sb.offset += to_copy
        end
        if sb.remaining == 0
          data_flags.value |= LibNghttp2::DATA_FLAG_EOF | LibNghttp2::DATA_FLAG_NO_END_STREAM
        end
        to_copy.to_i64
      end

      # service_full_name => Service
      @services : Hash(String, Service)
      # registered server-side interceptors (applied to all RPC variants)
      @interceptors : Array(ServerInterceptor)
      # GC anchor for the TLS socket (prevents premature collection)
      @tls_socket_anchor : OpenSSL::SSL::Socket::Server?
      # stream_id => ResponseContext (GC anchor for unary data provider source.ptr)
      @response_ctxs : Hash(Int32, ResponseContext)
      # stream_id => Void* (GC anchor for StreamData box)
      @stream_boxes : Hash(Int32, Void*)
      # stream_id => SendBuffer (GC anchor for streaming chunk currently being flushed)
      @stream_send_bufs : Hash(Int32, SendBuffer)
      # stream_id => [Void*] (GC anchors for nghttp2 DataSource.ptr boxes)
      @stream_send_boxes : Hash(Int32, Array(Void*))
      # stream_id => queued response chunks waiting for prior DATA send completion
      @stream_pending_chunks : Hash(Int32, Deque(Bytes))
      # stream_id => terminal trailers deferred until all DATA frames are sent
      @stream_pending_trailers : Hash(Int32, Status)
      # stream_id => whether a DATA frame is currently in flight
      @stream_data_in_flight : Set(Int32)
      # stream_id => live request-streaming state
      @live_request_states : Hash(Int32, LiveRequestState)
      # stream_id => active server context (for deadline/cancel propagation)
      @stream_contexts : Hash(Int32, ServerContext)
      # stream_id => whether response headers were already sent
      @stream_headers_sent : Set(Int32)
      # stream_id => whether terminal response/trailers were already sent
      @stream_terminated : Set(Int32)
      # stream_id => stop signal for deadline watcher fiber
      @deadline_watch_stops : Hash(Int32, ::Channel(Nil))

      def initialize(socket : IO, services : Hash(String, Service),
                     interceptors : Array(ServerInterceptor) = [] of ServerInterceptor,
                     peer_address : String = "unknown",
                     tls_socket : OpenSSL::SSL::Socket::Server? = nil)
        super(socket, peer_address)
        @services = services
        @interceptors = interceptors
        @tls_socket_anchor = tls_socket
        @response_ctxs = {} of Int32 => ResponseContext
        @stream_boxes = {} of Int32 => Void*
        @stream_send_bufs = {} of Int32 => SendBuffer
        @stream_send_boxes = {} of Int32 => Array(Void*)
        @stream_pending_chunks = {} of Int32 => Deque(Bytes)
        @stream_pending_trailers = {} of Int32 => Status
        @stream_data_in_flight = Set(Int32).new
        @live_request_states = {} of Int32 => LiveRequestState
        @stream_contexts = {} of Int32 => ServerContext
        @stream_headers_sent = Set(Int32).new
        @stream_terminated = Set(Int32).new
        @deadline_watch_stops = {} of Int32 => ::Channel(Nil)
        setup_session(server_side: true)
      end

      # ---- Callbacks ----

      def on_begin_headers_cb(frame : Void*) : Nil
        return unless frame_type(frame) == LibNghttp2::FRAME_HEADERS
        stream_id = frame_stream_id(frame)
        return if stream_id <= 0

        sd = StreamData.new
        boxed = Box.box(sd)
        @stream_boxes[stream_id] = boxed
        LibNghttp2.session_set_stream_user_data(@session, stream_id, boxed)
      end

      def on_header_cb(frame : Void*, name : UInt8*, nlen : LibC::SizeT,
                       value : UInt8*, vlen : LibC::SizeT) : Nil
        stream_id = frame_stream_id(frame)
        sd = stream_data_for(stream_id)
        return unless sd

        begin
          sd.headers.add_wire(String.new(name, nlen), String.new(value, vlen))
        rescue ex : ArgumentError
          sd.header_error = Status.invalid_argument(ex.message || "invalid request metadata")
        end
      end

      def on_data_chunk_cb(stream_id : Int32, data : UInt8*, len : LibC::SizeT) : Nil
        sd = stream_data_for(stream_id)
        return unless sd

        chunk = Slice.new(data, len)
        if target = request_stream_target(sd)
          service, method_name, kind = target
          state = @live_request_states[stream_id]?
          unless state
            state = LiveRequestState.new(kind)
            @live_request_states[stream_id] = state
            spawn dispatch_live_request_stream(stream_id, sd, service, method_name, state)
          end
          append_live_request_chunk(state, chunk)
          return
        end

        sd.body.write(chunk)
      end

      def on_frame_recv_cb(frame : Void*) : Nil
        return unless frame_end_stream?(frame)
        # Ignore SETTINGS ACK: FLAG_ACK == FLAG_END_STREAM == 0x01.
        return if frame_type(frame) == LibNghttp2::FRAME_SETTINGS
        stream_id = frame_stream_id(frame)
        return if stream_id <= 0
        sd = stream_data_for(stream_id)
        return unless sd

        if state = @live_request_states[stream_id]?
          finish_live_request_stream(stream_id, state)
          return
        end

        if target = request_stream_target(sd)
          service, method_name, kind = target
          state = LiveRequestState.new(kind)
          @live_request_states[stream_id] = state
          spawn dispatch_live_request_stream(stream_id, sd, service, method_name, state)
          finish_live_request_stream(stream_id, state)
          return
        end

        # Dispatch in a separate fiber so the recv loop is not blocked.
        spawn dispatch_request(stream_id, sd)
      end

      def on_stream_close_cb(stream_id : Int32, error_code : UInt32) : Nil
        @stream_boxes.delete(stream_id)
        @response_ctxs.delete(stream_id)
        @stream_send_bufs.delete(stream_id)
        @stream_send_boxes.delete(stream_id)
        @stream_pending_chunks.delete(stream_id)
        @stream_pending_trailers.delete(stream_id)
        @stream_data_in_flight.delete(stream_id)
        live_state = @live_request_states[stream_id]?
        if error_code != 0
          if state = @live_request_states.delete(stream_id)
            state.close
          end
          if ctx = @stream_contexts.delete(stream_id)
            ctx.cancel
          end
        elsif live_state.nil?
          if ctx = @stream_contexts.delete(stream_id)
            ctx.cancel
          end
        end
        if stop = @deadline_watch_stops.delete(stream_id)
          stop.send(nil) rescue nil
        end
      end

      def on_frame_send_cb(frame : Void*) : Nil
        return unless frame_type(frame) == LibNghttp2::FRAME_DATA
        stream_id = frame_stream_id(frame)
        return if stream_id <= 0

        next_chunk : Bytes? = nil
        @mutex.synchronize do
          @stream_data_in_flight.delete(stream_id)
          if queue = @stream_pending_chunks[stream_id]?
            next_chunk = queue.shift?
            @stream_pending_chunks.delete(stream_id) if queue.empty?
          end
          if chunk = next_chunk
            submit_stream_chunk_now(stream_id, chunk, flush: false)
          elsif trailer = @stream_pending_trailers.delete(stream_id)
            submit_stream_trailers_now(stream_id, trailer, flush: false)
          end
        end
      end

      # ---- Dispatch ----

      private def dispatch_request(stream_id : Int32, sd : StreamData) : Nil
        ctx : ServerContext? = nil
        if error = sd.header_error
          send_error(stream_id, error.code, error.message)
          return
        end

        path = sd.headers.get(":path") || ""
        meta = build_metadata(sd.headers)
        ctx = ServerContext.new(@peer_address, meta, parse_deadline(sd.headers))
        register_stream_context(stream_id, ctx)

        if deadline_exceeded?(ctx)
          send_error(stream_id, StatusCode::DEADLINE_EXCEEDED, "deadline exceeded")
          return
        end

        # path format: "/{PackageService}/{MethodName}"
        parts = path.split("/")
        service_full_name = parts[1]? || ""
        method_name = parts[2]? || ""

        service = @services[service_full_name]?
        unless service
          send_error(stream_id, StatusCode::UNIMPLEMENTED, "service #{service_full_name} not found")
          return
        end

        route_request(stream_id, service, method_name, sd.body_bytes, ctx)
      rescue ex : StatusError
        send_error(stream_id, ex.code, ex.message)
      rescue ex
        send_error(stream_id, StatusCode::INTERNAL, ex.message || "internal error")
      ensure
        unregister_stream_context(stream_id, ctx)
      end

      private def dispatch_live_request_stream(stream_id : Int32, sd : StreamData,
                                               service : Service, method_name : String,
                                               state : LiveRequestState) : Nil
        ctx : ServerContext? = nil
        if error = sd.header_error
          send_error(stream_id, error.code, error.message)
          return
        end

        meta = build_metadata(sd.headers)
        ctx = ServerContext.new(@peer_address, meta, parse_deadline(sd.headers))
        register_stream_context(stream_id, ctx)

        if deadline_exceeded?(ctx)
          send_error(stream_id, StatusCode::DEADLINE_EXCEEDED, "deadline exceeded")
          return
        end

        case state.kind
        when :client
          response_bytes, status = dispatch_client_stream(method_name, state.requests, ctx, service)
          if err = state.error_status
            send_error(stream_id, err.code, err.message)
          else
            send_response(stream_id, response_bytes, status)
          end
        when :bidi
          send_stream_headers(stream_id)
          writer = RawResponseStream.new(->(framed : Bytes) {
            send_stream_chunk(stream_id, framed)
          })
          status = dispatch_bidi_stream(method_name, state.requests, ctx, writer, service)
          send_stream_trailers(stream_id, state.error_status || status)
        else
          send_error(stream_id, StatusCode::UNIMPLEMENTED, "unknown streaming kind")
        end
      rescue ex : StatusError
        send_error(stream_id, ex.code, ex.message)
      rescue ex
        send_error(stream_id, StatusCode::INTERNAL, ex.message || "internal error")
      ensure
        state.close
        @mutex.synchronize { @live_request_states.delete(stream_id) }
        unregister_stream_context(stream_id, ctx)
      end

      # route_request handles unary and server-streaming RPCs only.
      # Client-streaming and bidi-streaming are handled live via dispatch_live_request_stream
      # (driven by on_data_chunk_cb / on_frame_recv_cb) and never reach this method.
      private def route_request(stream_id : Int32, service : Service,
                                method_name : String, request_bytes : Bytes,
                                ctx : ServerContext) : Nil
        if service.server_streaming?(method_name)
          decoded, _ = decode_message(request_bytes)
          send_stream_headers(stream_id)
          writer = RawResponseStream.new(->(framed : Bytes) {
            ctx.check_active!
            send_stream_chunk(stream_id, framed)
          })
          status = dispatch_server_stream(method_name, decoded, ctx, writer, service)
          send_stream_trailers(stream_id, status)
        else
          dispatch_unary(stream_id, method_name, request_bytes, ctx, service)
        end
      end

      private def dispatch_server_stream(method_name : String, decoded : Bytes,
                                         ctx : ServerContext, writer : RawResponseStream,
                                         service : Service) : Status
        if @interceptors.empty?
          service.dispatch_server_stream(method_name, decoded, ctx, writer)
        else
          full_path = "/#{service.service_full_name}/#{method_name}"
          info = CallInfo.new(full_path, RPCKind::ServerStreaming)
          request = RequestEnvelope.new(info, decoded)
          base = ServerStreamServerCall.new do |_, req, call_ctx, writer_inner|
            service.dispatch_server_stream(method_name, req.raw, call_ctx, writer_inner)
          end
          chain = Interceptors.build_server_chain(@interceptors, base)
          chain.call(full_path, request, ctx, writer)
        end
      end

      private def dispatch_client_stream(method_name : String, requests : RawRequestStream,
                                         ctx : ServerContext,
                                         service : Service) : {Bytes, Status}
        if @interceptors.empty?
          service.dispatch_client_stream(method_name, requests, ctx)
        else
          base = ClientStreamServerCall.new do |_, reqs, call_ctx|
            body, status = service.dispatch_client_stream(method_name, reqs, call_ctx)
            info = CallInfo.new("/#{service.service_full_name}/#{method_name}", RPCKind::ClientStreaming)
            ResponseEnvelope.new(info, body, status)
          end
          full_path = "/#{service.service_full_name}/#{method_name}"
          chain = Interceptors.build_server_chain(@interceptors, base)
          response = chain.call(full_path, requests, ctx)
          {response.raw, response.status}
        end
      end

      private def dispatch_bidi_stream(method_name : String, requests : RawRequestStream,
                                       ctx : ServerContext, writer : RawResponseStream,
                                       service : Service) : Status
        if @interceptors.empty?
          service.dispatch_bidi_stream(method_name, requests, ctx, writer)
        else
          base = BidiStreamServerCall.new do |_, reqs, call_ctx, writer_inner|
            service.dispatch_bidi_stream(method_name, reqs, call_ctx, writer_inner)
          end
          full_path = "/#{service.service_full_name}/#{method_name}"
          chain = Interceptors.build_server_chain(@interceptors, base)
          chain.call(full_path, requests, ctx, writer)
        end
      end

      private def dispatch_unary(stream_id : Int32, method_name : String,
                                 request_bytes : Bytes, ctx : ServerContext,
                                 service : Service) : Nil
        decoded, _ = decode_message(request_bytes)
        if @interceptors.empty?
          response_bytes, status = service.dispatch(method_name, decoded, ctx)
          ctx.check_active!
          send_response(stream_id, response_bytes, status)
        else
          full_path = "/#{service.service_full_name}/#{method_name}"
          info = CallInfo.new(full_path, RPCKind::Unary)
          request = RequestEnvelope.new(info, decoded)
          base = UnaryServerCall.new do |_, req, call_ctx|
            body, status = service.dispatch(method_name, req.raw, call_ctx)
            ResponseEnvelope.new(req.info, body, status).as(ResponseEnvelope)
          end
          chain = Interceptors.build_server_chain(@interceptors, base)
          response = chain.call(full_path, request, ctx)
          ctx.check_active!
          send_response(stream_id, response.raw, response.status)
        end
      end

      # ---- Response sending ----

      private def send_stream_headers(stream_id : Int32) : Nil
        @mutex.synchronize do
          return if stream_terminated?(stream_id)
          nva = StaticArray[
            make_nv(":status", "200"),
            make_nv("content-type", "application/grpc"),
          ]
          rc = LibNghttp2.submit_headers(@session, LibNghttp2::FLAG_NONE, stream_id, nil,
            nva.to_unsafe, nva.size, nil)
          raise_submit_error("submit_headers", rc) if rc < 0
          @stream_headers_sent.add(stream_id)
          flush_send
        end
      end

      private def send_stream_chunk(stream_id : Int32, framed_bytes : Bytes) : Nil
        @mutex.synchronize do
          return if stream_terminated?(stream_id)
          if @stream_data_in_flight.includes?(stream_id)
            (@stream_pending_chunks[stream_id] ||= Deque(Bytes).new) << framed_bytes
          else
            submit_stream_chunk_now(stream_id, framed_bytes, flush: true)
          end
        end
      end

      private def submit_stream_chunk_now(stream_id : Int32, framed_bytes : Bytes, flush : Bool) : Nil
        sb = SendBuffer.new(framed_bytes)
        @stream_send_bufs[stream_id] = sb
        boxed = Box.box(sb)
        (@stream_send_boxes[stream_id] ||= [] of Void*) << boxed
        src = LibNghttp2::DataSource.new
        src.ptr = boxed
        dp = LibNghttp2::DataProvider.new(source: src, read_callback: STREAMING_DATA_READ_CB)
        rc = LibNghttp2.submit_data(@session, LibNghttp2::FLAG_NONE, stream_id, pointerof(dp))
        raise ConnectionError.new("submit_data failed: #{String.new(LibNghttp2.strerror(rc))} (#{rc})") if rc < 0
        @stream_data_in_flight.add(stream_id)
        flush_send if flush
      end

      private def send_stream_trailers(stream_id : Int32, status : Status) : Nil
        @mutex.synchronize do
          return if stream_terminated?(stream_id)
          if @stream_data_in_flight.includes?(stream_id) || @stream_pending_chunks[stream_id]?
            @stream_pending_trailers[stream_id] = status
          else
            submit_stream_trailers_now(stream_id, status, flush: true)
          end
        end
      end

      private def submit_stream_trailers_now(stream_id : Int32, status : Status, flush : Bool) : Nil
        return if stream_terminated?(stream_id)
        mark_stream_terminated(stream_id)
        trailer_nva = build_status_trailers(status)
        rc = LibNghttp2.submit_trailer(@session, stream_id, trailer_nva.to_unsafe, trailer_nva.size)
        raise_submit_error("submit_trailer", rc) if rc < 0
        flush_send if flush
      end

      private def send_response(stream_id : Int32, body : Bytes, status : Status) : Nil
        @mutex.synchronize do
          return if stream_terminated?(stream_id)
          mark_stream_terminated(stream_id)
          framed = Codec.encode(body)
          resp_ctx = ResponseContext.new(
            framed, stream_id,
            status.code.value.to_s,
            percent_encode(status.message),
            TrailerCodec.encode_bin(status.details)
          )
          @response_ctxs[stream_id] = resp_ctx

          nva = StaticArray[
            make_nv(":status", "200"),
            make_nv("content-type", "application/grpc"),
          ]

          src = LibNghttp2::DataSource.new
          src.ptr = Box.box(resp_ctx)
          dp = LibNghttp2::DataProvider.new(source: src, read_callback: DATA_READ_CB)

          rc = LibNghttp2.submit_response(@session, stream_id, nva.to_unsafe, nva.size, pointerof(dp))
          raise_submit_error("submit_response", rc) if rc < 0
          flush_send
        end
      end

      private def send_error(stream_id : Int32, code : StatusCode, message : String) : Nil
        @mutex.synchronize do
          return if stream_terminated?(stream_id)
          mark_stream_terminated(stream_id)
          nva = StaticArray[
            make_nv(":status", "200"),
            make_nv("content-type", "application/grpc"),
          ]
          @stream_headers_sent.add(stream_id)
          headers_rc = LibNghttp2.submit_headers(@session, LibNghttp2::FLAG_NONE, stream_id, nil,
            nva.to_unsafe, nva.size, nil)
          raise_submit_error("submit_headers", headers_rc) if headers_rc < 0

          trailer_nva = build_status_trailers(Status.new(code, message))
          trailer_rc = LibNghttp2.submit_trailer(@session, stream_id, trailer_nva.to_unsafe, trailer_nva.size)
          raise_submit_error("submit_trailer", trailer_rc) if trailer_rc < 0
          flush_send
        end
      end

      private def build_status_trailers(status : Status) : Array(LibNghttp2::Nv)
        trailers = [
          make_nv("grpc-status", status.code.value.to_s),
          make_nv("grpc-message", percent_encode(status.message)),
        ] of LibNghttp2::Nv
        if encoded_details = TrailerCodec.encode_bin(status.details)
          trailers << make_nv("grpc-status-details-bin", encoded_details)
        end
        trailers
      end

      # ---- Helpers ----

      private def raise_submit_error(operation : String, rc : Int32) : NoReturn
        raise ConnectionError.new("#{operation} failed: #{String.new(LibNghttp2.strerror(rc))} (#{rc})")
      end

      private def decode_message(data : Bytes) : {Bytes, Int32}
        Codec.decode(data)
      end

      private def request_stream_target(sd : StreamData) : {Service, String, Symbol}?
        path = sd.headers.get(":path") || ""
        parts = path.split("/")
        service_full_name = parts[1]? || ""
        method_name = parts[2]? || ""
        service = @services[service_full_name]?
        return unless service
        return {service, method_name, :client} if service.client_streaming?(method_name)
        return {service, method_name, :bidi} if service.bidi_streaming?(method_name)
        nil
      end

      private def append_live_request_chunk(state : LiveRequestState, chunk : Bytes) : Nil
        return if state.error_status

        state.deframer.append(chunk)
        state.deframer.drain_messages.each { |message| state.push(message) }
      rescue ex : StatusError
        state.error_status = ex.status
        state.close
      rescue ex
        state.error_status = Status.internal(ex.message || "failed to decode request stream")
        state.close
      end

      private def finish_live_request_stream(stream_id : Int32, state : LiveRequestState) : Nil
        if state.deframer.remainder_size > 0 && state.error_status.nil?
          state.error_status = Status.internal("incomplete gRPC frame body")
        end
        state.close
      end

      private def register_stream_context(stream_id : Int32, ctx : ServerContext) : Nil
        stop = ::Channel(Nil).new(1)
        @mutex.synchronize do
          @stream_contexts[stream_id] = ctx
          @deadline_watch_stops[stream_id] = stop
        end
        start_deadline_watcher(stream_id, ctx, stop)
      end

      private def unregister_stream_context(stream_id : Int32, ctx : ServerContext?) : Nil
        return unless ctx
        @mutex.synchronize do
          current = @stream_contexts[stream_id]?
          if current.same?(ctx)
            @stream_contexts.delete(stream_id)
            if stop = @deadline_watch_stops.delete(stream_id)
              stop.send(nil) rescue nil
            end
          end
        end
      end

      private def start_deadline_watcher(stream_id : Int32, ctx : ServerContext, stop : ::Channel(Nil)) : Nil
        deadline = ctx.deadline
        return unless deadline

        spawn do
          remaining = deadline - Time.utc
          timed_out = false
          if remaining > Time::Span.zero
            select
            when stop.receive
            when timeout(remaining)
              timed_out = true
            end
          else
            timed_out = true
          end

          if timed_out
            should_enforce = @mutex.synchronize do
              current = @stream_contexts[stream_id]?
              !current.nil? && current.same?(ctx) && !stream_terminated?(stream_id)
            end
            if should_enforce
              ctx.cancel
              if state = @live_request_states[stream_id]?
                state.close
              end

              status = Status.new(StatusCode::DEADLINE_EXCEEDED, "deadline exceeded")
              if @mutex.synchronize { @stream_headers_sent.includes?(stream_id) }
                send_stream_trailers(stream_id, status)
              else
                send_error(stream_id, status.code, status.message)
              end
            end
          end
        end
      end

      private def stream_terminated?(stream_id : Int32) : Bool
        @stream_terminated.includes?(stream_id)
      end

      private def mark_stream_terminated(stream_id : Int32) : Nil
        @stream_terminated.add(stream_id)
      end

      private def deadline_exceeded?(ctx : ServerContext) : Bool
        return false unless deadline = ctx.deadline
        Time.utc >= deadline
      end

      private def build_metadata(headers : Metadata) : Metadata
        meta = Metadata.new
        headers.each_value do |k, v|
          next if k.starts_with?(":")
          next if k == "content-type" || k == "te" || k == "grpc-timeout"
          case v
          when String then meta.add(k, v)
          when Bytes  then meta.add_bin(k, v)
          end
        end
        meta
      end

      # parse_deadline reads grpc-timeout from headers and returns an absolute deadline.
      private def parse_deadline(headers : Metadata) : Time?
        timeout_str = headers.get("grpc-timeout")
        return if timeout_str.nil? || timeout_str.empty?

        # Format: integer followed by unit (H=hours, M=minutes, S=seconds, m=ms, u=us, n=ns)
        raise StatusError.new(StatusCode::INVALID_ARGUMENT, "invalid grpc-timeout") if timeout_str.size < 2
        unit = timeout_str[-1]
        value = timeout_str[0..-2].to_i64?
        raise StatusError.new(StatusCode::INVALID_ARGUMENT, "invalid grpc-timeout") unless value
        raise StatusError.new(StatusCode::INVALID_ARGUMENT, "invalid grpc-timeout") if value < 0

        span = timeout_unit_to_span(unit, value)
        raise StatusError.new(StatusCode::INVALID_ARGUMENT, "invalid grpc-timeout") unless span
        Time.utc + span
      end

      private def timeout_unit_to_span(unit : Char, value : Int64) : Time::Span?
        case unit
        when 'H' then value.hours
        when 'M' then value.minutes
        when 'S' then value.seconds
        when 'm' then value.milliseconds
        when 'u' then (value / 1000.0).seconds
        when 'n' then (value / 1_000_000.0).seconds
        end
      end

      # percent_encode applies RFC-3986 percent-encoding for the grpc-message trailer.
      private def percent_encode(s : String) : String
        String.build do |io|
          s.each_byte do |byte|
            c = byte.chr
            if c.alphanumeric? || "-_.~".includes?(c)
              io << c
            else
              io << '%' << byte.to_s(16, upcase: true).rjust(2, '0')
            end
          end
        end
      end
    end
  end
end
