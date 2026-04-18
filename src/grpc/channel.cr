require "./transport/http2_client_connection"
require "./endpoint"

module GRPC
  # Channel represents a client-side gRPC endpoint.
  # It manages HTTP/2 connections and is the entry point for RPC calls.
  #
  # Plain TCP examples:
  #   channel = GRPC::Channel.new("localhost:50051")
  #   channel = GRPC::Channel.new("http://api.example.com:8080")
  #
  # TLS examples:
  #   channel = GRPC::Channel.new("https://api.example.com:443")
  #   ctx = OpenSSL::SSL::Context::Client.new
  #   ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE  # skip cert check (testing only)
  #   channel = GRPC::Channel.new("https://localhost:50051", tls_context: ctx)
  class Channel
    alias ClientTransportFactory = Proc(String, Int32, Bool, OpenSSL::SSL::Context::Client?, EndpointConfig, Transport::ClientTransport)

    @endpoint : Endpoint
    @endpoint_config : EndpointConfig
    @tls_context : OpenSSL::SSL::Context::Client?
    @conn : Transport::ClientTransport?
    @transport_factory : ClientTransportFactory
    @mutex : Mutex
    @interceptors : Array(ClientInterceptor)
    @rate_limit_mutex : Mutex
    @rate_limit_starts : Deque(Time::Instant)
    @concurrency_permits : ::Channel(Nil)?

    getter endpoint : Endpoint
    getter endpoint_config : EndpointConfig

    # Create a channel to *address*.
    #
    # Address formats: "host:port", "http://host:port", "https://host:port".
    # HTTPS triggers TLS; use *tls_context* to supply a custom OpenSSL context
    # (e.g. to disable certificate verification for testing).
    def initialize(address : String,
                   interceptors : Array(ClientInterceptor) = [] of ClientInterceptor,
                   tls_context : OpenSSL::SSL::Context::Client? = nil,
                   endpoint_config : EndpointConfig = EndpointConfig.new,
                   transport_factory : ClientTransportFactory? = nil)
      initialize(
        Endpoint.parse(address),
        interceptors: interceptors,
        tls_context: tls_context,
        endpoint_config: endpoint_config,
        transport_factory: transport_factory,
      )
    end

    # Create a channel to *endpoint*.
    def initialize(endpoint : Endpoint,
                   interceptors : Array(ClientInterceptor) = [] of ClientInterceptor,
                   tls_context : OpenSSL::SSL::Context::Client? = nil,
                   endpoint_config : EndpointConfig = EndpointConfig.new,
                   transport_factory : ClientTransportFactory? = nil)
      @endpoint = endpoint
      @endpoint_config = endpoint_config
      @tls_context = tls_context
      @conn = nil
      @transport_factory = transport_factory || ->(host : String, port : Int32, use_tls : Bool, tls_ctx : OpenSSL::SSL::Context::Client?, config : EndpointConfig) {
        Transport::Http2ClientConnection.new(host, port, use_tls, tls_ctx, config).as(Transport::ClientTransport)
      }
      @mutex = Mutex.new
      @interceptors = interceptors
      @rate_limit_mutex = Mutex.new
      @rate_limit_starts = Deque(Time::Instant).new
      validate_endpoint_config!
      @concurrency_permits = build_concurrency_permits(endpoint_config)
    end

    # unary_call performs a single gRPC unary RPC.
    # This is the internal API used by generated client stubs.
    # Returns the raw unary response envelope with metadata and status.
    # If client interceptors are registered they wrap this call outermost-first.
    def unary_call(
      service : String,
      method : String,
      request_bytes : Bytes,
      ctx : ClientContext = ClientContext.new,
    ) : ResponseEnvelope
      wait_for_rate_limit_slot
      release = acquire_concurrency_slot

      if @interceptors.empty?
        begin
          get_or_create_connection.unary_call(service, method, request_bytes, ctx.effective_metadata)
        ensure
          release.call
        end
      else
        method_path = "/#{service}/#{method}"
        info = CallInfo.new(method_path, RPCKind::Unary)
        req = RequestEnvelope.new(info, request_bytes)
        base = UnaryClientCall.new do |_method_path, req_env, call_ctx|
          response = get_or_create_connection.unary_call(service, method, req_env.raw, call_ctx.effective_metadata)
          ResponseEnvelope.new(
            req_env.info,
            response.raw,
            response.status,
            response.initial_metadata,
            response.trailing_metadata,
            response.codec,
            response.descriptor
          )
        end
        chain = Interceptors.build_client_chain(@interceptors, base)
        begin
          chain.call(method_path, req, ctx)
        ensure
          release.call
        end
      end
    end

    # unary_call with a plain Metadata object — convenience overload for generated code.
    # Interceptors are NOT applied on this low-level overload.
    def unary_call(
      service : String,
      method : String,
      request_bytes : Bytes,
      metadata : Metadata,
    ) : ResponseEnvelope
      wait_for_rate_limit_slot
      release = acquire_concurrency_slot
      begin
        get_or_create_connection.unary_call(service, method, request_bytes, metadata)
      ensure
        release.call
      end
    end

    # open_server_stream initiates a server-streaming gRPC call.
    # Returns a RawServerStream that can be iterated to receive response messages.
    # This is the internal API used by generated client stubs.
    def open_server_stream(
      service : String,
      method : String,
      request_bytes : Bytes,
      ctx : ClientContext = ClientContext.new,
    ) : RawServerStream
      wait_for_rate_limit_slot
      release = acquire_concurrency_slot

      if @interceptors.empty?
        begin
          raw = get_or_create_connection.open_server_stream(service, method, request_bytes, ctx.effective_metadata)
          raw.with_on_finish(release)
        rescue ex
          release.call
          raise ex
        end
      else
        method_path = "/#{service}/#{method}"
        info = CallInfo.new(method_path, RPCKind::ServerStreaming)
        req = RequestEnvelope.new(info, request_bytes)
        base = ServerStreamClientCall.new do |_mp, req_env, call_ctx|
          get_or_create_connection.open_server_stream(service, method, req_env.raw, call_ctx.effective_metadata)
        end
        chain = Interceptors.build_client_chain(@interceptors, base)
        begin
          raw = chain.call(method_path, req, ctx)
          raw.with_on_finish(release)
        rescue ex
          release.call
          raise ex
        end
      end
    end

    # open_bidi_stream_live opens a true full-duplex bidi-streaming RPC.
    # Returns a RawBidiCall whose send_raw / close_send / each methods let the
    # caller interleave request messages and response messages freely.
    # *send_queue_size* limits how many outgoing frames may be buffered before
    # send_raw blocks the calling fiber (0 = unbounded).
    def open_bidi_stream_live(
      service : String,
      method : String,
      ctx : ClientContext = ClientContext.new,
      send_queue_size : Int32 = 0,
    ) : RawBidiCall
      wait_for_rate_limit_slot
      release = acquire_concurrency_slot

      method_path = "/#{service}/#{method}"
      if @interceptors.empty?
        begin
          raw = get_or_create_connection.open_bidi_stream_live(service, method, ctx.effective_metadata, send_queue_size)
          raw.with_on_finish(release)
        rescue ex
          release.call
          raise ex
        end
      else
        base = LiveBidiStreamClientCall.new do |_mp, call_ctx|
          get_or_create_connection.open_bidi_stream_live(service, method, call_ctx.effective_metadata, send_queue_size)
        end
        chain = Interceptors.build_client_chain(@interceptors, base)
        begin
          raw = chain.call(method_path, ctx)
          raw.with_on_finish(release)
        rescue ex
          release.call
          raise ex
        end
      end
    end

    # open_client_stream_live opens a live client-streaming RPC.
    # Returns a RawClientCall whose send_raw / close_and_recv methods let the
    # caller send messages incrementally then receive the server's single response.
    # *send_queue_size* limits how many outgoing frames may be buffered before
    # send_raw blocks the calling fiber (0 = unbounded).
    def open_client_stream_live(
      service : String,
      method : String,
      ctx : ClientContext = ClientContext.new,
      send_queue_size : Int32 = 0,
    ) : RawClientCall
      wait_for_rate_limit_slot
      release = acquire_concurrency_slot

      method_path = "/#{service}/#{method}"
      if @interceptors.empty?
        begin
          raw = get_or_create_connection.open_client_stream_live(service, method, ctx.effective_metadata, send_queue_size)
          raw.with_on_finish(release)
        rescue ex
          release.call
          raise ex
        end
      else
        base = LiveClientStreamClientCall.new do |_mp, call_ctx|
          get_or_create_connection.open_client_stream_live(service, method, call_ctx.effective_metadata, send_queue_size)
        end
        chain = Interceptors.build_client_chain(@interceptors, base)
        begin
          raw = chain.call(method_path, ctx)
          raw.with_on_finish(release)
        rescue ex
          release.call
          raise ex
        end
      end
    end

    def close : Nil
      @mutex.synchronize { @conn.try &.close }
    end

    private def get_or_create_connection : Transport::ClientTransport
      @mutex.synchronize do
        c = @conn
        if c.nil? || c.closed?
          c = @transport_factory.call(@endpoint.host, @endpoint.port, @endpoint.tls?, @tls_context, @endpoint_config)
          @conn = c
        end
        c
      end
    end

    private def validate_endpoint_config! : Nil
      validate_positive_span(@endpoint_config.connect_timeout, "endpoint_config.connect_timeout")
      validate_positive_span(@endpoint_config.tcp_keepalive, "endpoint_config.tcp_keepalive")
      validate_keepalive_config(@endpoint_config.keepalive)
      validate_concurrency_limit(@endpoint_config.concurrency_limit)
      validate_rate_limit(@endpoint_config.rate_limit)
    end

    private def validate_positive_span(span : Time::Span?, label : String) : Nil
      return unless span
      raise ArgumentError.new("#{label} must be > 0") if span <= Time::Span.zero
    end

    private def validate_keepalive_config(keepalive : KeepaliveParams?) : Nil
      return unless keepalive
      validate_positive_span(keepalive.interval, "endpoint_config.keepalive.interval")
      validate_positive_span(keepalive.timeout, "endpoint_config.keepalive.timeout")
    end

    private def validate_concurrency_limit(limit : Int32?) : Nil
      return unless limit
      raise ArgumentError.new("endpoint_config.concurrency_limit must be > 0") if limit <= 0
    end

    private def validate_rate_limit(rate : RateLimitConfig?) : Nil
      return unless rate
      raise ArgumentError.new("endpoint_config.rate_limit.limit must be > 0") if rate.limit == 0
      validate_positive_span(rate.period, "endpoint_config.rate_limit.period")
    end

    private def build_concurrency_permits(config : EndpointConfig) : ::Channel(Nil)?
      limit = config.concurrency_limit
      return unless limit

      permits = ::Channel(Nil).new(limit)
      limit.times { permits.send(nil) }
      permits
    end

    private def wait_for_rate_limit_slot : Nil
      cfg = @endpoint_config.rate_limit
      return unless cfg

      limit = cfg.limit.to_i
      period = cfg.period

      loop do
        wait_for = Time::Span.zero
        acquired = false

        @rate_limit_mutex.synchronize do
          now = Time.instant
          cutoff = now - period
          while !@rate_limit_starts.empty? && @rate_limit_starts.first < cutoff
            @rate_limit_starts.shift
          end

          if @rate_limit_starts.size < limit
            @rate_limit_starts << now
            acquired = true
          else
            wait_for = (@rate_limit_starts.first + period) - now
          end
        end

        return if acquired
        sleep(wait_for) if wait_for > Time::Span.zero
      end
    end

    private def acquire_concurrency_slot : -> Nil
      permits = @concurrency_permits
      return -> { } unless permits

      permits.receive
      released = false

      -> {
        unless released
          released = true
          permits.send(nil)
        end
      }
    end
  end
end
