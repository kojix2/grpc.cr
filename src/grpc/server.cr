require "log"

require "./transport/http2_server_connection"

module GRPC
  # Server hosts registered gRPC services and dispatches incoming RPCs.
  #
  # Plain TCP example:
  #   server = GRPC::Server.new
  #   server.handle GreeterImpl.new
  #   server.listen "0.0.0.0", 50051
  #
  # TLS example:
  #   server = GRPC::Server.new
  #   server.use_tls(cert: "server.crt", key: "server.key")
  #   server.handle GreeterImpl.new
  #   server.listen "0.0.0.0", 50051
  class Server
    LOGGER = ::Log.for(self)

    alias ServerTransportFactory = Proc(IO, Hash(String, Service), Array(ServerInterceptor), String, OpenSSL::SSL::Socket::Server?, Transport::ServerTransport)

    @services : Hash(String, Service)
    @tcp_server : TCPServer?
    @interceptors : Array(ServerInterceptor)
    @tls_context : OpenSSL::SSL::Context::Server?
    @transport_factory : ServerTransportFactory
    @health_reporter : Health::Reporter?
    @health_service : Health::Service?
    @reflection_service : Reflection::Service?
    @active_transports : Hash(UInt64, Transport::ServerTransport)
    @transport_mutex : Mutex

    def initialize(transport_factory : ServerTransportFactory? = nil)
      @services = {} of String => Service
      @tcp_server = nil
      @interceptors = [] of ServerInterceptor
      @tls_context = nil
      @health_reporter = nil
      @health_service = nil
      @reflection_service = nil
      @active_transports = {} of UInt64 => Transport::ServerTransport
      @transport_mutex = Mutex.new
      @transport_factory = transport_factory || ->(io : IO, services : Hash(String, Service), interceptors : Array(ServerInterceptor), peer : String, tls_sock : OpenSSL::SSL::Socket::Server?) {
        Transport::Http2ServerConnection.new(io, services, interceptors, peer, tls_sock).as(Transport::ServerTransport)
      }
    end

    # handle registers a service implementation with the server.
    def handle(service : Service) : self
      @services[service.service_full_name] = service
      @reflection_service.try &.register_service(service.service_full_name)
      if service.is_a?(Health::Service)
        @reflection_service.try &.add_file_descriptor(Health::Service::FILE_DESCRIPTOR_PROTO_BYTES)
      end
      self
    end

    # use_tls configures the server to accept TLS connections.
    # *cert* is the path to the PEM-encoded certificate chain file.
    # *key* is the path to the PEM-encoded private key file.
    # Must be called before listen/bind.
    def use_tls(cert : String, key : String) : self
      ctx = OpenSSL::SSL::Context::Server.new
      ctx.certificate_chain = cert
      ctx.private_key = key
      ctx.alpn_protocol = "h2"
      @tls_context = ctx
      self
    end

    # use_tls with a pre-built OpenSSL context for full control over TLS settings.
    def use_tls(context : OpenSSL::SSL::Context::Server) : self
      @tls_context = context
      self
    end

    # intercept registers a server-side interceptor.
    # Interceptors run outermost-first (first registered wraps all others).
    # Must be called before listen/bind.
    def intercept(interceptor : ServerInterceptor) : self
      @interceptors << interceptor
      self
    end

    # listen binds to the given host and port and starts serving. Blocks until stopped.
    def listen(host : String, port : Int32) : Nil
      @tcp_server = TCPServer.new(host, port)
      serve
    end

    # listen with port as a string.
    def listen(host : String, port : String) : Nil
      listen(host, port.to_i)
    end

    # listen with a combined "host:port" address string.
    def listen(address : String) : Nil
      host, port = split_address(address)
      listen(host, port)
    end

    # bind sets up the TCP listener without starting the serve loop.
    # Call serve (or start for non-blocking) afterward.
    def bind(address : String) : self
      host, port = split_address(address)
      @tcp_server = TCPServer.new(host, port)
      self
    end

    # serve starts the accept loop. Blocks until stopped.
    def serve : Nil
      tcp = @tcp_server
      raise "call bind or listen before serve" unless tcp

      loop do
        socket = tcp.accept? || break
        socket.tcp_nodelay = true
        services_snapshot = @services.dup
        interceptors_snapshot = @interceptors.dup
        spawn handle_connection(socket, services_snapshot, interceptors_snapshot)
      end
    end

    # start runs the server in a background fiber, returning immediately.
    # Useful for tests and embedded servers.
    def start : Nil
      spawn serve
      Fiber.yield
    end

    def stop : Nil
      @health_reporter.try &.shutdown!

      tcp = @tcp_server
      @tcp_server = nil
      tcp.try &.close

      transports = @transport_mutex.synchronize do
        @active_transports.values.dup
      end
      transports.each do |transport|
        transport.close unless transport.closed?
      end
    end

    # enable_health_checking registers the built-in health service and returns its reporter.
    # Calling this multiple times returns the same instance.
    def enable_health_checking(default_status : ::Grpc::Health::V1::HealthCheckResponse::ServingStatus = ::Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVING) : Health::Reporter
      if reporter = @health_reporter
        return reporter
      end

      service = Health::Service.new(default_status)
      handle(service)
      @health_reporter = service.reporter
      @health_service = service
      service.reporter
    end

    # enable_reflection registers the built-in reflection service and returns it.
    # Register FileDescriptorProto bytes on the returned service via add_file_descriptor.
    def enable_reflection : Reflection::Service
      if service = @reflection_service
        return service
      end

      service = Reflection::Service.new
      @services.each_key do |service_name|
        service.register_service(service_name)
      end
      if @health_service
        service.add_file_descriptor(Health::Service::FILE_DESCRIPTOR_PROTO_BYTES)
      end
      handle(service)
      @reflection_service = service
      service
    end

    # Returns the registered reflection service if enabled.
    def reflection_service? : Reflection::Service?
      @reflection_service
    end

    private def handle_connection(socket : TCPSocket, services : Hash(String, Service),
                                  interceptors : Array(ServerInterceptor)) : Nil
      peer = socket.remote_address.to_s rescue "unknown"
      tls_sock : OpenSSL::SSL::Socket::Server? = nil
      io : IO = socket
      if tls_ctx = @tls_context
        tls_sock = OpenSSL::SSL::Socket::Server.new(socket, tls_ctx, sync_close: true)
        io = tls_sock
      end
      conn = @transport_factory.call(io, services, interceptors, peer, tls_sock)
      register_transport(conn)
      conn.run_recv_loop
    rescue ex
      LOGGER.error(exception: ex) { "grpc connection error" }
    ensure
      unregister_transport(conn) if conn
      socket.close rescue nil
    end

    private def register_transport(transport : Transport::ServerTransport) : Nil
      @transport_mutex.synchronize do
        @active_transports[transport.object_id] = transport
      end
    end

    private def unregister_transport(transport : Transport::ServerTransport) : Nil
      @transport_mutex.synchronize do
        @active_transports.delete(transport.object_id)
      end
    end

    private def split_address(address : String) : {String, Int32}
      if address.includes?(":")
        idx = address.rindex(':') || 0
        host = address[0, idx]
        host = "0.0.0.0" if host.empty?
        port = address[idx + 1..].to_i
        {host, port}
      else
        {"0.0.0.0", address.to_i}
      end
    end
  end
end
