module GRPC
  # RPCKind enumerates the four gRPC streaming variants.
  # Available via CallInfo#kind so interceptors can branch on call type.
  enum RPCKind
    Unary
    ServerStreaming
    ClientStreaming
    Bidi
  end

  # CallInfo carries metadata about an in-flight RPC call passed to interceptors.
  # Interceptors should use this instead of parsing raw strings.
  #
  # Example:
  #   def call(info : GRPC::CallInfo, request, ctx, next_call)
  #     STDERR.puts "→ #{info.method_path} (#{info.kind})"
  #     next_call.call(info.method_path, request, ctx)
  #   end
  struct CallInfo
    # Full gRPC method path, e.g. "/helloworld.Greeter/SayHello"
    getter method_path : String
    # Streaming kind of this RPC
    getter kind : RPCKind

    def initialize(@method_path : String, @kind : RPCKind)
    end

    # service returns the service portion of the path, e.g. "helloworld.Greeter"
    def service : String
      parts = @method_path.split('/')
      parts.size >= 2 ? parts[1] : ""
    end

    # method returns the method name portion of the path, e.g. "SayHello"
    def method : String
      parts = @method_path.split('/')
      parts.size >= 3 ? parts[2] : ""
    end
  end

  # RequestEnvelope wraps a raw protobuf payload with call metadata.
  # Interceptors can inspect call info and decode the payload on demand.
  struct RequestEnvelope
    getter info : CallInfo
    getter raw : Bytes
    getter codec : String?
    getter descriptor : String?

    def initialize(@info : CallInfo, @raw : Bytes,
                   @codec : String? = nil, @descriptor : String? = nil)
    end

    def decode(type : T.class, marshaller : Marshaller(T)? = nil) : T forall T
      (marshaller || ProtoMarshaller(T).new).load(@raw)
    end
  end

  # ResponseEnvelope wraps a raw protobuf response payload, call metadata,
  # and final status for unary-style responses.
  struct ResponseEnvelope
    getter info : CallInfo
    getter raw : Bytes
    getter status : Status
    getter metadata : Metadata
    getter codec : String?
    getter descriptor : String?

    def initialize(@info : CallInfo, @raw : Bytes, @status : Status,
                   @metadata : Metadata = Metadata.new,
                   @codec : String? = nil, @descriptor : String? = nil)
    end

    def decode(type : T.class, marshaller : Marshaller(T)? = nil) : T forall T
      (marshaller || ProtoMarshaller(T).new).load(@raw)
    end
  end

  # Proc aliases for the transport closures passed through interceptor chains.

  # Unary RPC call types
  alias UnaryClientCall = Proc(String, RequestEnvelope, ClientContext, ResponseEnvelope)
  alias UnaryServerCall = Proc(String, RequestEnvelope, ServerContext, ResponseEnvelope)

  # Server-streaming RPC call types
  alias ServerStreamClientCall = Proc(String, RequestEnvelope, ClientContext, RawServerStream)
  alias ServerStreamServerCall = Proc(String, RequestEnvelope, ServerContext, RawResponseStream, Status)

  # Client-streaming RPC call types
  alias ClientStreamServerCall = Proc(String, RawRequestStream, ServerContext, ResponseEnvelope)

  # Bidirectional-streaming RPC call types
  alias BidiStreamServerCall = Proc(String, RawRequestStream, ServerContext, RawResponseStream, Status)

  # Live (non-batch) streaming RPC call types — no pre-buffering of messages.
  alias LiveClientStreamClientCall = Proc(String, ClientContext, RawClientCall)
  alias LiveBidiStreamClientCall = Proc(String, ClientContext, RawBidiCall)

  # ClientInterceptor is the base class for client-side interceptors.
  # Override `call` to wrap unary RPCs.  Override the streaming variants to
  # intercept server-streaming, client-streaming, or bidi-streaming calls.
  # Unimplemented streaming methods default to a transparent pass-through.
  #
  # Example:
  #   class LoggingInterceptor < GRPC::ClientInterceptor
  #     def call(method_path, request, ctx, next_call)
  #       STDERR.puts "→ #{method_path}"
  #       result = next_call.call(method_path, request, ctx)
  #       STDERR.puts "← #{result[1].code}"
  #       result
  #     end
  #   end
  #
  #   channel = GRPC::Channel.new("localhost:50051",
  #                               interceptors: [LoggingInterceptor.new] of GRPC::ClientInterceptor)
  abstract class ClientInterceptor
    abstract def call(
      request : RequestEnvelope,
      ctx : ClientContext,
      next_call : UnaryClientCall,
    ) : ResponseEnvelope

    def call_server_stream(
      request : RequestEnvelope,
      ctx : ClientContext,
      next_call : ServerStreamClientCall,
    ) : RawServerStream
      next_call.call(request.info.method_path, request, ctx)
    end

    def call_live_client_stream(
      info : CallInfo,
      ctx : ClientContext,
      next_call : LiveClientStreamClientCall,
    ) : RawClientCall
      next_call.call(info.method_path, ctx)
    end

    def call_live_bidi_stream(
      info : CallInfo,
      ctx : ClientContext,
      next_call : LiveBidiStreamClientCall,
    ) : RawBidiCall
      next_call.call(info.method_path, ctx)
    end
  end

  # ServerInterceptor is the base class for server-side interceptors.
  # Override `call` to wrap unary RPCs.  Override the streaming variants to
  # intercept server-streaming, client-streaming, or bidi-streaming calls.
  # Unimplemented streaming methods default to a transparent pass-through.
  #
  # Example:
  #   class AuthInterceptor < GRPC::ServerInterceptor
  #     def call(method, request, ctx, next_call)
  #       unless ctx.metadata["authorization"]? == "Bearer secret"
  #         return {Bytes.empty, GRPC::Status.new(GRPC::StatusCode::UNAUTHENTICATED, "bad token")}
  #       end
  #       next_call.call(method, request, ctx)
  #     end
  #   end
  #
  #   server.intercept AuthInterceptor.new
  abstract class ServerInterceptor
    abstract def call(
      request : RequestEnvelope,
      ctx : ServerContext,
      next_call : UnaryServerCall,
    ) : ResponseEnvelope

    def call_server_stream(
      request : RequestEnvelope,
      ctx : ServerContext,
      writer : RawResponseStream,
      next_call : ServerStreamServerCall,
    ) : Status
      next_call.call(request.info.method_path, request, ctx, writer)
    end

    def call_client_stream(
      info : CallInfo,
      requests : RawRequestStream,
      ctx : ServerContext,
      next_call : ClientStreamServerCall,
    ) : ResponseEnvelope
      next_call.call(info.method_path, requests, ctx)
    end

    def call_bidi_stream(
      info : CallInfo,
      requests : RawRequestStream,
      ctx : ServerContext,
      writer : RawResponseStream,
      next_call : BidiStreamServerCall,
    ) : Status
      next_call.call(info.method_path, requests, ctx, writer)
    end
  end

  # Interceptors contains helpers for building interceptor chains.
  # The first interceptor in the array executes outermost (first on the way in,
  # last on the way out).
  module Interceptors
    # build_client_chain wraps *base* with *interceptors*.
    # Returns a single UnaryClientCall that runs interceptors[0] first.
    def self.build_client_chain(
      interceptors : Array(ClientInterceptor),
      base : UnaryClientCall,
    ) : UnaryClientCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = UnaryClientCall.new do |method_path, request, ctx|
          info = CallInfo.new(method_path, RPCKind::Unary)
          req = RequestEnvelope.new(info, request.raw, request.codec, request.descriptor)
          outer.call(req, ctx, inner).as(ResponseEnvelope)
        end
      end
      chain
    end

    def self.build_client_chain(
      interceptors : Array(ClientInterceptor),
      base : ServerStreamClientCall,
    ) : ServerStreamClientCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = ServerStreamClientCall.new do |method_path, request, ctx|
          info = CallInfo.new(method_path, RPCKind::ServerStreaming)
          req = RequestEnvelope.new(info, request.raw, request.codec, request.descriptor)
          outer.call_server_stream(req, ctx, inner).as(RawServerStream)
        end
      end
      chain
    end

    def self.build_client_chain(
      interceptors : Array(ClientInterceptor),
      base : LiveClientStreamClientCall,
    ) : LiveClientStreamClientCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = LiveClientStreamClientCall.new do |method_path, ctx|
          info = CallInfo.new(method_path, RPCKind::ClientStreaming)
          outer.call_live_client_stream(info, ctx, inner).as(RawClientCall)
        end
      end
      chain
    end

    def self.build_client_chain(
      interceptors : Array(ClientInterceptor),
      base : LiveBidiStreamClientCall,
    ) : LiveBidiStreamClientCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = LiveBidiStreamClientCall.new do |method_path, ctx|
          info = CallInfo.new(method_path, RPCKind::Bidi)
          outer.call_live_bidi_stream(info, ctx, inner).as(RawBidiCall)
        end
      end
      chain
    end

    # build_server_chain wraps *base* with *interceptors*.
    # Returns a single call proc that runs interceptors[0] first.
    def self.build_server_chain(
      interceptors : Array(ServerInterceptor),
      base : UnaryServerCall,
    ) : UnaryServerCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = UnaryServerCall.new do |method_path, request, ctx|
          info = CallInfo.new(method_path, RPCKind::Unary)
          req = RequestEnvelope.new(info, request.raw, request.codec, request.descriptor)
          outer.call(req, ctx, inner).as(ResponseEnvelope)
        end
      end
      chain
    end

    def self.build_server_chain(
      interceptors : Array(ServerInterceptor),
      base : ServerStreamServerCall,
    ) : ServerStreamServerCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = ServerStreamServerCall.new do |method_path, request, ctx, writer|
          info = CallInfo.new(method_path, RPCKind::ServerStreaming)
          req = RequestEnvelope.new(info, request.raw, request.codec, request.descriptor)
          outer.call_server_stream(req, ctx, writer, inner).as(Status)
        end
      end
      chain
    end

    def self.build_server_chain(
      interceptors : Array(ServerInterceptor),
      base : ClientStreamServerCall,
    ) : ClientStreamServerCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = ClientStreamServerCall.new do |method_path, requests, ctx|
          info = CallInfo.new(method_path, RPCKind::ClientStreaming)
          outer.call_client_stream(info, requests, ctx, inner).as(ResponseEnvelope)
        end
      end
      chain
    end

    def self.build_server_chain(
      interceptors : Array(ServerInterceptor),
      base : BidiStreamServerCall,
    ) : BidiStreamServerCall
      chain = base
      interceptors.reverse_each do |interceptor|
        outer = interceptor
        inner = chain
        chain = BidiStreamServerCall.new do |method_path, requests, ctx, writer|
          info = CallInfo.new(method_path, RPCKind::Bidi)
          outer.call_bidi_stream(info, requests, ctx, writer, inner).as(Status)
        end
      end
      chain
    end
  end
end
