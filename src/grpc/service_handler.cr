module GRPC
  # RequestStream(T) is the server-side handle for an inbound request stream.
  # It exposes an Enumerable(T) interface over messages arriving from the client.
  # The channel is closed (nil sentinel) when the client sends END_STREAM.
  class RequestStream(T)
    include Enumerable(T)

    def initialize(
      @ch : ::Channel(Bytes?),
      @marshaller : Marshaller(T) = ProtoMarshaller(T).new,
    )
    end

    def each(& : T ->) : Nil
      loop do
        raw = @ch.receive?
        break if raw.nil?
        yield @marshaller.load(raw)
      end
    end
  end

  # RawRequestStream is an untyped variant used by the transport and interceptor layer.
  # Generated dispatch methods wrap it in RequestStream(T).
  class RawRequestStream
    include Enumerable(Bytes)

    getter ch : ::Channel(Bytes?)

    def initialize(@ch : ::Channel(Bytes?))
    end

    def each(& : Bytes ->) : Nil
      loop do
        raw = @ch.receive?
        break if raw.nil?
        yield raw
      end
    end
  end

  # Service is the abstract base class for gRPC service implementations.
  # Generated service base classes inherit from this.
  #
  # Users implement the generated abstract subclass, not this class directly.
  #
  # Example (generated code):
  #   module Helloworld
  #     abstract class GreeterService < GRPC::Service
  #       SERVICE_NAME = "helloworld.Greeter"
  #
  #       def service_name : String
  #         SERVICE_NAME
  #       end
  #
  #       abstract def say_hello(req : HelloRequest, ctx : GRPC::ServerContext) : HelloReply
  #
  #       def dispatch(method : String, body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
  #         case method
  #         when "SayHello"
  #           req  = HelloRequest.from_proto(body)
  #           resp = say_hello(req, ctx)
  #           {resp.to_proto, GRPC::Status.ok}
  #         else
  #           {Bytes.empty, GRPC::Status.unimplemented("method #{method} not found")}
  #         end
  #       rescue ex : GRPC::StatusError
  #         {Bytes.empty, ex.status}
  #       rescue ex
  #         {Bytes.empty, GRPC::Status.internal(ex.message || "internal error")}
  #       end
  #     end
  #   end
  abstract class Service
    # service_name returns the full gRPC service name (e.g. "helloworld.Greeter").
    abstract def service_name : String

    # dispatch routes an incoming unary RPC call to the correct method implementation.
    # Returns {response_body : Bytes, status : Status}.
    abstract def dispatch(method : String, request_body : Bytes, ctx : ServerContext) : {Bytes, Status}

    # server_streaming? returns true if *method* is a server-streaming RPC.
    # Generated service base classes override this; the default is false (unary).
    def server_streaming?(method : String) : Bool
      false
    end

    # dispatch_server_stream dispatches a server-streaming RPC.
    # The transport passes a RawResponseStream; generated subclasses wrap it in a
    # typed ResponseStream(T) before handing off to the user implementation.
    def dispatch_server_stream(method : String, request_body : Bytes,
                               ctx : ServerContext, writer : RawResponseStream) : Status
      Status.unimplemented("server streaming not implemented for #{method}")
    end

    # client_streaming? returns true if *method* is a client-streaming RPC.
    # Generated service base classes override this; the default is false (unary).
    def client_streaming?(method : String) : Bool
      false
    end

    # dispatch_client_stream dispatches a client-streaming RPC.
    # *requests* is a RawRequestStream that yields raw message bytes as they arrive.
    # Returns {response_body : Bytes, status : Status}.
    # Generated service base classes override this; the default returns UNIMPLEMENTED.
    def dispatch_client_stream(method : String, requests : RawRequestStream,
                               ctx : ServerContext) : {Bytes, Status}
      {Bytes.empty, Status.unimplemented("client streaming not implemented for #{method}")}
    end

    # bidi_streaming? returns true if *method* is a bidirectional streaming RPC.
    # Generated service base classes override this; the default is false (unary).
    def bidi_streaming?(method : String) : Bool
      false
    end

    # dispatch_bidi_stream dispatches a bidirectional streaming RPC.
    # *requests* is a RawRequestStream that yields raw message bytes as they arrive.
    # The transport passes a RawResponseStream; generated subclasses wrap it in a
    # typed ResponseStream(T) before handing off to the user implementation.
    def dispatch_bidi_stream(method : String, requests : RawRequestStream,
                             ctx : ServerContext, writer : RawResponseStream) : Status
      Status.unimplemented("bidi streaming not implemented for #{method}")
    end
  end

  # ServiceHandler is kept as an alias for Service.
  ServiceHandler = Service
end
