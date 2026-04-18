require "../helloworld/greeter.pb"

# LoggingInterceptor prints the method name and final status for every unary RPC.
class LoggingInterceptor < GRPC::ServerInterceptor
  def call(request : GRPC::RequestEnvelope, ctx : GRPC::ServerContext,
           next_call : GRPC::UnaryServerCall) : GRPC::ResponseEnvelope
    puts "[server] #{request.info.method_path} from #{ctx.peer}"
    result = next_call.call(request.info.method_path, request, ctx)
    puts "[server] #{request.info.method_path} -> #{result.status.code}"
    result
  end
end

# AuthInterceptor rejects calls that do not carry a valid authorization header.
class AuthInterceptor < GRPC::ServerInterceptor
  def call(request : GRPC::RequestEnvelope, ctx : GRPC::ServerContext,
           next_call : GRPC::UnaryServerCall) : GRPC::ResponseEnvelope
    token = ctx.metadata["authorization"]?
    unless token == "Bearer secret"
      return GRPC::ResponseEnvelope.new(
        request.info,
        Bytes.empty,
        GRPC::Status.new(GRPC::StatusCode::UNAUTHENTICATED, "invalid token")
      )
    end
    next_call.call(request.info.method_path, request, ctx)
  end
end

class GreeterImpl < Helloworld::Greeter::Service
  def say_hello(request : Helloworld::HelloRequest, ctx : GRPC::ServerContext) : Helloworld::HelloReply
    Helloworld::HelloReply.new(message: "Hello, #{request.name}!")
  end
end

port = ARGV[0]? || "50053"
server = GRPC::Server.new
server.intercept LoggingInterceptor.new # executes first
server.intercept AuthInterceptor.new    # executes second
server.handle GreeterImpl.new
puts "Interceptor example server on 0.0.0.0:#{port}"
server.listen "0.0.0.0", port
