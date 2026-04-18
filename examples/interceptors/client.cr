require "../helloworld/greeter.pb"

# AuthInterceptor injects an authorization header on every outbound call.
class AuthInterceptor < GRPC::ClientInterceptor
  def initialize(@token : String)
  end

  def call(request : GRPC::RequestEnvelope, ctx : GRPC::ClientContext,
           next_call : GRPC::UnaryClientCall) : GRPC::ResponseEnvelope
    ctx.metadata.set("authorization", "Bearer #{@token}")
    next_call.call(request.info.method_path, request, ctx)
  end
end

# RequestIDInterceptor stamps each call with a unique request ID.
class RequestIDInterceptor < GRPC::ClientInterceptor
  def call(request : GRPC::RequestEnvelope, ctx : GRPC::ClientContext,
           next_call : GRPC::UnaryClientCall) : GRPC::ResponseEnvelope
    ctx.metadata.set("x-request-id", Random::Secure.hex(8))
    next_call.call(request.info.method_path, request, ctx)
  end
end

host = ARGV[0]? || "localhost"
port = ARGV[1]? || "50053"
name = ARGV[2]? || "world"
token = ARGV[3]? || "secret"

channel = GRPC::Channel.new("#{host}:#{port}", interceptors: [
  AuthInterceptor.new(token),
  RequestIDInterceptor.new,
] of GRPC::ClientInterceptor)

client = Helloworld::Greeter::Client.new(channel)

begin
  reply = client.say_hello(Helloworld::HelloRequest.new(name: name))
  puts reply.message
rescue ex : GRPC::StatusError
  puts "RPC failed: #{ex.code} — #{ex.message}"
ensure
  channel.close
end
