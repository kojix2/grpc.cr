require "../helloworld/greeter.pb"

# Generate a self-signed cert for local testing:
#   openssl req -x509 -newkey rsa:2048 -keyout server.key -out server.crt \
#           -days 365 -nodes -subj "/CN=localhost"

class GreeterImpl < Helloworld::Greeter::Service
  def say_hello(request : Helloworld::HelloRequest, ctx : GRPC::ServerContext) : Helloworld::HelloReply
    Helloworld::HelloReply.new(message: "Hello over TLS, #{request.name}!")
  end
end

cert = ARGV[0]? || "server.crt"
key = ARGV[1]? || "server.key"
port = ARGV[2]? || "50443"

server = GRPC::Server.new
server.use_tls(cert: cert, key: key)
server.handle GreeterImpl.new
puts "TLS gRPC server on 0.0.0.0:#{port}"
server.listen "0.0.0.0", port
