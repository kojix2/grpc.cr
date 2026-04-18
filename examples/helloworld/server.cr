require "./greeter.pb"

class GreeterImpl < Helloworld::Greeter::Service
  def say_hello(request : Helloworld::HelloRequest, ctx : GRPC::ServerContext) : Helloworld::HelloReply
    puts "Received: #{request.name} from #{ctx.peer}"
    Helloworld::HelloReply.new(message: "Hello, #{request.name}!")
  end
end

port = ARGV[0]? || "50051"
server = GRPC::Server.new
server.handle GreeterImpl.new
puts "gRPC server listening on 0.0.0.0:#{port}"
server.listen "0.0.0.0", port
