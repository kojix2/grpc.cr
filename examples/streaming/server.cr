require "./numbers.pb"

class NumbersImpl < Numbers::Numbers::Service
  # Unary: returns value^2
  def square(req : Numbers::Number, ctx : GRPC::ServerContext) : Numbers::Number
    Numbers::Number.new(req.value ** 2)
  end

  # Server streaming: streams 1, 2, ..., req.value
  def range(req : Numbers::Number, writer : GRPC::ResponseStream(Numbers::Number),
            ctx : GRPC::ServerContext) : GRPC::Status
    (1..req.value).each do |i|
      writer.send(Numbers::Number.new(i))
    end
    GRPC::Status.ok
  end

  # Client streaming: returns the sum of all received numbers
  def sum(requests : GRPC::RequestStream(Numbers::Number), ctx : GRPC::ServerContext) : Numbers::Number
    total = 0_i32
    requests.each do |num|
      puts "[server] sum recv: #{num.value}"
      total += num.value
    end
    Numbers::Number.new(total)
  end

  # Bidirectional streaming: returns the square of each received number
  def transform(requests : GRPC::RequestStream(Numbers::Number), writer : GRPC::ResponseStream(Numbers::Number),
                ctx : GRPC::ServerContext) : GRPC::Status
    requests.each do |num|
      puts "[server] transform recv: #{num.value}"
      writer.send(Numbers::Number.new(num.value ** 2))
    end
    GRPC::Status.ok
  end
end

port = ARGV[0]? || "50052"
server = GRPC::Server.new
server.handle NumbersImpl.new
puts "Numbers gRPC server listening on 0.0.0.0:#{port}"
server.listen "0.0.0.0", port
