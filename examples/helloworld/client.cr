require "./greeter.pb"

host = ARGV[0]? || "localhost"
port = ARGV[1]? || "50051"
name = ARGV[2]? || "world"

channel = GRPC::Channel.new("#{host}:#{port}")
client = Helloworld::Greeter::Client.new(channel)

request = Helloworld::HelloRequest.new(name: name)
puts "Sending: SayHello(name=#{name})"

begin
  reply = client.say_hello(request)
  puts "Response: #{reply.message}"
rescue ex : GRPC::StatusError
  puts "RPC failed: #{ex.code} — #{ex.message}"
ensure
  channel.close
end
