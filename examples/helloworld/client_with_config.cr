require "./greeter.pb"

# Demonstrates EndpointConfig options: connect_timeout, keepalive,
# concurrency_limit, and rate_limit.

host = ARGV[0]? || "localhost"
port = ARGV[1]? || "50051"
name = ARGV[2]? || "world"

config = GRPC::EndpointConfig.new(
  # Fail fast when the server is unreachable (default: no timeout).
  connect_timeout: 5.seconds,

  # Enable OS-level TCP keepalive on the underlying socket.
  tcp_keepalive: 30.seconds,

  # At most 5 concurrent RPCs; further callers block until a slot is free.
  concurrency_limit: 5,

  # Allow at most 10 requests per second; excess callers sleep automatically.
  rate_limit: GRPC::RateLimitConfig.new(10_u64, 1.second),
)

channel = GRPC::Channel.new("#{host}:#{port}", endpoint_config: config)
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
