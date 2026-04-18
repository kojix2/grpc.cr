require "../helloworld/greeter.pb"

host = ARGV[0]? || "localhost"
port = ARGV[1]? || "50443"
name = ARGV[2]? || "world"

# For a self-signed cert, disable certificate verification.
# In production with a trusted CA, use GRPC::Channel.new("https://host:port")
# and omit tls_context entirely.
tls_ctx = OpenSSL::SSL::Context::Client.new
tls_ctx.verify_mode = OpenSSL::SSL::VerifyMode::NONE

channel = GRPC::Channel.new("https://#{host}:#{port}", tls_context: tls_ctx)
client = Helloworld::Greeter::Client.new(channel)

begin
  reply = client.say_hello(Helloworld::HelloRequest.new(name: name))
  puts reply.message
rescue ex : GRPC::StatusError
  puts "RPC failed: #{ex.code} — #{ex.message}"
ensure
  channel.close
end
