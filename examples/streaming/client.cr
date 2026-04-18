require "./numbers.pb"

host = ARGV[0]? || "localhost"
port = ARGV[1]? || "50052"

channel = GRPC::Channel.new("#{host}:#{port}")
client = Numbers::Numbers::Client.new(channel)

begin
  # --- Unary ---
  reply = client.square(Numbers::Number.new(7))
  puts "Square(7)    = #{reply.value}"
  # => Square(7)    = 49

  # --- Server streaming ---
  print "Range(5)     = "
  stream = client.range(Numbers::Number.new(5))
  stream.each { |num| print "#{num.value} " }
  puts
  # => Range(5)     = 1 2 3 4 5

  # --- Client streaming ---
  call = client.sum
  (1..5).each { |i| call.send(Numbers::Number.new(i)) }
  total = call.close_and_recv
  puts "Sum(1..5)    = #{total.value}"
  # => Sum(1..5)    = 15

  # --- Bidirectional streaming ---
  bidi = client.transform
  replies = [] of Int32
  recv_done = ::Channel(Nil).new(1)
  spawn do
    bidi.each { |num| replies << num.value }
    recv_done.send(nil)
  end
  [2, 3, 4].each do |i|
    bidi.send(Numbers::Number.new(i))
    Fiber.yield
  end
  bidi.close_send
  recv_done.receive
  print "Transform([2,3,4]²) = "
  replies.each { |v| print "#{v} " }
  puts
  # => Transform([2,3,4]²) = 4 9 16
rescue ex : GRPC::StatusError
  STDERR.puts "RPC failed: #{ex.code} — #{ex.message}"
  exit 1
ensure
  channel.close
end
