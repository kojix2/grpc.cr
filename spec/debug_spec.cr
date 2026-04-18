require "./spec_helper"

# Debug spec to trace the HTTP/2 transport behavior
class DebugService < GRPC::Service
  SERVICE_FULL_NAME = "debug.Test"

  def service_full_name : String
    SERVICE_FULL_NAME
  end

  def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
    STDERR.puts "[SERVER] dispatch called: method=#{method} body_size=#{request_body.size}"
    case method
    when "Ping"
      STDERR.puts "[SERVER] Ping handler, returning pong"
      {"\x00pong".to_slice, GRPC::Status.ok}
    else
      {Bytes.empty, GRPC::Status.unimplemented("unknown")}
    end
  end
end

describe "GRPC debug" do
  it "traces a full round trip" do
    server = TCPServer.new("127.0.0.1", 0)
    port = server.local_address.port
    server.close

    grpc_server = GRPC::Server.new
    grpc_server.handle DebugService.new
    grpc_server.bind("127.0.0.1:#{port}")
    grpc_server.start

    sleep 10.milliseconds

    STDERR.puts "[TEST] connecting to port #{port}"
    channel = GRPC::Channel.new("127.0.0.1:#{port}")

    STDERR.puts "[TEST] calling unary_call"
    response = channel.unary_call("debug.Test", "Ping", Bytes.empty)
    STDERR.puts "[TEST] got status=#{response.status.code} body_size=#{response.raw.size}"

    response.status.ok?.should be_true

    channel.close
    grpc_server.stop
  end
end
