require "http/client"
require "json"
require "socket"
require "./proto_helpers"

MISSING_GRIPMOCK_MSG = "gripmock is not available; failing gripmock client e2e tests"
GRIPMOCK_ADMIN_MSG   = "gripmock admin API is not available; failing gripmock client e2e tests"

GRIPMOCK_HOST       = ENV["GRIPMOCK_HOST"]? || "127.0.0.1"
GRIPMOCK_GRPC_PORT  = (ENV["GRIPMOCK_GRPC_PORT"]? || "4770").to_i
GRIPMOCK_ADMIN_PORT = (ENV["GRIPMOCK_ADMIN_PORT"]? || "4771").to_i

def tcp_port_open?(host : String, port : Int32) : Bool
  socket = TCPSocket.new(host, port)
  socket.close
  true
rescue
  false
end

def gripmock_available? : Bool
  tcp_port_open?(GRIPMOCK_HOST, GRIPMOCK_GRPC_PORT) &&
    tcp_port_open?(GRIPMOCK_HOST, GRIPMOCK_ADMIN_PORT)
end

def gripmock_target : String
  "#{GRIPMOCK_HOST}:#{GRIPMOCK_GRPC_PORT}"
end

def gripmock_admin_base : String
  "http://#{GRIPMOCK_HOST}:#{GRIPMOCK_ADMIN_PORT}"
end

def admin_get(path : String) : Int32?
  response = HTTP::Client.get("#{gripmock_admin_base}#{path}")
  response.status_code
rescue
  nil
end

def admin_post(path : String, payload : String) : Int32?
  headers = HTTP::Headers{"Content-Type" => "application/json"}
  response = HTTP::Client.post("#{gripmock_admin_base}#{path}", headers: headers, body: payload)
  response.status_code
rescue
  nil
end

def gripmock_try_post(paths : Array(String), payload : String) : Bool
  paths.any? do |path|
    code = admin_post(path, payload)
    !code.nil? && code >= 200 && code < 300
  end
end

def gripmock_clear_stubs : Bool
  code = admin_get("/clear")
  !code.nil? && code >= 200 && code < 300
end

def gripmock_add_stub(stub_payload : String) : Bool
  gripmock_try_post(["/add"], stub_payload)
end

def gripmock_admin_available? : Bool
  gripmock_clear_stubs
end

def require_gripmock! : Nil
  return if gripmock_available?

  grpc_open = tcp_port_open?(GRIPMOCK_HOST, GRIPMOCK_GRPC_PORT)
  admin_open = tcp_port_open?(GRIPMOCK_HOST, GRIPMOCK_ADMIN_PORT)
  raise "#{MISSING_GRIPMOCK_MSG} (host=#{GRIPMOCK_HOST}, grpc=#{GRIPMOCK_GRPC_PORT}:#{grpc_open}, admin=#{GRIPMOCK_ADMIN_PORT}:#{admin_open})"
end

def require_gripmock_admin! : Nil
  return if gripmock_admin_available?

  raise "#{GRIPMOCK_ADMIN_MSG} (admin_base=#{gripmock_admin_base})"
end

def require_stub_added!(payload : String) : Nil
  raise GRIPMOCK_ADMIN_MSG unless gripmock_add_stub(payload)
end

def build_stub(service : String, method : String, output_message : String,
               input_message : String? = nil, metadata : Hash(String, String)? = nil,
               code : Int32? = nil, error_message : String? = nil, delay_ms : Int32? = nil) : String
  output = JSON.build do |json|
    json.object do
      json.field "data" do
        json.object do
          json.field "message", output_message
        end
      end
      if value = code
        json.field "code", value
      end
      if value = error_message
        json.field "error", value
      end
      if value = delay_ms
        json.field "delay", value
      end
    end
  end

  JSON.build do |json|
    json.object do
      json.field "service", service
      json.field "method", method
      json.field "input" do
        json.object do
          unless input_message.nil?
            json.field "equals" do
              json.object do
                json.field "message", input_message
              end
            end
          end
          unless metadata.nil?
            json.field "headers" do
              json.object do
                # Use "contains" instead of "equals" because gRPC HTTP/2 automatically includes
                # :authority and content-type headers, so exact match wouldn't work
                json.field "contains" do
                  json.object do
                    metadata.each { |k, v| json.field k, v }
                  end
                end
              end
            end
          end
        end
      end
      json.field "output" do
        json.raw output
      end
    end
  end
end

describe "gripmock client e2e" do
  before_each do
    require_gripmock!
    require_gripmock_admin!
    gripmock_clear_stubs.should be_true
  end

  after_each do
    # Best-effort cleanup to keep examples independent without masking main failures.
    gripmock_clear_stubs if gripmock_available?
  end

  it "returns non-OK status when no stub is configured" do
    channel = GRPC::Channel.new(gripmock_target)
    begin
      response = channel.unary_call("e2e.Probe", "UnaryEcho", E2EProto.encode_string("no-stub"))
      response.status.ok?.should be_false
    ensure
      channel.close
    end
  end

  it "performs unary success against gripmock" do
    payload = build_stub(
      service: "Probe",
      method: "UnaryEcho",
      input_message: "hello",
      output_message: "echo:hello;token:none",
    )
    require_stub_added!(payload)

    channel = GRPC::Channel.new(gripmock_target)
    begin
      response = channel.unary_call("e2e.Probe", "UnaryEcho", E2EProto.encode_string("hello"))
      response.status.ok?.should be_true
      E2EProto.decode_string(response.raw).should eq("echo:hello;token:none")
    ensure
      channel.close
      gripmock_clear_stubs
    end
  end

  it "maps grpc error status from gripmock" do
    payload = build_stub(
      service: "Probe",
      method: "UnaryFail",
      input_message: "lost",
      output_message: "",
      code: 5,
      error_message: "missing:lost",
    )
    require_stub_added!(payload)

    channel = GRPC::Channel.new(gripmock_target)
    begin
      response = channel.unary_call("e2e.Probe", "UnaryFail", E2EProto.encode_string("lost"))
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::NOT_FOUND)
      response.status.message.should eq("missing:lost")
    ensure
      channel.close
      gripmock_clear_stubs
    end
  end

  it "handles parallel unary calls with distinct metadata tokens" do
    tokens = ["tok-1", "tok-2", "tok-3"]

    tokens.each do |token|
      payload = build_stub(
        service: "Probe",
        method: "UnaryEcho",
        input_message: token,
        metadata: {"x-e2e-token" => token},
        output_message: "echo:#{token};token:#{token}",
      )
      require_stub_added!(payload)
    end

    results = ::Channel({String, Bool, String}).new(tokens.size)

    begin
      tokens.each do |token|
        spawn do
          channel = GRPC::Channel.new(gripmock_target)
          begin
            ctx = GRPC::ClientContext.new(metadata: {"x-e2e-token" => token})
            response = channel.unary_call("e2e.Probe", "UnaryEcho", E2EProto.encode_string(token), ctx)
            results.send({token, response.status.ok?, E2EProto.decode_string(response.raw)})
          rescue ex
            results.send({token, false, ex.message || "exception"})
          ensure
            channel.close
          end
        end
      end

      seen = {} of String => {Bool, String}
      tokens.size.times do
        token, ok, value = results.receive
        seen[token] = {ok, value}
      end

      tokens.each do |token|
        ok, value = seen[token]
        ok.should be_true
        value.should eq("echo:#{token};token:#{token}")
      end
    ensure
      gripmock_clear_stubs
    end
  end

  it "sends metadata for unary calls" do
    # GripMock DOES support custom header matching in stubs
    payload = build_stub(
      service: "Probe",
      method: "UnaryEcho",
      input_message: "meta",
      metadata: {"x-e2e-token" => "abc123"},
      output_message: "echo:meta;token:abc123",
    )
    require_stub_added!(payload)

    channel = GRPC::Channel.new(gripmock_target)
    begin
      ctx = GRPC::ClientContext.new(metadata: {"x-e2e-token" => "abc123"})
      response = channel.unary_call("e2e.Probe", "UnaryEcho", E2EProto.encode_string("meta"), ctx)
      response.status.ok?.should be_true
      E2EProto.decode_string(response.raw).should eq("echo:meta;token:abc123")
    ensure
      channel.close
      gripmock_clear_stubs
    end
  end

  it "applies updated metadata values across sequential unary calls" do
    first_payload = build_stub(
      service: "Probe",
      method: "UnaryEcho",
      input_message: "token-a",
      metadata: {"x-e2e-token" => "token-a"},
      output_message: "echo:token-a;token:token-a",
    )
    second_payload = build_stub(
      service: "Probe",
      method: "UnaryEcho",
      input_message: "token-b",
      metadata: {"x-e2e-token" => "token-b"},
      output_message: "echo:token-b;token:token-b",
    )
    require_stub_added!(first_payload)
    require_stub_added!(second_payload)

    channel = GRPC::Channel.new(gripmock_target)
    begin
      ctx_a = GRPC::ClientContext.new(metadata: {"x-e2e-token" => "token-a"})
      first_response = channel.unary_call("e2e.Probe", "UnaryEcho", E2EProto.encode_string("token-a"), ctx_a)
      first_response.status.ok?.should be_true
      E2EProto.decode_string(first_response.raw).should eq("echo:token-a;token:token-a")

      ctx_b = GRPC::ClientContext.new(metadata: {"x-e2e-token" => "token-b"})
      second_response = channel.unary_call("e2e.Probe", "UnaryEcho", E2EProto.encode_string("token-b"), ctx_b)
      second_response.status.ok?.should be_true
      E2EProto.decode_string(second_response.raw).should eq("echo:token-b;token:token-b")
    ensure
      channel.close
      gripmock_clear_stubs
    end
  end

  it "keeps unary call healthy with deadline on a delayed stub" do
    payload = build_stub(
      service: "Probe",
      method: "SlowUnary",
      input_message: "sleep",
      output_message: "deadline:ok",
      delay_ms: 120,
    )
    require_stub_added!(payload)

    channel = GRPC::Channel.new(gripmock_target)
    begin
      ctx = GRPC::ClientContext.new(deadline: 3.seconds)
      response = channel.unary_call("e2e.Probe", "SlowUnary", E2EProto.encode_string("sleep"), ctx)
      response.status.ok?.should be_true
      E2EProto.decode_string(response.raw).should eq("deadline:ok")
    ensure
      channel.close
      gripmock_clear_stubs
    end
  end

  it "propagates delayed grpc error status and message" do
    payload = build_stub(
      service: "Probe",
      method: "UnaryFail",
      input_message: "wait-and-fail",
      output_message: "",
      code: 14,
      error_message: "temporarily unavailable",
      delay_ms: 100,
    )
    require_stub_added!(payload)

    channel = GRPC::Channel.new(gripmock_target)
    begin
      response = channel.unary_call("e2e.Probe", "UnaryFail", E2EProto.encode_string("wait-and-fail"))
      response.status.ok?.should be_false
      response.status.code.should eq(GRPC::StatusCode::UNAVAILABLE)
      response.status.message.should eq("temporarily unavailable")
    ensure
      channel.close
      gripmock_clear_stubs
    end
  end
end
