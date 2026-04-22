require "../spec_helper"
require "./shared"

describe "grpcurl e2e proto source" do
  it "invokes unary success and returns expected payload" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        ["-d", "{\"message\":\"hello\"}"],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("echo:hello")
    ensure
      server.stop
    end
  end

  it "passes rpc-header metadata and server observes it" do
    next unless grpcurl_available?

    observer = E2EProbeObserver.new
    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new(observer)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        [
          "-rpc-header", "x-e2e-token: abc123",
          "-d", "{\"message\":\"meta\"}",
        ],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("token:abc123")

      if observed = observer.last
        observed.token.should eq("abc123")
        observed.method.should eq("UnaryEcho")
      else
        fail("expected probe observation")
      end
    ensure
      server.stop
    end
  end

  it "returns grpc status for unary failure" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryFail",
        ["-d", "{\"message\":\"lost\"}"],
      )
      status, out, err = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?

      detail = "#{out}\n#{err}".downcase
      detail.should contain("error")
      detail.should contain("missing:lost")
    ensure
      server.stop
    end
  end

  it "returns FAILED_PRECONDITION for user-code unary rejection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryFailedPrecondition",
        ["-d", "{\"message\":\"locked\"}"],
      )
      status, stdout_text, stderr_text = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?

      detail = "#{stdout_text}\n#{stderr_text}".downcase
      detail.should contain("failedprecondition")
      detail.should contain("state blocked:locked")
    ensure
      server.stop
    end
  end

  it "returns DATA_LOSS for user-code unary corruption signal" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryDataLoss",
        ["-d", "{\"message\":\"segment-a\"}"],
      )
      status, stdout_text, stderr_text = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?

      detail = "#{stdout_text}\n#{stderr_text}".downcase
      detail.should contain("dataloss")
      detail.should contain("corrupted:segment-a")
    ensure
      server.stop
    end
  end

  it "enforces max-time timeout and server observes deadline" do
    next unless grpcurl_available?

    observer = E2EProbeObserver.new
    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new(observer)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/SlowUnary",
        [
          "-max-time", "0.1",
          "-d", "{\"message\":\"sleep\"}",
        ],
      )
      status, out, err = run_grpcurl(args)
      raise "expected timeout but got success" if status.success?

      detail = "#{out}\n#{err}".downcase
      is_timeout = detail.includes?("deadline") ||
                   detail.includes?("timeout") ||
                   detail.includes?("timed out")
      is_timeout.should be_true

      if observed = wait_for_probe_observation(observer)
        observed.method.should eq("SlowUnary")
        observed.deadline_set?.should be_true
      else
        fail("expected probe observation")
      end
    ensure
      server.stop
    end
  end

  it "receives multiple messages from server-streaming call" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/ServerStream",
        ["-d", "{\"message\":\"stream\"}"],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("stream:0:stream")
      out.should contain("stream:1:stream")
      out.should contain("stream:2:stream")
    ensure
      server.stop
    end
  end

  it "prints streamed messages before final INTERNAL status and exposes trailers" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/ServerStreamFailAfterTwo",
        ["-v", "-d", "{\"message\":\"boom\"}"],
      )
      status, stdout_text, stderr_text = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?

      stdout_text.should contain("stream-fail:0:boom")
      stdout_text.should contain("stream-fail:1:boom")
      stdout_text.downcase.should contain("response trailers received:")
      stdout_text.downcase.should contain("x-e2e-trailer: server-stream-failed")
      stdout_text.downcase.should contain("x-e2e-count: 2")

      detail = stderr_text.downcase
      detail.should contain("code: internal")
      detail.should contain("message: stream exploded:boom")
    ensure
      server.stop
    end
  end

  it "sends multiple messages to client-streaming call" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      payload = %({"message":"a"}\n{"message":"b"}\n{"message":"c"})
      args = grpcurl_call_args(
        port,
        "e2e.Probe/ClientStream",
        ["-d", payload],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("joined:a,b,c")
    ensure
      server.stop
    end
  end

  it "exchanges messages in bidirectional streaming call" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      payload = %({"message":"x"}\n{"message":"y"})
      args = grpcurl_call_args(
        port,
        "e2e.Probe/BidiStream",
        ["-d", payload],
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("bidi:0:x")
      out.should contain("bidi:1:y")
    ensure
      server.stop
    end
  end

  it "prints partial bidi responses before final RESOURCE_EXHAUSTED and exposes trailers" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      payload = %({"message":"x"}\n{"message":"y"}\n{"message":"z"})
      args = grpcurl_call_args(
        port,
        "e2e.Probe/BidiStreamFailAfterTwo",
        ["-v", "-d", payload],
      )
      status, stdout_text, stderr_text = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?

      stdout_text.should contain("bidi-fail:0:x")
      stdout_text.should contain("bidi-fail:1:y")
      stdout_text.downcase.should contain("response trailers received:")
      stdout_text.downcase.should contain("x-e2e-trailer: bidi-failed")
      stdout_text.downcase.should contain("x-e2e-count: 2")

      detail = stderr_text.downcase
      detail.should contain("code: resourceexhausted")
      detail.should contain("message: bidi limit reached")
    ensure
      server.stop
    end
  end
end
