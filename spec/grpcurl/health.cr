require "../spec_helper"
require "./shared"

describe "grpcurl e2e health" do
  it "invokes health Check via proto source" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    reporter = server.enable_health_checking
    reporter.set_status("demo.Service", Grpc::Health::V1::HealthCheckResponse::ServingStatus::NOT_SERVING)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "grpc.health.v1.Health/Check",
        ["-d", %({"service":"demo.Service"})],
        import_path: HEALTH_PROTO_IMPORT_PATH,
        proto: HEALTH_PROTO_FILE,
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl health check failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("NOT_SERVING")
    ensure
      server.stop
    end
  end

  it "returns NOT_FOUND for unknown health Check targets" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.enable_health_checking
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "grpc.health.v1.Health/Check",
        ["-d", %({"service":"missing.Service"})],
        import_path: HEALTH_PROTO_IMPORT_PATH,
        proto: HEALTH_PROTO_FILE,
      )
      status, stdout_text, stderr_text = run_grpcurl(args)
      raise "expected grpc error but got success" if status.success?

      detail = "#{stdout_text}\n#{stderr_text}".downcase
      detail.should contain("notfound")
      detail.should contain("unknown service missing.service")
    ensure
      server.stop
    end
  end

  it "invokes health Watch via reflection and streams updates" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.enable_reflection
    reporter = server.enable_health_checking
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)

      spawn do
        sleep 200.milliseconds
        reporter.set_status("watch.Service", Grpc::Health::V1::HealthCheckResponse::ServingStatus::SERVING)
      end

      args = grpcurl_call_args(
        port,
        "grpc.health.v1.Health/Watch",
        ["-max-time", "0.7", "-d", %({"service":"watch.Service"})],
        include_proto: false,
      )
      status, stdout_text, stderr_text = run_grpcurl(args)
      raise "expected watch to terminate via grpcurl timeout" if status.success?

      stdout_text.should contain("SERVICE_UNKNOWN")
      stdout_text.should contain("SERVING")

      detail = "#{stdout_text}\n#{stderr_text}".downcase
      has_timeout = detail.includes?("deadline") || detail.includes?("timeout") || detail.includes?("timed out")
      has_timeout.should be_true
    ensure
      server.stop
    end
  end
end
