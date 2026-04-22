require "../spec_helper"
require "./grpcurl_shared"

describe "grpcurl e2e reflection source" do
  it "fails list when reflection is disabled" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "list")
      status, out, err = run_grpcurl(args)
      raise "expected reflection list to fail" if status.success?

      detail = "#{out}\n#{err}".downcase
      has_expected_hint = detail.includes?("reflection") ||
                          detail.includes?("server does not support") ||
                          detail.includes?("failed to list services")
      has_expected_hint.should be_true
    ensure
      server.stop
    end
  end

  it "fails describe when reflection is disabled" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "describe", "e2e.Probe")
      status, out, err = run_grpcurl(args)
      raise "expected reflection describe to fail" if status.success?

      detail = "#{out}\n#{err}".downcase
      has_expected_hint = detail.includes?("reflection") ||
                          detail.includes?("server does not support") ||
                          detail.includes?("failed to resolve symbol")
      has_expected_hint.should be_true
    ensure
      server.stop
    end
  end

  it "lists services via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    enable_e2e_reflection(server)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "list")
      status, out, err = run_grpcurl(args)
      raise "grpcurl list failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("e2e.Probe")
    ensure
      server.stop
    end
  end

  it "lists methods for e2e.Probe via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    enable_e2e_reflection(server)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "list", "e2e.Probe")
      status, out, err = run_grpcurl(args)
      raise "grpcurl list service failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("e2e.Probe.UnaryEcho")
      out.should contain("e2e.Probe.ServerStream")
    ensure
      server.stop
    end
  end

  it "describes e2e.Probe via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    enable_e2e_reflection(server)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "describe", "e2e.Probe")
      status, out, err = run_grpcurl(args)
      raise "grpcurl describe failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("service Probe")
      out.should contain("rpc UnaryEcho")
    ensure
      server.stop
    end
  end

  it "describes e2e.StringMessage via reflection" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    enable_e2e_reflection(server)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_reflection_args(port, "describe", "e2e.StringMessage")
      status, out, err = run_grpcurl(args)
      raise "grpcurl describe message failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("message StringMessage")
      out.should contain("string message")
    ensure
      server.stop
    end
  end

  it "invokes unary with reflection only" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    enable_e2e_reflection(server)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      wait_for_server(port)
      args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        ["-d", "{\"message\":\"hello\"}"],
        include_proto: false,
      )
      status, out, err = run_grpcurl(args)
      raise "grpcurl reflection invoke failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("echo:hello")
    ensure
      server.stop
    end
  end

  it "exports protoset from reflection and invokes with protoset" do
    next unless grpcurl_available?

    port = find_free_port
    server = GRPC::Server.new
    server.handle E2EProbeService.new
    enable_e2e_reflection(server)
    server.bind("127.0.0.1:#{port}")
    server.start

    protoset_path = "/tmp/grpc-e2e-#{Time.utc.to_unix_ms}-#{Random.rand(1_000_000)}.protoset"

    begin
      wait_for_server(port)

      export_args = grpcurl_reflection_args(
        port,
        "describe",
        "e2e.Probe",
        ["-protoset-out", protoset_path],
      )
      export_status, export_out, export_err = run_grpcurl(export_args)
      raise "grpcurl protoset export failed: #{export_err}\nstdout: #{export_out}" unless export_status.success?
      File.exists?(protoset_path).should be_true
      File.size(protoset_path).should be > 0

      invoke_args = grpcurl_call_args(
        port,
        "e2e.Probe/UnaryEcho",
        [
          "-protoset", protoset_path,
          "-d", "{\"message\":\"hello\"}",
        ],
        include_proto: false,
      )
      status, out, err = run_grpcurl(invoke_args)
      raise "grpcurl protoset invoke failed: #{err}\nstdout: #{out}" unless status.success?
      out.should contain("echo:hello")
    ensure
      File.delete(protoset_path) if File.exists?(protoset_path)
      server.stop
    end
  end
end
