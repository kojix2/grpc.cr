require "./spec_helper"

class FakeClientTransport
  include GRPC::Transport::ClientTransport

  getter unary_calls = 0

  def initialize(@closed = false)
  end

  def unary_call(service : String, method : String,
                 request_body : Bytes, metadata : GRPC::Metadata) : GRPC::ResponseEnvelope
    @unary_calls += 1
    GRPC::ResponseEnvelope.new(
      GRPC::CallInfo.new("/#{service}/#{method}", GRPC::RPCKind::Unary),
      request_body,
      GRPC::Status.ok
    )
  end

  def open_server_stream(service : String, method : String,
                         request_bytes : Bytes,
                         metadata : GRPC::Metadata) : GRPC::RawServerStream
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok })
  end

  def open_bidi_stream_live(service : String, method : String,
                            metadata : GRPC::Metadata,
                            send_queue_size : Int32) : GRPC::RawBidiCall
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawBidiCall.new(
      ->(_b : Bytes) { },
      -> { },
      ch,
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def open_client_stream_live(service : String, method : String,
                              metadata : GRPC::Metadata,
                              send_queue_size : Int32) : GRPC::RawClientCall
    GRPC::RawClientCall.new(
      ->(_b : Bytes) { },
      -> { Bytes.empty },
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def closed? : Bool
    @closed
  end

  def close : Nil
    @closed = true
  end
end

class BlockingUnaryTransport
  include GRPC::Transport::ClientTransport

  getter started : ::Channel(Nil)
  getter releases : ::Channel(Nil)

  def initialize
    @started = ::Channel(Nil).new(8)
    @releases = ::Channel(Nil).new(8)
  end

  def unary_call(service : String, method : String,
                 request_body : Bytes, metadata : GRPC::Metadata) : GRPC::ResponseEnvelope
    @started.send(nil)
    @releases.receive
    GRPC::ResponseEnvelope.new(
      GRPC::CallInfo.new("/#{service}/#{method}", GRPC::RPCKind::Unary),
      request_body,
      GRPC::Status.ok
    )
  end

  def open_server_stream(service : String, method : String,
                         request_bytes : Bytes,
                         metadata : GRPC::Metadata) : GRPC::RawServerStream
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok })
  end

  def open_bidi_stream_live(service : String, method : String,
                            metadata : GRPC::Metadata,
                            send_queue_size : Int32) : GRPC::RawBidiCall
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawBidiCall.new(
      ->(_b : Bytes) { },
      -> { },
      ch,
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def open_client_stream_live(service : String, method : String,
                              metadata : GRPC::Metadata,
                              send_queue_size : Int32) : GRPC::RawClientCall
    GRPC::RawClientCall.new(
      ->(_b : Bytes) { },
      -> { Bytes.empty },
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def closed? : Bool
    false
  end

  def close : Nil
  end
end

class TimestampUnaryTransport
  include GRPC::Transport::ClientTransport

  getter starts : Array(Time::Instant)

  def initialize
    @starts = [] of Time::Instant
  end

  def unary_call(service : String, method : String,
                 request_body : Bytes, metadata : GRPC::Metadata) : GRPC::ResponseEnvelope
    @starts << Time.instant
    GRPC::ResponseEnvelope.new(
      GRPC::CallInfo.new("/#{service}/#{method}", GRPC::RPCKind::Unary),
      request_body,
      GRPC::Status.ok
    )
  end

  def open_server_stream(service : String, method : String,
                         request_bytes : Bytes,
                         metadata : GRPC::Metadata) : GRPC::RawServerStream
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok })
  end

  def open_bidi_stream_live(service : String, method : String,
                            metadata : GRPC::Metadata,
                            send_queue_size : Int32) : GRPC::RawBidiCall
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawBidiCall.new(
      ->(_b : Bytes) { },
      -> { },
      ch,
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def open_client_stream_live(service : String, method : String,
                              metadata : GRPC::Metadata,
                              send_queue_size : Int32) : GRPC::RawClientCall
    GRPC::RawClientCall.new(
      ->(_b : Bytes) { },
      -> { Bytes.empty },
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def closed? : Bool
    false
  end

  def close : Nil
  end
end

class BlockingServerStreamTransport
  include GRPC::Transport::ClientTransport

  getter started : ::Channel(Nil)

  def initialize
    @started = ::Channel(Nil).new(8)
  end

  def unary_call(service : String, method : String,
                 request_body : Bytes, metadata : GRPC::Metadata) : GRPC::ResponseEnvelope
    GRPC::ResponseEnvelope.new(
      GRPC::CallInfo.new("/#{service}/#{method}", GRPC::RPCKind::Unary),
      request_body,
      GRPC::Status.ok
    )
  end

  def open_server_stream(service : String, method : String,
                         request_bytes : Bytes,
                         metadata : GRPC::Metadata) : GRPC::RawServerStream
    @started.send(nil)
    ch = ::Channel(Bytes?).new(1)
    GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok }, -> { GRPC::Metadata.new }, -> { ch.close; nil })
  end

  def open_bidi_stream_live(service : String, method : String,
                            metadata : GRPC::Metadata,
                            send_queue_size : Int32) : GRPC::RawBidiCall
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawBidiCall.new(
      ->(_b : Bytes) { },
      -> { },
      ch,
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def open_client_stream_live(service : String, method : String,
                              metadata : GRPC::Metadata,
                              send_queue_size : Int32) : GRPC::RawClientCall
    GRPC::RawClientCall.new(
      ->(_b : Bytes) { },
      -> { Bytes.empty },
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def closed? : Bool
    false
  end

  def close : Nil
  end
end

class CyclingClientTransport
  include GRPC::Transport::ClientTransport

  getter id : Int32

  def initialize(@id : Int32)
    @closed = false
  end

  def unary_call(service : String, method : String,
                 request_body : Bytes, metadata : GRPC::Metadata) : GRPC::ResponseEnvelope
    GRPC::ResponseEnvelope.new(
      GRPC::CallInfo.new("/#{service}/#{method}", GRPC::RPCKind::Unary),
      Bytes[@id.to_u8],
      GRPC::Status.ok
    )
  end

  def open_server_stream(service : String, method : String,
                         request_bytes : Bytes,
                         metadata : GRPC::Metadata) : GRPC::RawServerStream
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawServerStream.new(ch, -> { GRPC::Status.ok })
  end

  def open_bidi_stream_live(service : String, method : String,
                            metadata : GRPC::Metadata,
                            send_queue_size : Int32) : GRPC::RawBidiCall
    ch = ::Channel(Bytes?).new(1)
    ch.close
    GRPC::RawBidiCall.new(
      ->(_b : Bytes) { },
      -> { },
      ch,
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def open_client_stream_live(service : String, method : String,
                              metadata : GRPC::Metadata,
                              send_queue_size : Int32) : GRPC::RawClientCall
    GRPC::RawClientCall.new(
      ->(_b : Bytes) { },
      -> { Bytes.empty },
      -> { GRPC::Metadata.new },
      -> { GRPC::Status.ok },
      -> { GRPC::Metadata.new },
      -> { }
    )
  end

  def closed? : Bool
    @closed
  end

  def close : Nil
    @closed = true
  end
end

class FailingClientInterceptor < GRPC::ClientInterceptor
  def call(request : GRPC::RequestEnvelope,
           ctx : GRPC::ClientContext,
           next_call : GRPC::UnaryClientCall) : GRPC::ResponseEnvelope
    raise GRPC::StatusError.new(GRPC::StatusCode::INTERNAL, "interceptor exploded")
  end
end

class FakeServerTransport
  include GRPC::Transport::ServerTransport

  @closed = false

  def initialize(@started : ::Channel(Nil))
  end

  def run_recv_loop : Nil
    @started.send(nil)
  end

  def closed? : Bool
    @closed
  end

  def close : Nil
    @closed = true
  end
end

class BlockingServerTransport
  include GRPC::Transport::ServerTransport

  getter started : ::Channel(Nil)
  getter closed_signal : ::Channel(Nil)

  def initialize
    @started = ::Channel(Nil).new(1)
    @closed_signal = ::Channel(Nil).new(1)
    @released = ::Channel(Nil).new(1)
    @closed = Atomic(Bool).new(false)
  end

  def run_recv_loop : Nil
    @started.send(nil)
    @released.receive
  end

  def closed? : Bool
    @closed.get
  end

  def close : Nil
    return unless @closed.compare_and_set(false, true)[1]
    @released.send(nil)
    @closed_signal.send(nil)
  end
end

describe "GRPC transport factory injection" do
  it "uses custom client transport factory in Channel" do
    created = 0
    fake = FakeClientTransport.new

    factory = ->(_host : String, _port : Int32, _use_tls : Bool, _tls_ctx : OpenSSL::SSL::Context::Client?, _config : GRPC::EndpointConfig) {
      created += 1
      fake.as(GRPC::Transport::ClientTransport)
    }

    channel = GRPC::Channel.new("127.0.0.1:12345", transport_factory: factory)

    begin
      response = channel.unary_call("test.Echo", "Echo", Bytes[1, 2, 3])
      response.status.ok?.should be_true
      response.raw.should eq(Bytes[1, 2, 3])
      created.should eq(1)
      fake.unary_calls.should eq(1)
    ensure
      channel.close
    end
  end

  it "uses custom server transport factory in Server" do
    started = ::Channel(Nil).new(1)
    created = 0

    factory = ->(_io : IO, _services : Hash(String, GRPC::Service), _interceptors : Array(GRPC::ServerInterceptor), _peer : String, _tls_sock : OpenSSL::SSL::Socket::Server?) {
      created += 1
      FakeServerTransport.new(started).as(GRPC::Transport::ServerTransport)
    }

    port_server = TCPServer.new("127.0.0.1", 0)
    port = port_server.local_address.port
    port_server.close

    server = GRPC::Server.new(transport_factory: factory)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      client = TCPSocket.new("127.0.0.1", port)
      client.close

      select
      when started.receive
      when timeout(1.second)
        fail "custom server transport factory was not used"
      end

      created.should eq(1)
    ensure
      server.stop
    end
  end

  it "closes active server transports on stop" do
    transport = BlockingServerTransport.new

    factory = ->(_io : IO, _services : Hash(String, GRPC::Service), _interceptors : Array(GRPC::ServerInterceptor), _peer : String, _tls_sock : OpenSSL::SSL::Socket::Server?) {
      transport.as(GRPC::Transport::ServerTransport)
    }

    port_server = TCPServer.new("127.0.0.1", 0)
    port = port_server.local_address.port
    port_server.close

    server = GRPC::Server.new(transport_factory: factory)
    server.bind("127.0.0.1:#{port}")
    server.start

    begin
      client = TCPSocket.new("127.0.0.1", port)
      client.close

      select
      when transport.started.receive
      when timeout(1.second)
        fail "server transport did not start"
      end

      server.stop

      select
      when transport.closed_signal.receive
      when timeout(1.second)
        fail "server stop did not close the active transport"
      end

      transport.closed?.should be_true
    ensure
      server.stop
    end
  end

  it "recreates the client transport after the previous one is closed" do
    created = [] of CyclingClientTransport
    factory = ->(_host : String, _port : Int32, _use_tls : Bool, _tls_ctx : OpenSSL::SSL::Context::Client?, _config : GRPC::EndpointConfig) {
      transport = CyclingClientTransport.new(created.size + 1)
      created << transport
      transport.as(GRPC::Transport::ClientTransport)
    }

    channel = GRPC::Channel.new("127.0.0.1:12345", transport_factory: factory)

    begin
      first_response = channel.unary_call("test.Echo", "Echo", Bytes.empty)
      first_response.raw.should eq(Bytes[1_u8])
      created.size.should eq(1)

      created.first.close

      second_response = channel.unary_call("test.Echo", "Echo", Bytes.empty)
      second_response.raw.should eq(Bytes[2_u8])
      created.size.should eq(2)
    ensure
      channel.close
    end
  end

  it "validates EndpointConfig numeric constraints eagerly" do
    expect_raises(ArgumentError, /connect_timeout/) do
      GRPC::Channel.new("127.0.0.1:12345", endpoint_config: GRPC::EndpointConfig.new(connect_timeout: 0.seconds))
    end

    expect_raises(ArgumentError, /tcp_keepalive/) do
      GRPC::Channel.new("127.0.0.1:12345", endpoint_config: GRPC::EndpointConfig.new(tcp_keepalive: -1.second))
    end

    expect_raises(ArgumentError, /keepalive.interval/) do
      GRPC::Channel.new(
        "127.0.0.1:12345",
        endpoint_config: GRPC::EndpointConfig.new(keepalive: GRPC::KeepaliveParams.new(interval: 0.seconds))
      )
    end

    expect_raises(ArgumentError, /keepalive.timeout/) do
      GRPC::Channel.new(
        "127.0.0.1:12345",
        endpoint_config: GRPC::EndpointConfig.new(keepalive: GRPC::KeepaliveParams.new(timeout: 0.seconds))
      )
    end

    expect_raises(ArgumentError, /concurrency_limit/) do
      GRPC::Channel.new("127.0.0.1:12345", endpoint_config: GRPC::EndpointConfig.new(concurrency_limit: 0))
    end

    expect_raises(ArgumentError, /rate_limit.limit/) do
      GRPC::Channel.new(
        "127.0.0.1:12345",
        endpoint_config: GRPC::EndpointConfig.new(rate_limit: GRPC::RateLimitConfig.new(0_u64, 1.second))
      )
    end

    expect_raises(ArgumentError, /rate_limit.period/) do
      GRPC::Channel.new(
        "127.0.0.1:12345",
        endpoint_config: GRPC::EndpointConfig.new(rate_limit: GRPC::RateLimitConfig.new(1_u64, 0.seconds))
      )
    end
  end

  it "enforces concurrency_limit for unary calls" do
    transport = BlockingUnaryTransport.new
    factory = ->(_host : String, _port : Int32, _use_tls : Bool, _tls_ctx : OpenSSL::SSL::Context::Client?, _config : GRPC::EndpointConfig) {
      transport.as(GRPC::Transport::ClientTransport)
    }
    config = GRPC::EndpointConfig.new(concurrency_limit: 1)
    channel = GRPC::Channel.new("127.0.0.1:12345", endpoint_config: config, transport_factory: factory)

    done1 = ::Channel(Nil).new(1)
    done2 = ::Channel(Nil).new(1)

    spawn do
      channel.unary_call("test.Echo", "Echo", Bytes[1])
      done1.send(nil)
    end

    select
    when transport.started.receive
    when timeout(1.second)
      fail "first unary call did not start"
    end

    spawn do
      channel.unary_call("test.Echo", "Echo", Bytes[2])
      done2.send(nil)
    end

    select
    when transport.started.receive
      fail "second unary call started before first finished"
    when timeout(120.milliseconds)
      # expected: second call is blocked by concurrency limit
    end

    transport.releases.send(nil)

    select
    when done1.receive
    when timeout(1.second)
      fail "first unary call did not complete"
    end

    select
    when transport.started.receive
    when timeout(1.second)
      fail "second unary call did not start after slot release"
    end

    transport.releases.send(nil)

    select
    when done2.receive
    when timeout(1.second)
      fail "second unary call did not complete"
    end

    channel.close
  end

  it "enforces rate_limit between request starts" do
    transport = TimestampUnaryTransport.new
    factory = ->(_host : String, _port : Int32, _use_tls : Bool, _tls_ctx : OpenSSL::SSL::Context::Client?, _config : GRPC::EndpointConfig) {
      transport.as(GRPC::Transport::ClientTransport)
    }
    config = GRPC::EndpointConfig.new(
      rate_limit: GRPC::RateLimitConfig.new(1_u64, 100.milliseconds)
    )
    channel = GRPC::Channel.new("127.0.0.1:12345", endpoint_config: config, transport_factory: factory)

    channel.unary_call("test.Echo", "Echo", Bytes[1])
    channel.unary_call("test.Echo", "Echo", Bytes[2])

    transport.starts.size.should eq(2)
    delta = transport.starts[1] - transport.starts[0]
    delta.should be >= 90.milliseconds

    channel.close
  end

  it "releases concurrency slot when server stream is cancelled" do
    transport = BlockingServerStreamTransport.new
    factory = ->(_host : String, _port : Int32, _use_tls : Bool, _tls_ctx : OpenSSL::SSL::Context::Client?, _config : GRPC::EndpointConfig) {
      transport.as(GRPC::Transport::ClientTransport)
    }
    config = GRPC::EndpointConfig.new(concurrency_limit: 1)
    channel = GRPC::Channel.new("127.0.0.1:12345", endpoint_config: config, transport_factory: factory)

    stream1 = channel.open_server_stream("test.Stream", "Watch", Bytes.empty)

    select
    when transport.started.receive
    when timeout(1.second)
      fail "first stream did not start"
    end

    stream2_ready = ::Channel(Nil).new(1)
    spawn do
      channel.open_server_stream("test.Stream", "Watch", Bytes.empty)
      stream2_ready.send(nil)
    end

    select
    when transport.started.receive
      fail "second stream started before first stream released its slot"
    when timeout(120.milliseconds)
      # expected: blocked by concurrency limit
    end

    stream1.cancel

    select
    when transport.started.receive
    when timeout(1.second)
      fail "second stream did not start after first stream cancellation"
    end

    select
    when stream2_ready.receive
    when timeout(1.second)
      fail "second stream call did not return"
    end

    channel.close
  end

  it "releases concurrency slots when a unary interceptor raises" do
    transport = BlockingUnaryTransport.new
    factory = ->(_host : String, _port : Int32, _use_tls : Bool, _tls_ctx : OpenSSL::SSL::Context::Client?, _config : GRPC::EndpointConfig) {
      transport.as(GRPC::Transport::ClientTransport)
    }
    channel = GRPC::Channel.new(
      "127.0.0.1:12345",
      interceptors: [FailingClientInterceptor.new] of GRPC::ClientInterceptor,
      endpoint_config: GRPC::EndpointConfig.new(concurrency_limit: 1),
      transport_factory: factory,
    )

    2.times do
      done = ::Channel(Nil).new(1)
      spawn do
        begin
          channel.unary_call("test.Echo", "Echo", Bytes[1])
          fail "expected interceptor to raise"
        rescue ex : GRPC::StatusError
          ex.code.should eq(GRPC::StatusCode::INTERNAL)
          done.send(nil)
        end
      end

      select
      when done.receive
      when timeout(1.second)
        fail "interceptor-raised unary call leaked its concurrency slot"
      end
    end

    select
    when transport.started.receive
      fail "failing interceptor should prevent the transport from being called"
    when timeout(120.milliseconds)
      # expected: interceptor failed before transport invocation
    end

    channel.close
  end
end
