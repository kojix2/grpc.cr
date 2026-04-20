module GRPC
  # ServerContext holds per-call state accessible to server-side RPC handlers.
  #
  # Example:
  #   def say_hello(req : HelloRequest, ctx : GRPC::ServerContext) : HelloReply
  #     puts ctx.peer
  #     puts ctx.metadata["authorization"]?
  #     HelloReply.new(message: "Hello!")
  #   end
  class ServerContext
    getter metadata : Metadata
    getter peer : String
    property deadline : Time?

    @cancelled : Atomic(Bool)

    def initialize(@peer : String, @metadata : Metadata = Metadata.new, @deadline : Time? = nil)
      @cancelled = Atomic(Bool).new(false)
    end

    # cancel marks this call as cancelled from the server side.
    def cancel : Nil
      @cancelled.set(true)
    end

    def cancelled? : Bool
      @cancelled.get || timed_out?
    end

    def timed_out? : Bool
      if dl = @deadline
        Time.utc >= dl
      else
        false
      end
    end

    # check_active! raises a StatusError when the call is no longer active.
    # This lets transport and handlers stop work promptly on cancellation/timeout.
    def check_active! : Nil
      raise StatusError.new(StatusCode::DEADLINE_EXCEEDED, "deadline exceeded") if timed_out?
      raise StatusError.new(StatusCode::CANCELLED, "call cancelled") if @cancelled.get
    end
  end

  # ClientContext carries per-call options for outbound RPCs.
  #
  # Example:
  #   ctx = GRPC::ClientContext.new(
  #     metadata: {"authorization" => "Bearer token"},
  #     deadline: 5.seconds
  #   )
  #   reply = client.say_hello(req, ctx: ctx)
  class ClientContext
    getter metadata : Metadata
    property deadline : Time?

    def initialize(
      metadata : Hash(String, String) | Metadata = Metadata.new,
      deadline : Time::Span | Time? = nil,
    )
      @metadata = case metadata
                  when Metadata then metadata
                  else               Metadata.new(metadata)
                  end
      @deadline = case deadline
                  when Time::Span then Time.utc + deadline
                  when Time       then deadline
                  end
    end

    # effective_metadata returns the call metadata including grpc-timeout if a deadline is set.
    def effective_metadata : Metadata
      dl = @deadline
      return @metadata unless dl

      remaining = dl - Time.utc
      m = Metadata.new
      m.merge!(@metadata)
      if remaining.total_seconds > 0
        # gRPC-timeout is encoded as an integer with a unit suffix.
        # 'm' = milliseconds, 'S' = seconds, 'M' = minutes, 'H' = hours.
        timeout_ms = remaining.total_milliseconds.ceil.to_i64
        m.set("grpc-timeout", "#{timeout_ms}m")
      end
      m
    end

    # remaining returns how much time is left before the deadline.
    def remaining : Time::Span?
      dl = @deadline
      return unless dl
      r = dl - Time.utc
      r > Time::Span.zero ? r : Time::Span.zero
    end

    def timed_out? : Bool
      dl = @deadline
      return false unless dl
      Time.utc >= dl
    end
  end
end
