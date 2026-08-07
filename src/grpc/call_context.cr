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
    getter trailing_metadata : Metadata
    property deadline : Time?

    @cancelled : Atomic(Bool)

    def initialize(@peer : String, @metadata : Metadata = Metadata.new, @deadline : Time? = nil)
      @trailing_metadata = Metadata.new
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
      m.set("grpc-timeout", encode_timeout(remaining)) if remaining > Time::Span.zero
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

    def check_active! : Nil
      raise StatusError.new(StatusCode::DEADLINE_EXCEEDED, "deadline exceeded") if timed_out?
    end

    private def encode_timeout(remaining : Time::Span) : String
      limit = 99_999_999_i64
      units = {
        {'n', remaining.total_nanoseconds},
        {'u', remaining.total_microseconds},
        {'m', remaining.total_milliseconds},
        {'S', remaining.total_seconds},
        {'M', remaining.total_minutes},
        {'H', remaining.total_hours},
      }
      units.each do |unit, raw|
        value = raw.ceil.to_i64
        return "#{Math.max(value, 1_i64)}#{unit}" if value <= limit
      end
      "#{limit}H"
    end
  end
end
