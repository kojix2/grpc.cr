module GRPC
  # StatusError is raised when an RPC call fails with a non-OK gRPC status.
  # This is the primary exception users should rescue in application code.
  #
  # Example:
  #   begin
  #     reply = client.say_hello(req)
  #   rescue ex : GRPC::StatusError
  #     puts ex.code     # => GRPC::StatusCode::NOT_FOUND
  #     puts ex.message  # => "user not found"
  #   end
  class StatusError < Exception
    getter status : Status
    getter trailers : Metadata

    def initialize(@status : Status, @trailers : Metadata = Metadata.new, cause : Exception? = nil)
      super("gRPC error #{@status.code}: #{@status.message}", cause: cause)
    end

    def initialize(code : StatusCode, message : String = "", cause : Exception? = nil)
      @status = Status.new(code, message)
      @trailers = Metadata.new
      super("gRPC error #{code}: #{message}", cause: cause)
    end

    def code : StatusCode
      @status.code
    end

    def message : String
      @status.message
    end
  end

  # ConnectionError is raised when transport connectivity fails.
  # It surfaces to callers as StatusCode::UNAVAILABLE.
  class ConnectionError < Exception
    getter status : Status

    def initialize(message : String)
      @status = Status.new(StatusCode::UNAVAILABLE, message)
      super(message)
    end
  end
end
