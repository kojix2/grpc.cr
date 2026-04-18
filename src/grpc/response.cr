module GRPC
  # UnaryResponse is the typed client-facing view of a unary RPC result.
  # It exposes the decoded message, initial metadata, trailing metadata,
  # and final gRPC status.
  struct UnaryResponse(T)
    getter message : T?
    getter initial_metadata : GRPC::Metadata
    getter trailing_metadata : GRPC::Metadata
    getter status : GRPC::Status

    def initialize(
      @message : T?,
      @initial_metadata : GRPC::Metadata = GRPC::Metadata.new,
      @trailing_metadata : GRPC::Metadata = GRPC::Metadata.new,
      @status : GRPC::Status = GRPC::Status.ok,
    )
    end

    def ok! : T
      raise GRPC::StatusError.new(@status, @trailing_metadata) unless @status.ok?
      message = @message
      raise GRPC::StatusError.new(GRPC::Status.internal("unary response message missing"), @trailing_metadata) if message.nil?
      message
    end
  end
end
