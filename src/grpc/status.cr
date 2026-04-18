module GRPC
  # gRPC status codes as defined in https://grpc.github.io/grpc/core/md_doc_statuscodes.html
  enum StatusCode : Int32
    OK                  =  0
    CANCELLED           =  1
    UNKNOWN             =  2
    INVALID_ARGUMENT    =  3
    DEADLINE_EXCEEDED   =  4
    NOT_FOUND           =  5
    ALREADY_EXISTS      =  6
    PERMISSION_DENIED   =  7
    RESOURCE_EXHAUSTED  =  8
    FAILED_PRECONDITION =  9
    ABORTED             = 10
    OUT_OF_RANGE        = 11
    UNIMPLEMENTED       = 12
    INTERNAL            = 13
    UNAVAILABLE         = 14
    DATA_LOSS           = 15
    UNAUTHENTICATED     = 16
  end

  class Status
    getter code : StatusCode
    getter message : String
    getter details : Bytes?

    def initialize(@code : StatusCode, @message : String = "", @details : Bytes? = nil)
    end

    def ok? : Bool
      @code == StatusCode::OK
    end

    def self.ok : Status
      new(StatusCode::OK)
    end

    def self.cancelled(message = "") : Status
      new(StatusCode::CANCELLED, message)
    end

    def self.unknown(message = "") : Status
      new(StatusCode::UNKNOWN, message)
    end

    def self.invalid_argument(message = "") : Status
      new(StatusCode::INVALID_ARGUMENT, message)
    end

    def self.not_found(message = "") : Status
      new(StatusCode::NOT_FOUND, message)
    end

    def self.unimplemented(message = "") : Status
      new(StatusCode::UNIMPLEMENTED, message)
    end

    def self.internal(message = "") : Status
      new(StatusCode::INTERNAL, message)
    end

    def self.unavailable(message = "") : Status
      new(StatusCode::UNAVAILABLE, message)
    end

    def to_s(io : IO) : Nil
      io << "Status(#{@code}"
      io << ", #{@message}" unless @message.empty?
      if details = @details
        io << ", details=#{details.size} bytes"
      end
      io << ")"
    end
  end
end
