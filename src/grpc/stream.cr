module GRPC
  # RawServerStream is the client-side view of a server-streaming or
  # bidi-streaming RPC response. It delivers raw protobuf bytes as they arrive.
  # Generated client stubs wrap this in a typed ServerStream(T) for ergonomic use.
  class RawServerStream
    include Enumerable(Bytes)

    def initialize(@messages : ::Channel(Bytes?), @status_proc : -> Status,
                   @trailers_proc : -> Metadata = -> { Metadata.new },
                   @cancel_proc : -> Nil = -> { },
                   @on_finish : -> Nil = -> { })
      @finished = false
      @finish_mutex = Mutex.new
    end

    def each(& : Bytes ->) : Nil
      begin
        loop do
          msg = @messages.receive?
          break if msg.nil?
          yield msg
        end
      ensure
        finish_once
      end
    end

    def status : Status
      @status_proc.call
    end

    def trailers : Metadata
      @trailers_proc.call
    end

    def cancel : Nil
      @cancel_proc.call
      finish_once
    end

    # with_on_finish appends a completion callback and returns self.
    # The callback is invoked once when iteration ends or cancel is called.
    def with_on_finish(hook : -> Nil) : self
      previous = @on_finish
      @on_finish = -> {
        previous.call
        hook.call
      }
      self
    end

    private def finish_once : Nil
      @finish_mutex.synchronize do
        return if @finished
        @finished = true
      end
      @on_finish.call
    end
  end

  # ServerStream(T) is the client's view of a server-streaming RPC response.
  # It supports typed iteration over response messages.
  #
  # Example (client streaming RPC):
  #   stream = client.list_features(rect)
  #   stream.each do |feature|
  #     pp feature
  #   end
  #   puts stream.status
  class ServerStream(T)
    include Enumerable(T)

    @messages : ::Channel(T | Exception)
    @status_proc : -> Status
    @trailers_proc : -> Metadata
    @cancel_proc : -> Nil
    @status_override : Status?

    def initialize(
      @status_proc : -> Status = -> { Status.ok },
      @trailers_proc : -> Metadata = -> { Metadata.new },
      @cancel_proc : -> Nil = -> { },
    )
      @messages = ::Channel(T | Exception).new(128)
      @status_override = nil
    end

    def each(& : T ->) : Nil
      loop do
        msg = @messages.receive? || break
        case msg
        when Exception then raise msg
        else                yield msg
        end
      end
    end

    def status : Status
      @status_override || @status_proc.call
    end

    def trailers : Metadata
      @trailers_proc.call
    end

    def cancel : Nil
      @cancel_proc.call
    end

    # --- Internal API for transport layer ---

    def push(msg : T) : Nil
      @messages.send(msg)
    end

    # finish signals end of stream.  Pass a status_override only for error
    # cases; on the normal path status comes from the transport via @status_proc.
    def finish(status_override : Status? = nil) : Nil
      @status_override = status_override
      @messages.close
    end
  end

  # ClientStream(Req, Res) is the client handle for a client-streaming RPC.
  # The client sends multiple request messages, then receives a single response.
  #
  # Example:
  #   call = client.record_route
  #   points.each { |p| call.send(p) }
  #   summary = call.close_and_recv
  class ClientStream(Req, Res)
    @send_proc : Req -> Nil
    @close_proc : -> Nil
    @result : ::Channel(Res | Exception)
    @closed : Bool
    @status_proc : -> Status
    @trailers_proc : -> Metadata
    @cancel_proc : -> Nil

    def initialize(@send_proc, @close_proc, @result,
                   @status_proc : -> Status = -> { Status.ok },
                   @trailers_proc : -> Metadata = -> { Metadata.new },
                   @cancel_proc : -> Nil = -> { })
      @closed = false
    end

    # send transmits a request message to the server.
    def send(message : Req) : Nil
      @send_proc.call(message)
    end

    # close_and_recv closes the request stream and waits for the server response.
    def close_and_recv : Res
      unless @closed
        @closed = true
        @close_proc.call
      end
      result = @result.receive
      case result
      when Exception then raise result
      else                result
      end
    end

    def cancel : Nil
      @cancel_proc.call
    end

    def status : Status
      @status_proc.call
    end

    def trailers : Metadata
      @trailers_proc.call
    end
  end

  # BidiCall(Req, Res) is the client handle for a bidirectional streaming RPC.
  # The client sends and receives messages concurrently.
  #
  # Example:
  #   call = client.chat
  #   spawn do
  #     messages.each { |msg| call.send(msg) }
  #     call.close_send
  #   end
  #   call.each { |reply| pp reply }
  class BidiCall(Req, Res)
    include Enumerable(Res)

    @send_proc : Req -> Nil
    @close_proc : -> Nil
    @recv_chan : ::Channel(Res | Exception)
    @closed : Bool
    @status_proc : -> Status
    @trailers_proc : -> Metadata
    @cancel_proc : -> Nil

    def initialize(@send_proc, @close_proc, @recv_chan,
                   @status_proc : -> Status = -> { Status.ok },
                   @trailers_proc : -> Metadata = -> { Metadata.new },
                   @cancel_proc : -> Nil = -> { })
      @closed = false
    end

    # send transmits a request message to the server.
    def send(message : Req) : Nil
      @send_proc.call(message)
    end

    # close_send signals the end of the client's request stream.
    def close_send : Nil
      return if @closed
      @closed = true
      @close_proc.call
    end

    def each(& : Res ->) : Nil
      loop do
        msg = @recv_chan.receive? || break
        case msg
        when Exception then raise msg
        else                yield msg
        end
      end
    end

    def cancel : Nil
      @cancel_proc.call
    end

    def status : Status
      @status_proc.call
    end

    def trailers : Metadata
      @trailers_proc.call
    end
  end

  # RawResponseStream is the internal transport-level handle for sending streaming
  # responses. It operates on raw Bytes and is used by the transport and interceptor
  # layers. Service implementations receive the typed ResponseStream(T) instead.
  class RawResponseStream
    def initialize(@send_proc : Bytes -> Nil)
    end

    # send_raw encodes one message body and pushes it to the HTTP/2 stream.
    def send_raw(message_bytes : Bytes) : Nil
      @send_proc.call(Codec.encode(message_bytes))
    end
  end

  # ResponseStream(T) is the public server-side handle for sending typed streaming
  # responses. Service implementations receive this in server-streaming and bidi RPCs.
  # The framework handles protobuf encoding internally.
  #
  # Example (server streaming handler):
  #   def range(req : Numbers::Number, writer : GRPC::ResponseStream(Numbers::Number),
  #             ctx : GRPC::ServerContext) : GRPC::Status
  #     (1..req.value).each { |i| writer.send(Numbers::Number.new(i)) }
  #     GRPC::Status.ok
  #   end
  class ResponseStream(T)
    def initialize(
      @raw : RawResponseStream,
      @marshaller : Marshaller(T) = ProtoMarshaller(T).new,
    )
    end

    # send serialises *message* to protobuf and transmits it to the client.
    def send(message : T) : Nil
      @raw.send_raw(@marshaller.dump(message))
    end
  end

  # RawClientCall is the client-side handle for a live client-streaming RPC.
  # All transport operations are captured as procs so the type is independent
  # of any concrete transport implementation.
  # Generated client stubs wrap this in a typed ClientStream(Req, Res).
  class RawClientCall
    def initialize(
      @send_proc : Bytes -> Nil,
      @close_and_recv_proc : -> Bytes,
      @status_proc : -> Status,
      @trailers_proc : -> Metadata,
      @cancel_proc : -> Nil,
      @on_finish : -> Nil = -> { },
    )
      @finished = false
      @finish_mutex = Mutex.new
    end

    # send_raw passes *message_bytes* (raw protobuf, no gRPC framing) to the transport.
    def send_raw(message_bytes : Bytes) : Nil
      @send_proc.call(message_bytes)
    end

    # close_and_recv closes the request stream and blocks until the server's
    # single response arrives.  Returns the raw response bytes.
    def close_and_recv : Bytes
      @close_and_recv_proc.call
    ensure
      finish_once
    end

    def cancel : Nil
      @cancel_proc.call
      finish_once
    end

    def status : Status
      @status_proc.call
    end

    def trailers : Metadata
      @trailers_proc.call
    end

    # with_on_finish appends a completion callback and returns self.
    # The callback is invoked once when close_and_recv/cancel completes.
    def with_on_finish(hook : -> Nil) : self
      previous = @on_finish
      @on_finish = -> {
        previous.call
        hook.call
      }
      self
    end

    private def finish_once : Nil
      @finish_mutex.synchronize do
        return if @finished
        @finished = true
      end
      @on_finish.call
    end
  end

  # RawBidiCall is the client-side handle for a full-duplex bidi-streaming RPC.
  # All transport operations are captured as procs so the type is independent
  # of any concrete transport implementation.
  # Generated client stubs wrap this in a typed BidiCall(Req, Res).
  class RawBidiCall
    include Enumerable(Bytes)

    def initialize(
      @send_proc : Bytes -> Nil,
      @close_proc : -> Nil,
      @messages : ::Channel(Bytes?),
      @status_proc : -> Status,
      @trailers_proc : -> Metadata,
      @cancel_proc : -> Nil,
      @on_finish : -> Nil = -> { },
    )
      @finished = false
      @finish_mutex = Mutex.new
    end

    # send_raw passes *message_bytes* (raw protobuf, no gRPC framing) to the transport.
    def send_raw(message_bytes : Bytes) : Nil
      @send_proc.call(message_bytes)
    end

    # close_send signals the end of the client request stream.
    def close_send : Nil
      @close_proc.call
    end

    def each(& : Bytes ->) : Nil
      begin
        loop do
          msg = @messages.receive?
          break if msg.nil?
          yield msg
        end
      ensure
        finish_once
      end
    end

    def cancel : Nil
      @cancel_proc.call
      finish_once
    end

    def status : Status
      @status_proc.call
    end

    def trailers : Metadata
      @trailers_proc.call
    end

    # with_on_finish appends a completion callback and returns self.
    # The callback is invoked once when iteration ends or cancel is called.
    def with_on_finish(hook : -> Nil) : self
      previous = @on_finish
      @on_finish = -> {
        previous.call
        hook.call
      }
      self
    end

    private def finish_once : Nil
      @finish_mutex.synchronize do
        return if @finished
        @finished = true
      end
      @on_finish.call
    end
  end
end
