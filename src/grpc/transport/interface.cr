module GRPC
  module Transport
    # ClientTransport is the interface that Channel depends on.
    # Concrete implementations (Http2ClientConnection) include this module.
    # All methods return public stream types; PendingStream stays internal.
    module ClientTransport
      abstract def unary_call(service : String, method : String,
                              request_body : Bytes, metadata : Metadata) : ResponseEnvelope

      abstract def open_server_stream(service : String, method : String,
                                      request_bytes : Bytes,
                                      metadata : Metadata) : RawServerStream

      abstract def open_bidi_stream_live(service : String, method : String,
                                         metadata : Metadata,
                                         send_queue_size : Int32) : RawBidiCall

      abstract def open_client_stream_live(service : String, method : String,
                                           metadata : Metadata,
                                           send_queue_size : Int32) : RawClientCall

      abstract def closed? : Bool
      abstract def close : Nil
    end

    # ServerTransport is the interface that Server depends on.
    # Concrete implementations (Http2ServerConnection) include this module.
    module ServerTransport
      abstract def run_recv_loop : Nil
    end
  end
end
