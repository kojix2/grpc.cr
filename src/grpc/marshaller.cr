module GRPC
  # Marshaller converts between typed messages and raw bytes.
  #
  # gRPC transport remains byte-oriented; only typed boundaries should depend
  # on a marshaller.
  abstract class Marshaller(T)
    abstract def encode(value : T) : Bytes
    abstract def decode(bytes : Bytes) : T
  end

  # Default proto marshaller.
  # It delegates to generated message APIs.
  class ProtoMarshaller(T) < Marshaller(T)
    def encode(value : T) : Bytes
      value.encode
    end

    def decode(bytes : Bytes) : T
      T.decode(bytes)
    end
  end
end
