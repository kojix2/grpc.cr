module GRPC
  # Marshaller converts between typed messages and raw bytes.
  #
  # gRPC transport remains byte-oriented; only typed boundaries should depend
  # on a marshaller.
  abstract class Marshaller(T)
    abstract def dump(value : T) : Bytes
    abstract def load(bytes : Bytes) : T
  end

  # Default proto marshaller.
  # It delegates to generated message APIs.
  class ProtoMarshaller(T) < Marshaller(T)
    def dump(value : T) : Bytes
      value.to_proto
    end

    def load(bytes : Bytes) : T
      T.from_proto(bytes)
    end
  end
end
