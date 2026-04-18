require "base64"

module GRPC
  # Metadata represents gRPC metadata (HTTP/2 headers), carrying key-value pairs.
  # Binary-valued keys must end in "-bin" and their values are base64-encoded.
  class Metadata
    alias Value = String | Bytes

    def initialize
      @data = Hash(String, Array(Value)).new { |hash, key| hash[key] = [] of Value }
    end

    def initialize(hash : Hash(String, String))
      @data = Hash(String, Array(Value)).new { |store, key| store[key] = [] of Value }
      hash.each { |k, v| set(k, v) }
    end

    def set(key : String, value : String) : Nil
      @data[normalize_key(key)] = [value]
    end

    def add(key : String, value : String) : Nil
      @data[normalize_key(key)] << value
    end

    def set_bin(key : String, value : Bytes) : Nil
      @data[normalize_key(key)] = [value.dup]
    end

    def add_bin(key : String, value : Bytes) : Nil
      @data[normalize_key(key)] << value.dup
    end

    def add_wire(key : String, value : String) : Nil
      normalized = normalize_key(key)
      if binary_key?(normalized)
        begin
          add_bin(normalized, Base64.decode(value))
        rescue
          add(normalized, value)
        end
      else
        add(normalized, value)
      end
    end

    def get(key : String) : String?
      @data[normalize_key(key)]?.try do |values|
        values.each do |value|
          return value if value.is_a?(String)
        end
        nil
      end
    end

    def get_all(key : String) : Array(String)
      values = @data[normalize_key(key)]?
      return [] of String unless values

      result = [] of String
      values.each do |value|
        result << value if value.is_a?(String)
      end
      result
    end

    def get_bin(key : String) : Bytes?
      @data[normalize_key(key)]?.try do |values|
        values.each do |value|
          return value if value.is_a?(Bytes)
        end
        nil
      end
    end

    def get_all_bin(key : String) : Array(Bytes)
      values = @data[normalize_key(key)]?
      return [] of Bytes unless values

      result = [] of Bytes
      values.each do |value|
        result << value if value.is_a?(Bytes)
      end
      result
    end

    def []?(key : String) : String?
      get(key)
    end

    def []=(key : String, value : String) : String
      set(key, value)
      value
    end

    def each(&block : String, String ->) : Nil
      each_wire(&block)
    end

    def each_value(&block : String, Value ->) : Nil
      @data.each do |key, values|
        values.each { |value| block.call(key, value) }
      end
    end

    def each_wire(&block : String, String ->) : Nil
      each_value do |key, value|
        wire_value = value.is_a?(Bytes) ? Base64.strict_encode(value) : value
        block.call(key, wire_value)
      end
    end

    def to_h : Hash(String, String)
      result = {} of String => String
      each_wire do |key, value|
        result[key] ||= value
      end
      result
    end

    def empty? : Bool
      @data.empty?
    end

    def merge!(other : Metadata) : self
      other.each_value do |key, value|
        case value
        when String then add(key, value)
        when Bytes  then add_bin(key, value)
        end
      end
      self
    end

    def dup : self
      copy = self.class.new
      copy.merge!(self)
      copy
    end

    private def normalize_key(key : String) : String
      key.downcase
    end

    private def binary_key?(key : String) : Bool
      key.ends_with?("-bin")
    end
  end
end
