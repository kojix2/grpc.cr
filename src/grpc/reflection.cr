module GRPC
  module Reflection
    struct Request
      getter host : String
      getter kind : Symbol
      getter value : String

      def initialize(@host : String, @kind : Symbol, @value : String)
      end

      def self.decode(bytes : Bytes) : self
        cursor = 0
        host = ""
        kind = :unknown
        value = ""

        while cursor < bytes.size
          tag, cursor = Wire.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          case field_number
          when 1
            if wire_type == 2
              host, cursor = Wire.read_string(bytes, cursor)
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          when 3
            if wire_type == 2
              value, cursor = Wire.read_string(bytes, cursor)
              kind = :file_by_filename
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          when 4
            if wire_type == 2
              value, cursor = Wire.read_string(bytes, cursor)
              kind = :file_containing_symbol
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          when 7
            if wire_type == 2
              value, cursor = Wire.read_string(bytes, cursor)
              kind = :list_services
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          else
            cursor = Wire.skip_field(bytes, cursor, wire_type)
          end
        end

        new(host, kind, value)
      end
    end

    struct DescriptorIndex
      getter file_name : String
      getter dependency_names : Array(String)
      getter symbol_names : Array(String)

      def initialize(@file_name : String, @dependency_names : Array(String), @symbol_names : Array(String))
      end

      def self.parse(bytes : Bytes) : self
        cursor = 0
        file_name = ""
        package_name = ""
        dependency_names = [] of String
        symbol_names = Set(String).new

        while cursor < bytes.size
          tag, cursor = Wire.read_varint(bytes, cursor)
          file_name, package_name, cursor = parse_file_descriptor_field(
            bytes,
            tag,
            cursor,
            file_name,
            package_name,
            dependency_names,
            symbol_names,
          )
        end

        raise ArgumentError.new("descriptor file name missing") if file_name.empty?
        new(file_name, dependency_names, symbol_names.to_a.sort)
      end

      private def self.parse_descriptor_proto(bytes : Bytes, parent_name : String, symbol_names : Set(String)) : Nil
        cursor = 0
        name = ""
        nested_messages = [] of Bytes
        nested_enums = [] of Bytes

        while cursor < bytes.size
          tag, cursor = Wire.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          case field_number
          when 1
            if wire_type == 2
              name, cursor = Wire.read_string(bytes, cursor)
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          when 3
            if wire_type == 2
              nested_bytes, cursor = Wire.read_bytes(bytes, cursor)
              nested_messages << nested_bytes
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          when 4
            if wire_type == 2
              enum_bytes, cursor = Wire.read_bytes(bytes, cursor)
              nested_enums << enum_bytes
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          else
            cursor = Wire.skip_field(bytes, cursor, wire_type)
          end
        end

        return if name.empty?

        current_name = qualify(parent_name, name)
        symbol_names.add(current_name)
        nested_messages.each { |child_message_bytes| parse_descriptor_proto(child_message_bytes, current_name, symbol_names) }
        nested_enums.each { |child_enum_bytes| parse_enum_descriptor(child_enum_bytes, current_name, symbol_names) }
      end

      private def self.parse_enum_descriptor(bytes : Bytes, parent_name : String, symbol_names : Set(String)) : Nil
        cursor = 0
        name = ""

        while cursor < bytes.size
          tag, cursor = Wire.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          if field_number == 1 && wire_type == 2
            name, cursor = Wire.read_string(bytes, cursor)
          else
            cursor = Wire.skip_field(bytes, cursor, wire_type)
          end
        end

        return if name.empty?
        symbol_names.add(qualify(parent_name, name))
      end

      private def self.parse_service_descriptor(bytes : Bytes, package_name : String, symbol_names : Set(String)) : Nil
        cursor = 0
        name = ""
        method_names = [] of String

        while cursor < bytes.size
          tag, cursor = Wire.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          case field_number
          when 1
            if wire_type == 2
              name, cursor = Wire.read_string(bytes, cursor)
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          when 2
            if wire_type == 2
              method_bytes, cursor = Wire.read_bytes(bytes, cursor)
              if method_name = parse_method_name(method_bytes)
                method_names << method_name
              end
            else
              cursor = Wire.skip_field(bytes, cursor, wire_type)
            end
          else
            cursor = Wire.skip_field(bytes, cursor, wire_type)
          end
        end

        return if name.empty?

        service_name = qualify(package_name, name)
        symbol_names.add(service_name)
        method_names.each do |rpc_method_name|
          symbol_names.add("#{service_name}.#{rpc_method_name}")
        end
      end

      private def self.parse_file_descriptor_field(bytes : Bytes, tag : UInt64, cursor : Int32,
                                                   file_name : String, package_name : String,
                                                   dependency_names : Array(String),
                                                   symbol_names : Set(String)) : {String, String, Int32}
        field_number = (tag >> 3).to_i32
        wire_type = (tag & 0x7_u64).to_i32

        case field_number
        when 1
          file_name, cursor = read_optional_string_field(bytes, cursor, wire_type, file_name)
        when 2
          package_name, cursor = read_optional_string_field(bytes, cursor, wire_type, package_name)
        when 3
          cursor = read_dependency_field(bytes, cursor, wire_type, dependency_names)
        when 4
          cursor = read_message_field(bytes, cursor, wire_type, package_name, symbol_names)
        when 5
          cursor = read_enum_field(bytes, cursor, wire_type, package_name, symbol_names)
        when 6
          cursor = read_service_field(bytes, cursor, wire_type, package_name, symbol_names)
        else
          cursor = Wire.skip_field(bytes, cursor, wire_type)
        end

        {file_name, package_name, cursor}
      end

      private def self.read_optional_string_field(bytes : Bytes, cursor : Int32, wire_type : Int32,
                                                  current_value : String) : {String, Int32}
        return Wire.read_string(bytes, cursor) if wire_type == 2
        {current_value, Wire.skip_field(bytes, cursor, wire_type)}
      end

      private def self.read_dependency_field(bytes : Bytes, cursor : Int32, wire_type : Int32,
                                             dependency_names : Array(String)) : Int32
        if wire_type == 2
          dependency_name, cursor = Wire.read_string(bytes, cursor)
          dependency_names << dependency_name
          return cursor
        end

        Wire.skip_field(bytes, cursor, wire_type)
      end

      private def self.read_message_field(bytes : Bytes, cursor : Int32, wire_type : Int32,
                                          package_name : String, symbol_names : Set(String)) : Int32
        if wire_type == 2
          message_bytes, cursor = Wire.read_bytes(bytes, cursor)
          parse_descriptor_proto(message_bytes, package_name, symbol_names)
          return cursor
        end

        Wire.skip_field(bytes, cursor, wire_type)
      end

      private def self.read_enum_field(bytes : Bytes, cursor : Int32, wire_type : Int32,
                                       package_name : String, symbol_names : Set(String)) : Int32
        if wire_type == 2
          enum_bytes, cursor = Wire.read_bytes(bytes, cursor)
          parse_enum_descriptor(enum_bytes, package_name, symbol_names)
          return cursor
        end

        Wire.skip_field(bytes, cursor, wire_type)
      end

      private def self.read_service_field(bytes : Bytes, cursor : Int32, wire_type : Int32,
                                          package_name : String, symbol_names : Set(String)) : Int32
        if wire_type == 2
          service_bytes, cursor = Wire.read_bytes(bytes, cursor)
          parse_service_descriptor(service_bytes, package_name, symbol_names)
          return cursor
        end

        Wire.skip_field(bytes, cursor, wire_type)
      end

      private def self.parse_method_name(bytes : Bytes) : String?
        cursor = 0

        while cursor < bytes.size
          tag, cursor = Wire.read_varint(bytes, cursor)
          field_number = (tag >> 3).to_i32
          wire_type = (tag & 0x7_u64).to_i32

          if field_number == 1 && wire_type == 2
            name, cursor = Wire.read_string(bytes, cursor)
            return name
          end

          cursor = Wire.skip_field(bytes, cursor, wire_type)
        end

        nil
      end

      private def self.qualify(prefix : String, name : String) : String
        return name if prefix.empty?
        "#{prefix}.#{name}"
      end
    end

    module Wire
      def self.write_varint(io : IO, value : UInt64) : Nil
        current = value
        loop do
          byte = (current & 0x7F_u64).to_u8
          current >>= 7
          if current == 0_u64
            io.write_byte(byte)
            break
          else
            io.write_byte(byte | 0x80_u8)
          end
        end
      end

      def self.read_varint(bytes : Bytes, start : Int32) : {UInt64, Int32}
        result = 0_u64
        shift = 0
        cursor = start

        while cursor < bytes.size
          byte = bytes[cursor]
          cursor += 1
          result |= ((byte & 0x7F_u8).to_u64 << shift)
          return {result, cursor} if (byte & 0x80_u8) == 0_u8
          shift += 7
          raise ArgumentError.new("malformed varint") if shift >= 64
        end

        raise ArgumentError.new("truncated varint")
      end

      def self.read_string(bytes : Bytes, start : Int32) : {String, Int32}
        value, cursor = read_bytes(bytes, start)
        {String.new(value), cursor}
      end

      def self.read_bytes(bytes : Bytes, start : Int32) : {Bytes, Int32}
        raw_length, cursor = read_varint(bytes, start)
        length = raw_length.to_i
        ensure_length!(bytes, cursor, length)
        {bytes[cursor, length], cursor + length}
      end

      def self.skip_field(bytes : Bytes, start : Int32, wire_type : Int32) : Int32
        case wire_type
        when 0
          _, cursor = read_varint(bytes, start)
          cursor
        when 1
          ensure_length!(bytes, start, 8)
          start + 8
        when 2
          _, cursor = read_bytes(bytes, start)
          cursor
        when 5
          ensure_length!(bytes, start, 4)
          start + 4
        else
          raise ArgumentError.new("unsupported wire type: #{wire_type}")
        end
      end

      def self.ensure_length!(bytes : Bytes, start : Int32, length : Int32) : Nil
        if length < 0 || start < 0 || start + length > bytes.size
          raise ArgumentError.new("truncated message")
        end
      end

      def self.write_key(io : IO, field_number : Int32, wire_type : Int32) : Nil
        write_varint(io, ((field_number << 3) | wire_type).to_u64)
      end

      def self.write_varint_field(io : IO, field_number : Int32, value : UInt64) : Nil
        write_key(io, field_number, 0)
        write_varint(io, value)
      end

      def self.write_string(io : IO, field_number : Int32, value : String) : Nil
        write_key(io, field_number, 2)
        bytes = value.to_slice
        write_varint(io, bytes.size.to_u64)
        io.write(bytes)
      end

      def self.write_bytes(io : IO, field_number : Int32, value : Bytes) : Nil
        write_key(io, field_number, 2)
        write_varint(io, value.size.to_u64)
        io.write(value)
      end
    end

    class Service < GRPC::Service
      SERVICE_FULL_NAME = "grpc.reflection.v1alpha.ServerReflection"

      @service_names : Set(String)
      @descriptor_indexes : Hash(String, DescriptorIndex)
      @descriptors_by_name : Hash(String, Bytes)
      @descriptor_files_by_symbol : Hash(String, Array(String))

      def initialize
        @service_names = Set{SERVICE_FULL_NAME}
        @descriptor_indexes = {} of String => DescriptorIndex
        @descriptors_by_name = {} of String => Bytes
        @descriptor_files_by_symbol = {} of String => Array(String)
      end

      def service_full_name : String
        SERVICE_FULL_NAME
      end

      def register_service(service_name : String) : self
        @service_names.add(service_name)
        self
      end

      def add_file_descriptor(bytes : Bytes) : self
        index = DescriptorIndex.parse(bytes)
        if previous_index = @descriptor_indexes[index.file_name]?
          remove_symbol_mappings(previous_index)
        end
        @descriptor_indexes[index.file_name] = index
        @descriptors_by_name[index.file_name] = bytes
        add_symbol_mappings(index)
        self
      end

      def dispatch(method : String, request_body : Bytes, ctx : GRPC::ServerContext) : {Bytes, GRPC::Status}
        _ = method
        _ = request_body
        _ = ctx
        {Bytes.empty, GRPC::Status.unimplemented("unknown method")}
      end

      def bidi_streaming?(method : String) : Bool
        method == "ServerReflectionInfo"
      end

      def dispatch_bidi_stream(method : String, requests : GRPC::RawRequestStream,
                               ctx : GRPC::ServerContext, writer : GRPC::RawResponseStream) : GRPC::Status
        _ = ctx
        return GRPC::Status.unimplemented("unknown method") unless method == "ServerReflectionInfo"

        requests.each do |request_bytes|
          request = Request.decode(request_bytes)
          writer.send_raw(handle_request(request, request_bytes))
        rescue ex
          writer.send_raw(encode_response("", request_bytes, 7, encode_error_response(
            GRPC::StatusCode::INVALID_ARGUMENT.value,
            ex.message || "invalid reflection request"
          )))
        end

        GRPC::Status.ok
      end

      private def handle_request(request : Request, original_request : Bytes) : Bytes
        case request.kind
        when :list_services
          encode_response(request.host, original_request, 6, encode_list_services_response)
        when :file_by_filename
          if descriptors = descriptor_closure_for_file(request.value)
            encode_response(request.host, original_request, 4, encode_file_descriptor_response(descriptors))
          else
            encode_response(request.host, original_request, 7, encode_error_response(5, "file not found: #{request.value}"))
          end
        when :file_containing_symbol
          if descriptors = descriptor_closure_for_symbol(request.value)
            encode_response(request.host, original_request, 4, encode_file_descriptor_response(descriptors))
          else
            encode_response(request.host, original_request, 7, encode_error_response(5, "symbol not found: #{request.value}"))
          end
        else
          encode_response(request.host, original_request, 7, encode_error_response(12, "unsupported reflection request"))
        end
      end

      private def descriptor_closure_for_file(file_name : String) : Array(Bytes)?
        return unless @descriptor_indexes.has_key?(file_name)
        collect_descriptor_closure(file_name)
      end

      private def add_symbol_mappings(index : DescriptorIndex) : Nil
        index.symbol_names.each do |symbol_name|
          files = (@descriptor_files_by_symbol[symbol_name] ||= [] of String)
          files << index.file_name unless files.includes?(index.file_name)
        end
      end

      private def remove_symbol_mappings(index : DescriptorIndex) : Nil
        index.symbol_names.each do |symbol_name|
          next unless files = @descriptor_files_by_symbol[symbol_name]?
          files.reject! { |file_name| file_name == index.file_name }
          @descriptor_files_by_symbol.delete(symbol_name) if files.empty?
        end
      end

      private def descriptor_closure_for_symbol(symbol_name : String) : Array(Bytes)?
        file_names = @descriptor_files_by_symbol[symbol_name]?
        return if file_names.nil? || file_names.empty?
        collect_descriptor_closure(file_names.first)
      end

      private def collect_descriptor_closure(root_file_name : String) : Array(Bytes)
        descriptor_bytes = [] of Bytes
        seen = Set(String).new
        pending = [root_file_name]

        until pending.empty?
          file_name = pending.pop
          next if seen.includes?(file_name)
          seen.add(file_name)

          if bytes = @descriptors_by_name[file_name]?
            descriptor_bytes << bytes
          end

          if index = @descriptor_indexes[file_name]?
            index.dependency_names.reverse_each do |dependency_name|
              pending << dependency_name
            end
          end
        end

        descriptor_bytes
      end

      private def encode_response(host : String, original_request : Bytes, field_number : Int32, payload : Bytes) : Bytes
        io = IO::Memory.new
        Wire.write_string(io, 1, host) unless host.empty?
        Wire.write_bytes(io, 2, original_request)
        Wire.write_bytes(io, field_number, payload)
        io.to_slice
      end

      private def encode_list_services_response : Bytes
        io = IO::Memory.new
        service_names = @service_names.to_a
        service_names.sort!
        service_names.each do |service_name|
          entry = IO::Memory.new
          Wire.write_string(entry, 1, service_name)
          Wire.write_bytes(io, 1, entry.to_slice)
        end
        io.to_slice
      end

      private def encode_file_descriptor_response(descriptor_bytes : Array(Bytes)) : Bytes
        io = IO::Memory.new
        descriptor_bytes.each do |bytes|
          Wire.write_bytes(io, 1, bytes)
        end
        io.to_slice
      end

      private def encode_error_response(code : Int32, message : String) : Bytes
        io = IO::Memory.new
        Wire.write_varint_field(io, 1, code.to_u64)
        Wire.write_string(io, 2, message)
        io.to_slice
      end
    end
  end
end
