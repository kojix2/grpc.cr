require "./protoc-gen-crystal-grpc"

buf = IO::Memory.new
IO.copy(STDIN, buf)
request_bytes = buf.to_slice
request = PluginCodeGeneratorRequest.parse(request_bytes)
generator = CrystalGrpcCodeGenerator.new
STDOUT.write(generator.run(request))
