require "socket"

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    puts "Listening on port #{@port}..."
    server = TCPServer.new(@port)
    client = server.accept
  end
end

YourRedisServer.new(6379).start
