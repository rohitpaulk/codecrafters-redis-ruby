require "socket"

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    puts "Listening on port #{@port}..."
    server = TCPServer.new(@port)
    client = server.accept
    client.recv(1024)
    client.write("+PONG\r\n")
  end
end

YourRedisServer.new(6379).start
