require "socket"

class YourRedisServer
  def initialize(port)
    @port = port
  end

  def start
    puts "Listening on port #{@port}..."
    server = TCPServer.new(@port)

    loop do
      client = server.accept
      Thread.new { handle_client(client) }
    end
  end

  protected

  def handle_client(client)
    loop do
      client.recv(1024)
      client.write("+PONG\r\n")
    end
  end
end

YourRedisServer.new(6379).start
