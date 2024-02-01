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
    peer_address = "#{client.peeraddr[3]}:#{client.peeraddr[1]}"
    puts "Handling client: #{peer_address}"

    loop do
      client.recv(1024)
      client.write("+PONG\r\n")
    rescue Errno::EPIPE
      puts "Connection closed: #{peer_address}"
      return
    end
  end
end

YourRedisServer.new(6379).start
