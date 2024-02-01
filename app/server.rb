require "socket"

require_relative "resp_decoder"

$stdout.sync = true

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
      command, *arguments = RESPDecoder.decode(client)
      puts "Received: #{command} #{arguments.join(" ")}"

      case command.downcase
      when "ping"
        client.write("+PONG\r\n")
      when "echo"
        client.write("$#{arguments[0].length}\r\n#{arguments[0]}\r\n")
      else
        client.write("-ERR unknown command `#{command}`\r\n")
      end
    rescue Errno::EPIPE, IncompleteRESP => e
      puts "Connection closed: #{peer_address} (#{e.class})"
      return
    end
  end
end

YourRedisServer.new(6379).start
