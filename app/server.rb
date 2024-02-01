require "socket"

require_relative "command_line_options_parser"
require_relative "database"
require_relative "resp_decoder"

$stdout.sync = true

class RedisServer
  def initialize(port)
    @port = port
    @database = Database.new
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

      handle_client_command(client, command, arguments)
    rescue Errno::EPIPE, IncompleteRESP => e
      puts "Connection closed: #{peer_address} (#{e.class})"
      return
    end
  end

  def handle_client_command(client, command, arguments)
    command = command.downcase

    if respond_to?("handle_#{command}_command")
      send("handle_#{command}_command", client, arguments)
    else
      client.write("-ERR unknown command `#{command}`\r\n")
    end
  end

  def handle_ping_command(client, arguments)
    client.write("+PONG\r\n")
  end

  def handle_echo_command(client, arguments)
    client.write("$#{arguments[0].length}\r\n#{arguments[0]}\r\n")
  end

  def handle_get_command(client, arguments)
    value = @database.get(arguments[0])

    if value.nil?
      client.write("$-1\r\n")
    else
      client.write("$#{value.length}\r\n#{value}\r\n")
    end
  end

  def handle_set_command(client, arguments)
    option_arguments = []

    if arguments.length > 2
      option_arguments = arguments[2..]
      arguments = arguments[0..1]
    end

    key, value = arguments

    if option_arguments.empty?
      @database.set(key, value)
      client.write("+OK\r\n")
    elsif option_arguments.first.eql?("px") && option_arguments.length == 2
      @database.set_with_expiry(key, value, option_arguments[1].to_i)
      client.write("+OK\r\n")
    else
      client.write("-ERR unsupported SET options: #{option_arguments.join(" ")}\r\n")
    end
  end
end

command_line_options = CommandLineOptionsParser.parse(ARGV)
RedisServer.new(command_line_options["port"]&.to_i || 6379).start
