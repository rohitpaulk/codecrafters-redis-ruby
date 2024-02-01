require "socket"
require "securerandom"

require_relative "command_line_options_parser"
require_relative "database"
require_relative "resp_connection"
require_relative "resp_decoder"
require_relative "resp_encoder"
require_relative "replication_client"

$stdout.sync = true

class RedisServer
  attr_reader :replica_of
  attr_reader :port

  def initialize(command_line_options)
    @port = command_line_options["port"] || 6379
    @replica_of = command_line_options["replicaof"]
    @replication_id = SecureRandom.hex(40)
    @replication_offset = 0
    @database = Database.new
    @replication_client = ReplicationClient.new(self) if @replica_of
  end

  def start
    @replication_client&.start!

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
    case command.downcase
    when "ping"
      handle_ping_command(client, arguments)
    when "echo"
      handle_echo_command(client, arguments)
    when "set"
      handle_set_command(client, arguments)
    when "get"
      handle_get_command(client, arguments)
    when "info"
      handle_info_command(client, arguments)
    when "replconf"
      client.write("+OK\r\n") # CodeCrafters runs master code too when only testing replica
    else
      client.write(RESPEncoder.encode_error_message("unknown command `#{command}`"))
    end
  end

  def handle_ping_command(client, arguments)
    client.write(RESPEncoder.encode("PONG"))
  end

  def handle_echo_command(client, arguments)
    client.write(RESPEncoder.encode(arguments[0]))
  end

  def handle_get_command(client, arguments)
    value = @database.get(arguments[0])
    client.write(RESPEncoder.encode(value))
  end

  def handle_info_command(client, arguments)
    case arguments[0]
    when "replication"
      keys = {
        "role" => @replica_of ? "slave" : "master",
        "master_replid" => @replication_id,
        "master_repl_offset" => @replication_offset,
      }

      client.write(RESPEncoder.encode("#{keys.map { |k, v| "#{k}:#{v}" }.join("\n")}"))
    else
      client.write(RESPEncoder.encode_error_message("unsupported INFO options: #{arguments.join(" ")}"))
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
      client.write(RESPEncoder.encode_error_message("unsupported SET options: #{option_arguments.join(" ")}"))
    end
  end
end

command_line_options = CommandLineOptionsParser.parse(ARGV)
RedisServer.new(command_line_options).start
