require "socket"
require "securerandom"
require "stringio"

require_relative "command_line_options_parser"
require_relative "database"
require_relative "resp_connection"
require_relative "resp_decoder"
require_relative "resp_encoder"
require_relative "replication_client"
require_relative "replication_stream"

$stdout.sync = true

class RedisServer
  attr_reader :replica_of
  attr_reader :port
  attr_reader :replication_id
  attr_reader :replication_offset

  def initialize(command_line_options)
    @port = command_line_options["port"] || 6379
    @replica_of = command_line_options["replicaof"]
    @replication_id = SecureRandom.hex(40)
    @replication_offset = 0
    @database = Database.new
    @replication_client = ReplicationClient.new(self) if @replica_of
    @replication_streams = []
  end

  def start
    if @replication_client
      puts "Replica: Initiating replication client"

      Thread.new do
        @replication_client.start!

        @replication_client.each_command do |command, arguments|
          dummy_io = StringIO.new
          handle_client_command(dummy_io, command, arguments)
        end
      end
    end

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
      when "psync" # For PSYNC, we need to take over the loop and handle replication messages instead
        replication_stream = ReplicationStream.new(self, client)
        replication_stream.start!
        @replication_streams << replication_stream
      when "replconf" # REPLCONF isn't a "client" command, so let's handle it here
        client.write("+OK\r\n") # We don't support any options for now
      else
        handle_client_command(client, command, arguments)

        if is_write_command?(command)
          puts "Propagating command to #{@replication_streams.size} replicas: #{command} #{arguments.join(" ")}"

          @replication_streams.each do |replication_stream|
            Thread.new do
              replication_stream.propagate_command(command, arguments)
            end
          end
        end
      end
    rescue Errno::EPIPE, IncompleteRESP, Errno::ECONNRESET => e
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
    when "wait"
      handle_wait_command(client, arguments)
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

  def handle_wait_command(client, arguments)
    current_replication_offset = @replication_offset
    expected_number_of_replicas = arguments[0].to_i
    timeout_in_milliseconds = arguments[1].to_i

    Timeout.timeout(timeout_in_milliseconds / 1000.0) do
      # Ask for ACKs everywhere
      @replication_streams.each do |replication_stream|
        Thread.new do
          replication_stream.refresh_offset!
        end
      end

      loop do
        confirmed, unconfirmed = @replication_streams.partition { |replication_stream| replication_stream.offset >= current_replication_offset }

        if confirmed.size >= expected_number_of_replicas
          puts "Received ACKs from #{confirmed.size} replicas, which is enough. Responding"
          client.write(RESPEncoder.encode(confirmed.size))
          return
        else
          puts "Received #{confirmed.size} ACKs, waiting for #{expected_number_of_replicas - confirmed.size} more"
          sleep 0.1
        end
      end
    end
  rescue Timeout::Error
    confirmed, unconfirmed = @replication_streams.partition { |replication_stream| replication_stream.offset >= current_replication_offset }
    puts "Timed out waiting for replicas, needed #{expected_number_of_replicas} but only found #{confirmed.size}"
    client.write(RESPEncoder.encode(confirmed.size))
  end

  def is_write_command?(command)
    case command.downcase
    when "replconf", "ping", "echo", "info", "wait"
      false
    else
      true
    end
  end
end

command_line_options = CommandLineOptionsParser.parse(ARGV)
RedisServer.new(command_line_options).start
