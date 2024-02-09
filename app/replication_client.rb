class ReplicationClient
  def initialize(server)
    @server = server
    @connection = nil
  end

  def start!
    return unless @server.replica_of

    puts "connecting to master at #{master_host}:#{master_port}..."

    @connection = begin
      RESPConnection.new(TCPSocket.new(master_host, master_port))
    rescue Errno::ECONNREFUSED
      puts "Master not available at #{master_host}:#{master_port}"
      return
    end

    puts "Connected to master."

    puts "Sending PING..."
    response = @connection.send_command("PING")
    puts "Sent PING"

    if response.downcase != "pong"
      puts "Invalid PING response from master: #{response.inspect}"
    end

    response = @connection.send_command("REPLCONF", "listening-port", @server.port.to_s)
    puts "Sent REPLCONF with capabilties"

    if response.downcase != "ok"
      puts "Invalid REPLCONF response from master: #{response.inspect}"
    end

    response = @connection.send_command("REPLCONF", "capa", "psync2")
    puts "Sent REPLCONF with capabilties"

    if response.downcase != "ok"
      puts "Invalid REPLCONF response from master: #{response.inspect}"
    end

    response = @connection.send_command(["PSYNC", "?", "-1"])
    puts "Sent PSYNC"

    if !response.downcase.start_with?("fullresync")
      puts "Invalid PSYNC response from master: #{response.inspect}"
      return
    end

    puts "Received fullresync from master (#{master_host}:#{master_port})"

    rdb_file_contents = @connection.read_rdb_file
    puts "Received RDB file (#{rdb_file_contents.bytesize} bytes) from master"
    puts "Connected to master at #{master_host}:#{master_port}"
  rescue RESPConnection::TimeoutError
    puts "Timed out waiting for response from master, ignoring replication client"
  end

  def each_command(&block)
    loop do
      command, *arguments = @connection.read
      case command.downcase
      when "replconf"
        if arguments[0].downcase != "getack"
          @connection.write_error("Unrecognized REPLCONF subcommand: #{arguments[0]}")
          continue
        end

        @connection.write(["REPLCONF",  "ACK", "0"])
      else
        yield command, arguments
      end
    rescue Errno::EPIPE, IncompleteRESP, Errno::ECONNRESET => e
      puts "Replication stream closed (#{e.class})"
      return
    end
  end

  def master_host
    @server.replica_of[0]
  end

  def master_port
    @server.replica_of[1]
  end
end