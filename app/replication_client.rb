class ReplicationClient
  def initialize(server)
    @server = server
  end

  def start!
    return unless @server.replica_of

    sleep 1 # CodeCrafters doesn't boot master in time always

    connection = RESPConnection.new(TCPSocket.new(master_host, master_port))

    response = connection.send_command("PING")

    if response.downcase != "pong"
      puts "Invalid PING response from master: #{response.inspect}"
    end

    # TODO: CodeCrafters doesn't accept uppercase REPLCONF
    response = connection.send_command("replconf", "listening-port", @server.port.to_s)

    if response.downcase != "ok"
      puts "Invalid REPLCONF response from master: #{response.inspect}"
    end

    # TODO: Handshake part 3?

    puts "Connected to master at #{master_host}:#{master_port}"
  end

  def master_host
    @server.replica_of[0]
  end

  def master_port
    @server.replica_of[1]
  end
end