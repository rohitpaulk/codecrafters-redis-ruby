class ReplicationClient
  def initialize(server)
    @server = server
  end

  def start!
    return unless @server.replica_of

    connection = TCPSocket.new(master_host, master_port)
    connection.write(RESPEncoder.encode(["PING"]))
    response = RESPDecoder.decode(connection)

    if response.downcase != "pong"
      puts "Invalid PING response from master: #{response.inspect}"
      connection.write(RESPEncoder.encode_error_message("invalid PING response"))
    end
  end

  def master_host
    @server.replica_of[0]
  end

  def master_port
    @server.replica_of[1]
  end
end