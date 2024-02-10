require "base64"

class ReplicationStream
  attr_reader :offset

  def initialize(server, client_socket)
    @server = server
    @connection = RESPConnection.new(client_socket)
    @offset = 0
  end

  def start!
    @connection.write_raw("+FULLRESYNC #{@server.replication_id} #{@server.replication_offset}\r\n")

    empty_rdb_file_contents = Base64.decode64("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

    # RDB files are sent in a format similar to bulk strings, but without the \r\n at the end
    @connection.write_raw("$#{empty_rdb_file_contents.bytesize}\r\n#{empty_rdb_file_contents}")
  end

  def propagate_command(command, arguments)
    puts "- Propagating #{command} #{arguments.join(" ")} to replica"
    @connection.write([command, *arguments])
  end

  def refresh_offset!
    puts "- Refreshing replication offset for replica"
    response = @connection.send_command_without_timeout("REPLCONF", "GETACK", "*")

    raise "Invalid response to REPLCONF GETACK: #{response}" unless response.is_a?(Integer)

    @offset = response
  rescue IncompleteRESP
    puts "- Incomplete response to REPLCONF GETACK, ignoring"
  end
end