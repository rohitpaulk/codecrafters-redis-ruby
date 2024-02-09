require "base64"

class ReplicationStream
  def initialize(server, client_connection)
    @server = server
    @client_connection = client_connection
  end

  def start!
    resp_connection = RESPConnection.new(@client_connection)

    @client_connection.write("+FULLRESYNC #{@server.replication_id} #{@server.replication_offset}\r\n")

    empty_rdb_file_contents = Base64.decode64("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")

    # RDB files are sent in a format similar to bulk strings, but without the \r\n at the end
    resp_connection.write_raw("$#{empty_rdb_file_contents.bytesize}\r\n#{empty_rdb_file_contents}")
  end

  def propagate_command(command, arguments)
    puts "- Propagating #{command} #{arguments.join(" ")} to replica"
    @client_connection.write(RESPEncoder.encode([command, *arguments]))
  end
end