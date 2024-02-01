require "base64"

class ReplicationServer
  def initialize(server, client_connection)
    @server = server
    @client_connection = client_connection
  end

  def start!
    resp_connection = RESPConnection.new(@client_connection)

    @client_connection.write("+FULLRESYNC #{@server.replication_id} #{@server.replication_offset}\r\n")

    empty_rdb_file_contents = Base64.decode64("UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==")
    puts empty_rdb_file_contents
    puts empty_rdb_file_contents.length
    resp_connection.write(empty_rdb_file_contents)

    sleep 600 # Not implemented the rest yet
  end
end