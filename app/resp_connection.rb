class RESPConnection
  def initialize(socket)
    @socket = socket
  end

  def read
    RESPDecoder.decode(@socket)
  end

  def write(value)
    @socket.write(RESPEncoder.encode(value))
  end

  def write_error(message)
    @socket.write(RESPEncoder.encode_error_message(message))
  end

  def send_command(command, *arguments)
    write([command, *arguments])
    read
  end
end

