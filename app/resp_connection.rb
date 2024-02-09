require "timeout"

class RESPConnection
  class TimeoutError < StandardError; end

  def initialize(socket)
    @socket = socket
  end

  def read
    RESPDecoder.decode(@socket)
  end

  def read_rdb_file
    if @socket.read(1) != "$"
      raise "Expected RDB file to start with $"
    end

    length = RESPDecoder.read_int_with_clrf(@socket)
    @socket.read(length)
  end

  def write(value)
    @socket.write(RESPEncoder.encode(value))
  end

  def write_raw(bytes)
    @socket.write(bytes)
  end

  def write_error(message)
    @socket.write(RESPEncoder.encode_error_message(message))
  end

  def send_command(command, *arguments)
    write([command, *arguments])

    Timeout.timeout(0.5) do
      read
    end
  rescue Timeout::Error
    raise RESPConnection::TimeoutError, "Timed out waiting for response to #{command}"
  end
end

