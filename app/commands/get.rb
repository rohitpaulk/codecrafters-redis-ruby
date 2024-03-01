class Commands::Get < Commands::Base
  def run(arguments)
    value = server.database.get(arguments[0])

    if value.is_a?(Values::String)
      client.write(RESPEncoder.encode(value.data))
    elsif value.nil?
      client.write(RESPEncoder.encode(nil))
    else
      client.write(RESPEncoder.encode_error_message("WRONGTYPE Operation against a key holding the wrong kind of value"))
    end
  end
end
