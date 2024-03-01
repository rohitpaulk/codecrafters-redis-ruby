class Commands::Xadd < Commands::Base
  def run(arguments)
    stream_key = arguments[0]
    entry_id_argument = arguments[1]
    key_value_pairs = arguments[2..] # TODO: Use this

    database = server.database

    database.with_lock do
      stream = database.get(stream_key) || Values::Stream.new
      entry_id = entry_id_from_argument(stream, entry_id_argument)

      if entry_id.nil?
        client.write(RESPEncoder.encode_error_message("ERR Invalid ID specified in XADD"))
        return
      end

      if entry_id <= Values::Stream::EntryID.new(0, 0)
        client.write(RESPEncoder.encode_error_message("ERR The ID specified in XADD must be greater than 0-0"))
        return
      end

      if stream.sorted_entries.last && entry_id <= stream.sorted_entries.last.id
        client.write(RESPEncoder.encode_error_message("ERR The ID specified in XADD is equal or smaller than the target stream top item"))
        return
      end

      entry = Values::Stream::Entry.new(entry_id, key_value_pairs)
      stream.add_entry(entry)
      database.set(stream_key, stream)
      client.write(RESPEncoder.encode(entry.id.to_s))
    end
  end

  protected

  def entry_id_from_argument(stream, entry_id_argument)
    entry_time_part = entry_time_part_from_argument(entry_id_argument)
    return nil if entry_time_part.nil?

    entry_sequence_number_part = entry_sequence_number_part_from_argument(stream, entry_time_part, entry_id_argument)
    return nil if entry_sequence_number_part.nil?

    Values::Stream::EntryID.new(entry_time_part, entry_sequence_number_part)
  end

  def entry_time_part_from_argument(entry_id_argument)
    case entry_id_argument
    when /\d+-\d+/
      entry_id_argument.split("-").first.to_i
    when "*"
      Time.now.to_i * 1000
    when /\d+-\*/
      entry_id_argument.split("-").first.to_i
    end
  end

  def entry_sequence_number_part_from_argument(stream, time_part, entry_id_argument)
    case entry_id_argument
    when /\d+-\d+/
      entry_id_argument.split("-").last.to_i
    when "*", /\d+-\*/
      if stream.sorted_entries.last&.id&.time_part.eql?(time_part)
        stream.sorted_entries.last.id.sequence_number_part + 1
      else
        time_part.eql?(0) ? 1 : 0
      end
    end
  end
end
