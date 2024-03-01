class Commands::Xrange < Commands::Base
  def run(arguments)
    stream_key = arguments[0]
    start_id = Values::Stream::EntryID.from_string(arguments[1])
    end_id = Values::Stream::EntryID.from_string(arguments[2])

    database = server.database

    database.with_lock do
      stream = database.get(stream_key)

      if stream.nil?
        client.write(RESPEncoder.encode([]))
        return
      end

      entries = stream.sorted_entries.select do |entry|
        entry.id >= start_id && entry.id <= end_id
      end

      response = entries.map do |entry|
        [
          entry.id.to_s,
          entry.values.flatten
        ]
      end

      client.write(RESPEncoder.encode(response))
    end
  end
end
