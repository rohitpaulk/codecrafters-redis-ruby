class Values::Stream
  class EntryID
    include Comparable

    attr_accessor :time_part
    attr_accessor :sequence_number_part

    def initialize(time_part, sequence_number_part)
      @time_part = time_part
      @sequence_number_part = sequence_number_part
    end

    def self.from_string(string)
      time_part, sequence_number_part = string.split("-")
      # raise "Invalid EntryID string: #{string}" if time_part.nil? || sequence_number_part.nil?
      new(time_part.to_i, sequence_number_part.to_i)
    end

    def <=>(other)
      [time_part, sequence_number_part] <=> [other.time_part, other.sequence_number_part]
    end

    def ==(other)
      time_part.eql?(other.time_part) && sequence_number_part.eql?(other.sequence_number_part)
    end

    alias_method :eql?, :==

    def inspect
      "#<EntryID #{self}>"
    end

    def to_s
      "#{time_part}-#{sequence_number_part}"
    end
  end

  class Entry
    include Comparable

    attr_accessor :id
    attr_accessor :values

    def initialize(id, values)
      @id = id
      @values = values
    end

    def <=>(other)
      id <=> other.id
    end

    def ==(other)
      id == other.id
    end

    alias_method :eql?, :==
  end

  attr_accessor :sorted_entries

  def initialize
    @sorted_entries = []
  end

  def add_entry(entry)
    @sorted_entries << entry
  end

  def type
    "stream"
  end
end
