class Values::Stream
  attr_accessor :entries

  def initialize
    @entries = []
  end

  def add_entry(entry)
    @entries << entry
  end

  def type
    "stream"
  end
end