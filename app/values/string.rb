class Values::String
  attr_accessor :data

  def initialize(data)
    @data = data
  end

  def type
    "string"
  end
end