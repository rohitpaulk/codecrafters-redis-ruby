class Database
  def initialize
    @data = {}
  end

  def set(key, value)
    @data[key] = {
      value: value,
      expiry: nil
    }
  end

  def set_with_expiry(key, value, expiry_in_milliseconds)
    @data[key] = {
      value: value,
      expiry: Time.now + (expiry_in_milliseconds / 1000.0)
    }
  end

  def get(key)
    return nil unless @data[key]

    if @data[key][:expiry] && @data[key][:expiry] < Time.now
      @data.delete(key)
      return nil
    end

    @data[key][:value]
  end
end