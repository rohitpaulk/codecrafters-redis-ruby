class Database
  def initialize
    @keyspace = {}
  end

  def set(key, value)
    @keyspace[key] = {
      value: value,
      expiry: nil
    }
  end

  def set_with_expiry(key, value, expiry_in_milliseconds)
    @keyspace[key] = {
      value: value,
      expiry: Time.now + (expiry_in_milliseconds / 1000.0)
    }
  end

  def get(key)
    return nil unless @keyspace[key]

    if @keyspace[key][:expiry] && @keyspace[key][:expiry] < Time.now
      @keyspace.delete(key)
      return nil
    end

    @keyspace[key][:value]
  end
end
