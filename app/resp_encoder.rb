class IncompleteRESP < RuntimeError; end

class RESPEncoder
  class << self
    def encode(value)
      case value
      when Array
        "*#{value.length}\r\n#{value.map { |v| encode(v) }.join}"
      when String
        "$#{value.length}\r\n#{value}\r\n"
      when nil
        "$-1\r\n"
      end
    rescue EOFError
      raise IncompleteRESP
    end

    def encode_error_message(value)
      "-#{value}\r\n"
    end
  end
end
