class IncompleteRESP < RuntimeError; end

class RESPDecoder
  class << self
    def decode(resp_io)
      first_char = resp_io.read(1)
      raise IncompleteRESP if first_char.nil?

      if first_char == "+"
        decode_simple_string(resp_io)
      elsif first_char == "$"
        decode_bulk_string(resp_io)
      elsif first_char == "*"
        decode_array(resp_io)
      else
        raise "Unhandled first_char: #{first_char}"
      end
    rescue EOFError
      raise IncompleteRESP
    end

    def decode_simple_string(resp_io)
      read = resp_io.readline("\r\n")
      if read[-2..] != "\r\n"
        raise IncompleteRESP
      end

      read[0..-3]
    end

    def decode_bulk_string(resp_io)
      byte_count = read_int_with_clrf(resp_io)
      str = resp_io.read(byte_count)

      # Exactly the advertised number of bytes must be present
      raise IncompleteRESP unless str && str.length == byte_count

      # Consume the ending CLRF
      raise IncompleteRESP unless resp_io.read(2) == "\r\n"

      str
    end

    def decode_array(resp_io)
      element_count = read_int_with_clrf(resp_io)

      # Recurse, using decode
      element_count.times.map { decode(resp_io) }
    end

    def read_int_with_clrf(resp_io)
      int_with_clrf = resp_io.readline("\r\n")

      if int_with_clrf[-2..] != "\r\n"
        raise IncompleteRESP
      end

      int_with_clrf.to_i
    end
  end
end
