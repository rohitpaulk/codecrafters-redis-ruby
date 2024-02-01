class CommandLineOptionsParser
  def self.parse(command_line_arguments)
    options_map = {}
    current_option = nil

    command_line_arguments.each do |argument|
      if argument.start_with?("--")
        argument[2..]
        current_option = argument[2..]
      elsif current_option.nil?
        raise "Expected argument #{argument} must start with --"
      elsif options_map[current_option]&.is_a?(Array)
        options_map[current_option] << argument
      elsif options_map[current_option]
        options_map[current_option] = [options_map[current_option], argument]
      else
        options_map[current_option] = argument
      end
    end

    options_map
  end
end
