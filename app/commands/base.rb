class Commands::Base
  attr_accessor :client
  attr_accessor :server

  def initialize(client, server)
    @client = client
    @server = server
  end
end
