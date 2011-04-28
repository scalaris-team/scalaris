require 'one.rb'

class ScalarisHelper
  def self.create
    description = File.read("scalaris.one.vm")
    puts description

    client = Client.new(CREDENTIALS, ENDPOINT)
    begin
      response=client.call("vm.allocate", description)
      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end
end
