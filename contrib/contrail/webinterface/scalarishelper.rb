require 'erb'

require 'one.rb'

class ScalarisHelper
  def self.create
    description = get_description(SCALARISIMAGE, "true")
    puts description

    client = Client.new(CREDENTIALS, ENDPOINT)
    begin
      response=client.call("vm.allocate", description)
      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end

  def self.get_description(image=SCALARISIMAGE, scalarisfirst="true")
    @image = image
    @scalarisfirst = scalarisfirst
    erb = ERB.new(File.read("scalaris.one.vm.erb"))
    erb.result binding
  end
end
