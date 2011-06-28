module OpenNebulaHelper
  def self.create_vm(description)
    client = Client.new(CREDENTIALS, ENDPOINT)
    client.call("vm.allocate", description)
  end

  def self.delete_vm(id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id == id.to_i}
    puts vm.class
    vm.finalize
  end
end
