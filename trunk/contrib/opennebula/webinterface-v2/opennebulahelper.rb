require 'oca'

include OpenNebula

class OpenNebulaHelper
  def create()
    description = get_master_description()
    begin
      response = create_vm(description)
      if OpenNebula.is_error?(response)
        [false, response.to_str]
      else
        vm_id = response.to_i
        url = "http://#{get_ip(vm_id)}:4567/jsonrpc"
        [true, url]
      end
    rescue Exception => e
      [false, e.message]
    end
  end

  def add(num, instance)
    ips = get_ips(instance)
    master_node = get_ip(instance.master_node)
    description = get_slave_description(ips, master_node)
    begin
      response = create_vm(description)

      new_vm_id = response.to_i
      instance.add_vm(:one_vm_id => new_vm_id)

      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end

  def destroy(instance)
    get_vms(instance).each do |vm_id|
      delete_vm vm_id
    end
    instance.remove_all_vms
    instance.destroy
    "success"
  end

  def list(instance)
    ids = get_vms(instance)
    {:peers => ids}
  end

  private

  def create_vm(description)
    client = Client.new(CREDENTIALS, ENDPOINT)
    client.call("vm.allocate", description)
  end

  def delete_vm(id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id == id.to_i}
    puts vm.class
    vm.finalize
  end

  def get_ip(one_id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    if vm == nil
      raise "unknown machine identifier"
    end
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def get_vms(instance)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vms = []
    if instance == nil
      raise "unknown service instance"
    end
    instance.vms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        vms.push vm.id.to_s
      end
    end
    vms
  end

  def get_ips(instance)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    ips = []
    if instance == nil
      raise "unknown service instance"
    end
    instance.vms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        ips.push Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
      end
    end
    ips
  end
end
