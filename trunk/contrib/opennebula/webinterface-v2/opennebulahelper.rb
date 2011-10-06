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

  def get_ip(one_id)
    if settings.test?
      return settings.one_helper.get_ip(one_id)
    end

    pool = get_pool()
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    if vm == nil
      raise "unknown machine identifier"
    end
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def redirect_to_vm_failed_with(vmid, method, params, instance, ex)
    puts ex.class
    puts ex.message
    vm = instance.vms_dataset.first(:one_vm_id => vmid)
    result = {}
    result[:vmid] = vmid
    if vm == nil
      result[:state] = "UNKNOWN"
    else #vm has been started but is not reachable yet
      result[:state] = "INIT"
    end
    result
  end

  private

  def create_vm(description)
    if settings.test?
      return settings.one_helper.create_vm(description)
    end

    client = Client.new(CREDENTIALS, ENDPOINT)
    client.call("vm.allocate", description)
  end

  def delete_vm(id)
    if settings.test?
      return settings.one_helper.delete_vm(id)
    end

    pool = get_pool()
    vm = pool.find {|i| i.id == id.to_i}
    puts vm.class
    vm.finalize
  end

  def get_vms(instance)
    if settings.test?
      return settings.one_helper.get_vms(instance)
    end

    pool = get_pool()
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
    pool = get_pool()
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

  def get_pool()
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    pool
  end
end
