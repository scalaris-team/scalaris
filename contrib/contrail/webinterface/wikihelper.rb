require 'erb'

require 'one.rb'

class WikiHelper
  def self.create(user, node)
    description = get_description(WIKIIMAGE, node)
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)

      vm_id = response.to_i
      instance = Wiki.create(:user => user, :master_node => vm_id)
      instance.add_wikivm(:one_vm_id => vm_id)

      [true, instance.id]
    rescue Exception => e
      [false, e.message]
    end
  end

  def self.destroy(id)
    get_vms(id).each do |vm_id|
      OpenNebulaHelper.delete_vm vm_id
    end
    Wiki[id].remove_all_wikivms
    Wiki[id].destroy
  end

  def self.get_instance_info(id)
    info = {}
    info["ips"] = get_ips(id)
    info["id"] = id
    info
  end

  private

  def self.get_description(image, node)
    @image = image
    @node = node
    erb = ERB.new(File.read("scalaris-wiki.one.vm.erb"))
    erb.result binding
  end

  def self.get_ip(one_id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def self.get_vms(id)
    instance = Wiki[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vms = []
    if instance == nil
      raise "unknown wiki instance"
    end
    instance.wikivms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        vms.push vm.id
      end
    end
    vms
  end

  def self.get_ips(id)
    instance = Wiki[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    ips = []
    if instance == nil
      raise "unknown wiki instance"
    end
    instance.wikivms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        ips.push Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
      end
    end
    ips
  end
end
