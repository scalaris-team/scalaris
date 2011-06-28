require 'erb'

require 'one.rb'

class ScalarisHelper
  def self.create(user)
    description = get_description(SCALARISIMAGE, "true", "", "{mgmt_server, {{127,0,0,1},14195,mgmt_server}}.")
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)

      vm_id = response.to_i
      instance = Scalaris.create(:user => user, :head_node => vm_id)
      instance.add_scalarisvm(:one_vm_id => vm_id)

      [true, instance.id]
    rescue Exception => e
      [false, e.message]
    end
  end

  def self.add(id)
    ips = get_ips(id)
    head_node = get_ip(Scalaris[id].head_node)
    mgmt_server = "{mgmt_server, {{#{head_node.gsub(/\./, ',')}}, 14195, mgmt_server}}."
    description = get_description(SCALARISIMAGE, "false", ips, mgmt_server)
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)

      instance = Scalaris[id]
      new_vm_id = response.to_i
      instance.add_scalarisvm(:one_vm_id => new_vm_id)

      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end

  def self.destroy(id)
    get_vms(id).each do |vm_id|
      OpenNebulaHelper.delete_vm vm_id
    end
    Scalaris[id].remove_all_scalarisvms
    Scalaris[id].destroy
  end

  def self.get_instance_info(id)
    info = {}
    info["ips"] = get_ips(id)
    info["id"] = id
    info
  end

  private

  def self.get_description(image, scalarisfirst, ips, mgmt_server)
    @image = image
    @scalarisfirst = scalarisfirst
    @known_hosts = render_known_hosts(ips)
    @mgmt_server = mgmt_server
    erb = ERB.new(File.read("scalaris.one.vm.erb"))
    erb.result binding
  end

  def self.ip_to_erlang(ip)
  end

  def self.render_known_hosts(ips)
    nodes = ips.map {|ip|
      "{{#{ip.gsub(/\./, ',')}}, 14195, service_per_vm}"
    }.join(", ")
    "{known_hosts, [#{nodes}]}."
  end

  def self.get_ip(one_id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def self.get_vms(id)
    instance = Scalaris[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vms = []
    if instance == nil
      raise "unknown scalaris instance"
    end
    instance.scalarisvms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        vms.push vm.id
      end
    end
    vms
  end

  def self.get_ips(id)
    instance = Scalaris[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    ips = []
    if instance == nil
      raise "unknown scalaris instance"
    end
    instance.scalarisvms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        ips.push Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
      end
    end
    ips
  end
end
