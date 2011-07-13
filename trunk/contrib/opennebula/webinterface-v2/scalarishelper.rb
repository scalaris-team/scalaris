require 'erb'
require 'oca'

require 'one.rb'
require 'opennebulahelper.rb'

include OpenNebula

class ScalarisHelper
  def create()
    description = get_description(SCALARISIMAGE, "true", "",
                                  "{mgmt_server, {{127,0,0,1},14195,mgmt_server}}.")
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)
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
    head_node = get_ip(instance.head_node)
    mgmt_server = "{mgmt_server, {{#{head_node.gsub(/\./, ',')}}, 14195, mgmt_server}}."
    description = get_description(SCALARISIMAGE, "false", ips, mgmt_server)
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)

      new_vm_id = response.to_i
      instance.add_scalarisvm(:one_vm_id => new_vm_id)

      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end

  def list(instance)
    ips = get_ips(instance)
    puts "x#{ips}x"
    ips
  end

  def destroy(instance)
    get_vms(id).each do |vm_id|
      OpenNebulaHelper.delete_vm vm_id
    end
    Scalaris[id].remove_all_scalarisvms
    Scalaris[id].destroy
  end

  def get_instance_info(instance)
    info = {}
    info["ips"] = get_ips(instance)
    info
  end

  private

  def get_description(image, scalarisfirst, ips, mgmt_server)
    @image = image
    @scalarisfirst = scalarisfirst
    @known_hosts = render_known_hosts(ips)
    @mgmt_server = mgmt_server
    erb = ERB.new(File.read("scalaris.one.vm.erb"))
    erb.result binding
  end

  def ip_to_erlang(ip)
  end

  def render_known_hosts(ips)
    nodes = ips.map {|ip|
      "{{#{ip.gsub(/\./, ',')}}, 14195, service_per_vm}"
    }.join(", ")
    "{known_hosts, [#{nodes}]}."
  end

  def get_ip(one_id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def get_vms(instance)
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

  def get_ips(instance)
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
