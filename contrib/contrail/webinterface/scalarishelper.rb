require 'erb'

require 'one.rb'

class ScalarisHelper
  def self.create
    description = get_description(SCALARISIMAGE, "true", "", "{mgmt_server, {{127,0,0,1},14195,mgmt_server}}.")
    puts description

    client = Client.new(CREDENTIALS, ENDPOINT)
    begin
      response=client.call("vm.allocate", description)
      [true, response.to_i]
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

    client = Client.new(CREDENTIALS, ENDPOINT)
    begin
      response=client.call("vm.allocate", description)
      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end

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
    puts one_id
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def self.get_ips(id)
    instance = Scalaris[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    ips = []
    instance.vms_dataset.each do |vm|
      puts vm.one_vm_id
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        puts Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
        ips.push Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
      end
    end
    ips
  end
end
