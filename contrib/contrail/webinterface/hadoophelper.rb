require 'erb'

require 'one.rb'

class HadoopHelper
  def self.create(user)
    description = get_description(HADOOPIMAGE, "true", "")
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)

      vm_id = response.to_i
      instance = Hadoop.create(:user => user, :master_node => vm_id)
      instance.add_hadoopvm(:one_vm_id => vm_id)

      [true, instance.id]
    rescue Exception => e
      [false, e.message]
    end
  end

  def self.add(id)
    ips = get_ips(id)
    head_node = get_ip(Hadoop[id].master_node)
    description = get_description(HADOOPIMAGE, "false", head_node)
    puts description

    begin
      response = OpenNebulaHelper.create_vm(description)

      instance = Hadoop[id]
      new_vm_id = response.to_i
      instance.add_hadoopvm(:one_vm_id => new_vm_id)

      [true, response.to_i]
    rescue Exception => e
      [false, e.message]
    end
  end

  def self.destroy(id)
    get_vms(id).each do |vm_id|
      OpenNebulaHelper.delete_vm vm_id
    end
    Hadoop[id].remove_all_hadoopvms
    Hadoop[id].destroy
  end

  def self.get_instance_info(id)
    info = {}
    info["ips"] = get_ips(id)
    info["id"] = id
    info
  end

  private

  def self.get_description(image, first, master_ip)
    @image = image
    @hadoopfirst = first
    @hadoopmaster = master_ip
    erb = ERB.new(File.read("hadoop.one.vm.erb"))
    erb.result binding
  end

  def self.ip_to_erlang(ip)
  end

  def self.get_ip(one_id)
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vm = pool.find {|i| i.id.to_i == one_id.to_i}
    Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
  end

  def self.get_vms(id)
    instance = Hadoop[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    vms = []
    if instance == nil
      raise "unknown hadoop instance"
    end
    instance.hadoopvms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        vms.push vm.id
      end
    end
    vms
  end

  def self.get_ips(id)
    instance = Hadoop[id]
    client = Client.new(CREDENTIALS, ENDPOINT)
    pool = VirtualMachinePool.new(client, -1)
    pool.info
    ips = []
    if instance == nil
      raise "unknown hadoop instance"
    end
    instance.hadoopvms_dataset.each do |vm|
      vm = pool.find {|i| i.id == vm.one_vm_id.to_i}
      if vm != nil
        ips.push Nokogiri::XML(vm.to_xml).xpath("/VM/TEMPLATE/NIC/IP/text()").text
      end
    end
    ips
  end
end
