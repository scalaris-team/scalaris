require 'erb'

require 'one.rb'

class HadoopHelper < OpenNebulaHelper
  def get_master_description()
    description = get_description(HADOOPIMAGE, "true", "$NIC[IP]")
    puts description
    description
  end

  def get_slave_description(ips, head_node)
    description = get_description(HADOOPIMAGE, "false", head_node)
    puts description
    description
  end

  def remove(num, instance)
    [false, "Not yet implemented"]
  end

  def get_node_info(instance, vmid)
    info = {}
    info
  end

  def get_node_performance(instance, vmid)
    perf = {}
    perf
  end

  def get_service_info(instance)
    info = {}
    info
  end

  def get_service_performance(instance)
    perf = {}
    perf
  end

  private

  def get_description(image, first, master_ip)
    @image = image
    @hadoopfirst = first
    @hadoopmaster = master_ip
    erb = ERB.new(File.read("hadoop.one.vm.erb"))
    erb.result binding
  end
end
