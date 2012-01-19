require 'erb'
require 'oca'

require 'one.rb'
require 'opennebulahelper.rb'

class ScalarisHelper < OpenNebulaHelper
  def get_master_description()
    description = get_description(SCALARISIMAGE, "true", "",
                                  "{mgmt_server, {{127,0,0,1},14195,mgmt_server}}.")
    #puts description
    description
  end

  def get_slave_description(ips, head_node)
    mgmt_server = "{mgmt_server, {{#{head_node.gsub(/\./, ',')}}, 14195, mgmt_server}}."
    description = get_description(SCALARISIMAGE, "false", ips, mgmt_server)
    #puts description
    description
  end

  def remove(num, instance)
    [false, "Not yet implemented"]
  end

  def get_node_info(instance, vmid)
    info = {}
    info[:ip] = get_ip(ENV['VMID'])
    info[:rpm_version] = get_scalaris_version()
    info[:vmid] = ENV['VMID']
    #info[:state] = get_local_state()
    begin
      node_info = get_scalaris_info("get_node_info")
      if node_info != nil
        info.merge!(node_info)
      end
      info[:state] = "RUNNING"
    rescue
      info[:state] = "ERROR"
    end
    info
  end

  def get_node_performance(instance, vmid)
    perf = {}
    perf[:vmid] = ENV['VMID']
    begin
      node_perf = get_scalaris_info("get_node_performance")
      if node_perf != nil
        perf.merge!(node_perf)
      end
      perf[:state] = "RUNNING"
    rescue
      perf[:state] = "ERROR"
    end
    perf
  end

  def get_service_info(instance)
    info = {}
    info[:rpm_version] = get_scalaris_version()
    begin
      service_info = get_scalaris_info("get_service_info")
      if service_info != nil
        info.merge!(service_info)
      end
      info[:state] = "RUNNING"
    rescue
      info[:state] = "ERROR"
    end
    info
  end

  def get_service_performance(instance)
    perf = {}
    begin
      service_perf = get_scalaris_info("get_service_performance")
      if service_perf != nil
        perf.merge!(service_perf)
      end
      perf[:state] = "RUNNING"
    rescue
      perf[:state] = "ERROR"
    end
    perf
  end

  private

  def get_scalaris_info(call)
    url = URI.parse("http://localhost:8000/jsonrpc.yaws")
    JSONRPC.json_call(url, call, [])["result"]["value"]
  end

  def get_scalaris_version()
    result = %x[rpm -q scalaris-svn --qf "%{VERSION}"]
    result.to_s
  end

  def get_description(image, scalarisfirst, ips, mgmt_server)
    @image = image
    @scalarisfirst = scalarisfirst
    @known_hosts = render_known_hosts(ips)
    @mgmt_server = mgmt_server
    erb = ERB.new(File.read("scalaris.one.vm.erb"))
    erb.result binding
  end

  def render_known_hosts(ips)
    nodes = ips.map {|ip|
      "{{#{ip.gsub(/\./, ',')}}, 14195, service_per_vm}"
    }.join(", ")
    "{known_hosts, [#{nodes}]}."
  end
end
