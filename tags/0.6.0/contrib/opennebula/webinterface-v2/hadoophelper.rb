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

  def list(instance)
    nodes = {}
    nodes[:masters] = [ENV["VMID"]] 
    nodes[:workers] = get_vms(instance)
    nodes
  end

  def get_node_info(instance, vmid)
    info = {}
    dn_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_datanode'")
    tt_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_tasktracker'")
    metrics_avail = system("test -f /tmp/mrmetrics.log");

    errorstring = (dn_running ? "" : "DataNode ") + (tt_running ? "" : "TaskTracker ")

    myip = get_ip(ENV['VMID'])
    if myip == ENV['HADOOP_MASTER']
      nn_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_namenode'")
      sn_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_secondarynamenode'")
      jt_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_jobtracker'")
      errorstring += (nn_running ? "" : "NameNode ") + (sn_running ? "" : "SecondaryNameNode ")
      errorstring += (jt_running ? "" : "JobTracker")
    else
      nn_running = true
      sn_running = true
      jt_running = true
    end
    if dn_running && tt_running && nn_running && sn_running && jt_running
      if ! metrics_avail
        info[:state] = "INIT"
      else
        info[:state] = "RUNNING"
      end
    else
      info[:state] = "ERROR"
      info[:error] = "Missing processes: " + errorstring
    end

    found = {}
    File.readlines("/tmp/mrmetrics.log").reverse_each do |line|
      if line.start_with?("mapred.tasktracker") || line.start_with?("mapred.shuffleOutput")
        category = line.split(':')[0]
        if ! found.key?(category)
          found[category] = true
          line.split(' ').each do |field|
            if field.match("=") 
              data = field.split('=')
              key = category, ".", data[0]
              value = data[1].sub(/,/, '')
              info[key] = value
            end
          end
          if found.size() == 2
            break
          end
        end
      end
    end
    info
  end

  def get_node_performance(instance, vmid)
    perf = {}
    perf
  end

  def get_service_info(instance)
    myip = get_ip(ENV['VMID'])
    if myip == ENV['HADOOP_MASTER']
      metrics_avail = system("test -f /tmp/mrmetrics.log");
      info = {}
      if metrics_avail
        File.readlines("/tmp/mrmetrics.log").reverse_each do |line|
          if line.start_with?("mapred.jobtracker") 
            line.split(' ').each do |field|
              if field.match("=") 
                data = field.split('=')
                info[data[0]] = data[1].sub(/,/, '') 
              end
            end
            break
          end
        end
        nn_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_namenode'")
        sn_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_secondarynamenode'")
        jt_running = system("ps ax | grep -v grep | grep -q 'java -Dproc_jobtracker'")
        errorstring = (nn_running ? "" : "NameNode ") + (sn_running ? "" : "SecondaryNameNode ")
        errorstring += (jt_running ? "" : "JobTracker")
        if nn_running && sn_running && jt_running
          info[:state] = "RUNNING"
        else
          info[:state] = "ERROR"
          info[:error] = "Missing processes: " + errorstring
        end
      else
        info[:state] = "INIT"
      end
      info
    else
      url = "http://" << ENV['HADOOP_MASTER'] << ":4567/jsonrpc"
      JSONRPC.json_call(URI.parse(url), "get_service_info", [])
    end
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
