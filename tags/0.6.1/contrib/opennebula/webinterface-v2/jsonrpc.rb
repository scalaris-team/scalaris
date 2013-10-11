class JSONRPC
  def self.create_scalaris(params, helper, instance)
    user = params[0]
    res = ScalarisHelper.new.create()
    if res[0] == true
      {:result => res[1]}
    else
      {:error => "create_scalaris failed with #{res[1]}"}
    end
  end

  def self.destroy_scalaris(params, helper, instance)
    res = nil
    url = params[0]
    begin
      jsonres = json_call(URI.parse(url), "destroy", [])
      if jsonres['error'] == nil
        {:result => jsonres["result"]}
      else
        {:error => jsonres["error"]}
      end
    rescue
      {:error => $!.to_s}
    end
  end

  def self.create_hadoop(params, helper, instance)
    user = params[0]
    res = HadoopHelper.new.create()
    if res[0] == true
      {:result => res[1]}
    else
      {:error => "create_hadoop failed with #{res[1]}"}
    end
  end

  def self.destroy_hadoop(params, helper, instance)
    res = nil
    url = params[0]
    begin
      jsonres = json_call(URI.parse(url), "destroy", [])
      if jsonres['error'] == nil
        {:result => jsonres["result"]}
      else
        {:error => jsonres["error"]}
      end
    rescue
      {:error => $!.to_s}
    end
  end

  # called in manager
  def self.add_nodes(params, helper, instance)
    count = params[0].to_i
    res = helper.add(count, instance)
    if res[0] == true
      {:result => res[1]}
    else
      {:error => "add_nodes failed with #{res[1]}"}
    end
  end

  def self.remove_nodes(params, helper, instance)
    count = params[0].to_i
    res = helper.remove(count, instance)
    if res[0] == true
      {:result => res[1]}
    else
      {:error => "remove_nodes failed with #{res[1]}"}
    end
  end

  def self.list_nodes(params, helper, instance)
    {:result => helper.list(instance)}
  end

  def self.get_node_info(params, helper, instance)
    vmid = params[0]
    if(vmid == ENV["VMID"])
      {:result => helper.get_node_info(instance, vmid)}
    else
      begin
        {:result => self.redirect_call_to_vm(vmid, "get_node_info", params, helper)}
      rescue
        {:result => helper.redirect_to_vm_failed_with(vmid, "get_node_info", params, instance, $!)}
      end
    end
  end

  def self.get_node_performance(params, helper, instance)
    vmid = params[0]
    if(vmid == ENV["VMID"])
      {:result => helper.get_node_performance(instance, vmid)}
    else
      begin
        {:result => self.redirect_call_to_vm(vmid, "get_node_performance", params, helper)}
      rescue
        {:result => helper.redirect_to_vm_failed_with(vmid, "get_node_performance", params, instance, $!)}
      end
    end
 end

  def self.get_service_info(params, helper, instance)
    {:result => helper.get_service_info(instance)}
  end

  def self.get_service_performance(params, helper, instance)
    {:result => helper.get_service_performance(instance)}
  end

  # kinda private only called via the frontend
  def self.destroy(params, helper, instance)
    {:result => helper.destroy(instance)}
  end

  def self.call(jsonreq, helper, instance)
    method = jsonreq["method"]

    res = {}
    begin
      res = self.send(method, jsonreq["params"], helper, instance)
    rescue
      puts $!.class
      #puts $!.backtrace
      puts $!.message
      res[:error] = $!.to_json
    end
    res[:id] = jsonreq['id']
    res.to_json
  end

  def self.json_call(url, function, params)
    if Sinatra::Application::test?
      puts "mocked json_call"
      return settings.json_helper.json_call(url, function, params)
    end

    req = Net::HTTP::Post.new(url.path)
    req.add_field 'Content-Type', 'application/json'
    req.body =
      {
      :version => '1.1',
      :method => function,
      :params => params,
      :id => 0}.to_json
    #begin
      res = Net::HTTP.start(url.host, url.port){|http|http.request(req)}
      JSON.parse(res.body)
    #rescue
      #puts $!.class
      #puts "#{url}: #{$!}"
      #nil
    #end
  end

  def self.redirect_call_to_vm(vmid, function, params, helper)
    ip = helper.get_ip(vmid)
    url = "http://" << ip << ":4567/jsonrpc"
    self.json_call(URI.parse(url), function, params)["result"]
  end
end
