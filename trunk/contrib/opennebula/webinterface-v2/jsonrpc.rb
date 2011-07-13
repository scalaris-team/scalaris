module JSONRPC
  def self.get_scalaris_info(params, helper, instance)
    info = helper.get_instance_info(instance)
    {:result => info}
  end

  def self.create_scalaris(params, helper, instance)
    user = params[0]
    res = ScalarisHelper.new.create()
    if res[0] == true
      {:result => res[1]}
    else
      {:error => "create_scalaris failed with #{res[1]}"}
    end
  end

  def self.add_nodes(params, helper, instance)
    count = params[0].to_i
    res = helper.add(count, instance)
    if res[0] == true
      {:result => res[1]}
    else
      {:error => "add_nodes failed with #{res[1]}"}
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

  def self.call(jsonreq, helper, instance)
    method = jsonreq["method"]

    res = {}
    begin
      res = self.send(method, jsonreq["params"], helper, instance)
    rescue NoMethodError
      puts $!
      res[:error] = "undefined method #{method}"
    rescue
      puts $!
      res[:error] = $!.to_json
    end
    res[:id] = jsonreq['id']
    res.to_json
  end

  def self.json_call(url, function, params)
    req = Net::HTTP::Post.new(url.path)
    req.add_field 'Content-Type', 'application/json'
    req.body =
      {
      :version => '1.1',
      :method => function,
      :params => params,
      :id => 0}.to_json
    res = Net::HTTP.start(url.host, url.port){|http|http.request(req)}
    JSON.parse(res.body)
  end
end
