module JSONRPC
  def self.get_scalaris_info(params)
    id = params[0].to_i
    info = ScalarisHelper.get_instance_info(id)
    info
  end

  def self.create_scalaris(params)
    user = params[0]
    res = ScalarisHelper.create(user)
    if res[0] == true
      {"scalaris_id" => res[1]}
    else
      raise "create failed with #{res[1]}"
    end
  end

  def self.add_vm_scalaris(params)
    id = params[0].to_i
    res = ScalarisHelper.add(id)
    if res[0] == true
      {"vm_id" => res[1]}
    else
      raise "create failed with #{res[1]}"
    end
  end

  def self.destroy_scalaris(params)
    id = params[0].to_i
    res = ScalarisHelper.destroy(id)
    "success"
  end

  def self.get_wiki_info(params)
    id = params[0].to_i
    info = WikiHelper.get_instance_info(id)
    info
  end

  def self.create_wiki(params)
    user = params[0]
    node = params[1]
    res = WikiHelper.create(user, node)
    if res[0] == true
      {"wiki_id" => res[1]}
    else
      raise "create failed with #{res[1]}"
    end
  end

  def self.destroy_wiki(params)
    id = params[0].to_i
    res = WikiHelper.destroy(id)
    "success"
  end

  def self.call(jsonreq)
    method = jsonreq["method"]

    begin
      res = self.send method, jsonreq["params"]
      "{ \"result\": #{res.to_json}, \"error\": null, \"id\": #{jsonreq['id']}}"
    rescue NoMethodError
      puts $!
      "{ \"error\": \"undefined method #{method}\", \"id\": #{jsonreq['id']}}"
    rescue
      puts $!
      "{ \"error\": #{$!.to_json}, \"id\": #{jsonreq['id']}}"
    end
  end
end
