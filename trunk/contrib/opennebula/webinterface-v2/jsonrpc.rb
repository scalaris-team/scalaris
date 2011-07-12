module JSONRPC
  def self.get_scalaris_info(params, helper, instance)
    info = helper.get_instance_info(instance)
    info
  end

  def self.create_scalaris(params, helper, instance)
    user = params[0]
    res = ScalarisHelper.new.create()
    if res[0] == true
      {"scalaris_id" => res[1]}
    else
      raise "create_scalaris failed with #{res[1]}"
    end
  end

  def self.add_vm_scalaris(params, helper, instance)
    count = params[0].to_i
    res = helper.add(count, instance)
    if res[0] == true
      {"vm_id" => res[1]}
    else
      raise "add_vm_scalaris failed with #{res[1]}"
    end
  end

  def self.destroy_scalaris(params, helper, instance)
    url = params[0]
    res = ScalarisHelper.new.destroy(url)
    "success"
  end

  def self.call(jsonreq, helper, instance)
    method = jsonreq["method"]

    begin
      res = self.send(method, jsonreq["params"], helper, instance)
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
