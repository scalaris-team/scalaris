require 'rubygems'
require 'sinatra'
require 'test/unit'
require 'rack/test'
require 'mocha'

ENV['VMID'] = '42'
set :environment, :test

require 'manager'

class ManagerTest < Test::Unit::TestCase
  def json_call(function, params)
    {
      :version => '1.1',
      :method => function,
      :params => params,
      :id => 0}.to_json
  end

  def test_index_works
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    browser.get '/'
    assert browser.last_response.ok?
  end

  def test_get_jsonrpc_works
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    browser.get('/jsonrpc')
    assert browser.last_response.redirection?
  end

  def test_undefined_method_works
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("ttt", [])
    browser.post('/jsonrpc', body)
    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"], 0
    assert_equal "\"undefined method `ttt' for JSONRPC:Class\"", res["error"]
  end

  def test_list_nodes_works
    one_helper = mock()
    one_helper.expects(:get_vms).returns(["42"])
    Sinatra::Application.set :one_helper, one_helper

    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("list_nodes", [])
    browser.post('/jsonrpc', body)
    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal ["42"], res["result"]["peers"]
  end

  def test_get_node_info_works
    info = {"result" => {"value" => {}}}
    json_helper = mock()
    json_helper.expects(:json_call).with(URI.parse("http://localhost:8000/jsonrpc.yaws"), "get_node_info", []).returns(info)
    one_helper = mock()
    one_helper.expects(:get_ip).with("42").returns("1.2.3.4")
    Sinatra::Application.set :json_helper, json_helper
    Sinatra::Application.set :one_helper, one_helper
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("get_node_info", ['42'])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal "42", res["result"]["vmid"]
    assert_equal "1.2.3.4", res["result"]["ip"]
  end

  def test_get_service_info_works
    info = {"result" => {"value" => {}}}
    json_helper = mock()
    json_helper.expects(:json_call).with(URI.parse("http://localhost:8000/jsonrpc.yaws"), "get_service_info", []).returns(info)
    Sinatra::Application.set :json_helper, json_helper
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("get_service_info", [])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
  end

  def test_get_node_performance_works
    info = {"result" => {"value" => {}}}
    json_helper = mock()
    json_helper.expects(:json_call).with(URI.parse("http://localhost:8000/jsonrpc.yaws"), "get_node_performance", []).returns(info)
    Sinatra::Application.set :json_helper, json_helper
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("get_node_performance", ['42'])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal "42", res["result"]["vmid"]
  end

  def test_get_service_performance_works
    info = {"result" => {"value" => {}}}
    json_helper = mock()
    json_helper.expects(:json_call).with(URI.parse("http://localhost:8000/jsonrpc.yaws"), "get_service_performance", []).returns(info)
    Sinatra::Application.set :json_helper, json_helper
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("get_service_performance", [])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal nil, res["result"]["vmid"]
  end

  def test_destroy_works
    one_helper = mock()
    one_helper.expects(:get_vms).returns(["43"])
    one_helper.expects(:delete_vm).returns(["43"])
    Sinatra::Application.set :one_helper, one_helper

    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("destroy", [])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal nil, res["result"]["vmid"]
  end

  def test_redirect_unknown_works
    info = {"result" => {"value" => {}}}
    json_helper = mock()
    json_helper.expects(:json_call).with(URI.parse("http://1.2.3.4:4567/jsonrpc"), "get_node_info", ["43"]).raises(NoMethodError, 'message')
    Sinatra::Application.set :json_helper, json_helper

    one_helper = mock()
    one_helper.expects(:get_ip).with("43").returns("1.2.3.4")
    Sinatra::Application.set :one_helper, one_helper

    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("get_node_info", ["43"])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal "43", res["result"]["vmid"]
    assert_equal "UNKNOWN", res["result"]["state"]
  end

  def test_get_node_info_works
    json_helper = mock()
    json_helper.expects(:json_call).with(URI.parse("http://localhost:8000/jsonrpc.yaws"), "get_node_info", []).raises(NoMethodError, 'message')
    one_helper = mock()
    one_helper.expects(:get_ip).with("42").returns("1.2.3.4")
    Sinatra::Application.set :json_helper, json_helper
    Sinatra::Application.set :one_helper, one_helper
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("get_node_info", ['42'])
    browser.post('/jsonrpc', body)

    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal "42", res["result"]["vmid"]
    assert_equal "1.2.3.4", res["result"]["ip"]
  end


end
