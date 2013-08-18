require 'rubygems'
require 'sinatra'
require 'test/unit'
require 'rack/test'
require 'mocha'

set :environment, :test

require 'frontend'

class FrontEndTest < Test::Unit::TestCase
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

  def test_undefined_method_works
    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("ttt", [])
    browser.post('/jsonrpc', body)
    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"], 0
    assert_equal "undefined method in ttt: undefined method `ttt' for JSONRPC:Class", res["error"]
  end

  def test_create_scalaris_works
    one_helper = mock()
    one_helper.expects(:create_vm).returns("43")
    one_helper.expects(:get_ip).with(43).returns("4.3.2.1")
    Sinatra::Application.set :one_helper, one_helper

    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("create_scalaris", [])
    browser.post('/jsonrpc', body)
    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal "http://4.3.2.1:4567/jsonrpc", res["result"]
  end

  def test_destroy_scalaris_works
    info = {}
    json_helper = mock()
    JSONRPC.expects(:json_call).with(URI.parse("http://1.2.3.5:4567/jsonrpc"), "destroy", []).returns(info)

    Sinatra::Application.set :json_helper, json_helper

    browser = Rack::Test::Session.new(Rack::MockSession.new(Sinatra::Application))
    body = json_call("destroy_scalaris", ["http://1.2.3.5:4567/jsonrpc"])
    browser.post('/jsonrpc', body)
    res = JSON.parse(browser.last_response.body)
    assert_equal 0, res["id"]
    assert_equal nil, res["result"]
  end
end
