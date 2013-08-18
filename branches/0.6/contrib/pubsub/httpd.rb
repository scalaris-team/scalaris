#!/usr/bin/ruby
require 'webrick'

include WEBrick
# ---------------------------------------------
# Define a new class
class LogServlet < WEBrick::HTTPServlet::AbstractServlet
  def do_POST(request, response)
    puts request
    puts response
    puts "======================================="
    response['Content-Type'] = 'text/plain'
    response.status = 200
    response.body = Time.now.to_s + "\n"
  end
end
# ----------------------------------------------
# Create an HTTP server
s = HTTPServer.new(
  :Port            => 8080,
  :DocumentRoot    => "/tmp/"
)
s.mount("/log", LogServlet)
# When the server gets a control-C, kill it
trap("INT"){ s.shutdown }
# Start the server
s.start
