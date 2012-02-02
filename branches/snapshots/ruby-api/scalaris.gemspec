# coding: UTF-8

Gem::Specification.new do |s|
  s.name              = "scalaris"
  s.version           = "0.0.1"
  s.platform          = Gem::Platform::RUBY
  s.authors           = ["Nico Kruber, Florian Schintke, Thorsten Schütt"]
  s.email             = ["scalaris@googlegroups.com"]
  s.homepage          = "https://code.google.com/p/scalaris/"
  s.summary           = "Ruby bindings for Scalaris"
  s.description       = "Ruby bindings for Scalaris"

  s.required_rubygems_version = ">= 1.3.6"
  
  # If you have runtime dependencies, add them here
  s.add_runtime_dependency "json", ">= 1.4.0"
  
  # If you have development dependencies, add them here
  # s.add_development_dependency "another", "= 0.9"

  # The list of files to be contained in the gem
  s.files         = Dir["scalaris.rb", "scalarisclient.rb"]
  s.bindir        = '.'
  s.executables   = Dir["scalarisclient.rb"]
  # s.extensions    = Dir["ext/extconf.rb"]
  
  # s.require_path = 'lib'

  # For C extensions
  # s.extensions = "ext/extconf.rb"
end
