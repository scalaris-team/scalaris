#!/usr/bin/ruby

require 'erb'
require 'set'

class Run
  attr_reader :servers
  attr_reader :vms_per_server
  attr_reader :nodes_per_server
  attr_reader :clients_per_server
  attr_reader :iterations_per_server
  attr_reader :reads
  attr_reader :increments

  def initialize(servers, vms_per_server, nodes_per_server, clients_per_server,
                 iterations_per_server, reads, increments)
    @servers = servers
    @vms_per_server = vms_per_server
    @nodes_per_server = nodes_per_server
    @clients_per_server = clients_per_server
    @iterations_per_server = iterations_per_server
    @reads = reads
    @increments = increments
  end

  def to_gnuplot
    "#{servers} #{vms_per_server} #{nodes_per_server} #{clients_per_server} #{iterations_per_server} #{reads} #{increments}"
  end
end

class ServerGroup
  attr_reader :servers
  attr_reader :runs

  def initialize(servers)
    @servers = servers.to_i
    @runs = Set.new
  end

  def add(run)
    @runs.add(run)
  end

  def top_read(count)
    sorted_runs = @runs.sort {|x,y| y.reads <=> x.reads}
    sorted_runs[0, count]
  end

  def top_increment(count)
    sorted_runs = @runs.sort {|x,y| y.increments <=> x.increments}
    sorted_runs[0, count]
  end

  def <=>(other)
    @servers <=> other.servers
  end
end

if ARGV.length != 1
  puts "latex.rb <logfile>"
  exit
end

fn = ARGV[0]

servers = Set.new
runs = Set.new
IO.foreach(fn) {|line|
  elements = line.split(' ')
  server_count = elements[1].to_i 
  clients_per_server = elements[11].to_i
  iterations_per_server = elements[13].to_f * clients_per_server
  reads = elements[17].to_f
  increments = elements[15].to_f
  if server_count > 100
    puts "#{elements[1]} #{elements[11]}"
    puts line
  end
  servers.add(server_count)
  runs.add(Run.new(server_count, elements[5].to_i, elements[7].to_i, clients_per_server,
                   iterations_per_server, reads, increments))
}

servergroups = Hash.new
servers.each {|server_count|
  servergroups[server_count] = ServerGroup.new(server_count)
}

min_server = servers.min
max_server = servers.max

# create groups per server_count
f = File.new("all.data", "w+")
runs.each {|run|
  servergroups[run.servers].add(run)
  f.puts(run.to_gnuplot)
}
f.close

#top 1
r = File.new("top1read.data", "w+")
w = File.new("top1write.data", "w+")
servers.to_a.sort.each {|server_count|
  servergroups[server_count].top_read(1).each {|run|
    r.puts(run.to_gnuplot)
  }
  servergroups[server_count].top_increment(1).each {|run|
    w.puts(run.to_gnuplot)
  }
}
r.close
w.close

template = File.read('plotall.gnuplot.erb')
eruby = ERB.new(template)
f = File.new("plotall.gnuplot", "w+")
f.puts eruby.result(binding())
f.close

system "gnuplot plotall.gnuplot"


# create gnuplot files
servers.each {|server_count|
  f = File.new("#{server_count}.data", "w+")
  servergroups[server_count].runs.each {|run|
    f.puts(run.to_gnuplot)
  }
  f.close

  template = File.read('plot.gnuplot.erb')
  eruby = ERB.new(template)
  f = File.new("plot.gnuplot", "w+")
  f.puts eruby.result(binding())
  f.close

  system "gnuplot plot.gnuplot"
}

fn =~ %r{sum-test.run-(\d+)-\d+}
#gitinfo = `git log --pretty=format:'%h' -n 1`
gitinfo = $1
template = File.read('main.tex.erb')
eruby = ERB.new(template)
f = File.new("main.tex", "w+")
f.puts eruby.result(binding())
f.close

system "rm main.toc main.aux main.log"
system "pdflatex main.tex > texlog.txt"
system "pdflatex main.tex >> texlog.txt"
system "mv main.pdf #{fn + '.pdf'}"
puts "wrote results to #{fn + '.pdf'}"
