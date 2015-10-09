#!/usr/bin/ruby -KU
# Copyright 2008-2011 Zuse Institute Berlin
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

require 'rubygems'
begin
gem 'json', '>=1.4.1'
rescue LoadError
end
require 'json'
require 'optparse'
require 'pp'
begin
  require "#{File.expand_path(File.dirname(__FILE__))}/scalaris"
rescue LoadError => e
  raise unless e.message =~ /scalaris/
  require "scalaris"
end

def get_replication_factor()
  (Scalaris::RoutingTable.new).get_replication_factor
end

def write(sc, key_value_list)
  key, value = key_value_list
  sc.write(key, value)
end

def test_and_set(sc, key_value_list)
  key, old_value, new_value = key_value_list
  sc.test_and_set(key, old_value, new_value)
end

def add_on_nr(sc, key_value_list)
  begin
    key, to_add = key_value_list
    sc.add_on_nr(key, to_add)
  rescue Scalaris::NotANumberError
    $stderr.print "Error: " + $!.to_s.to_json + "\n"
    exit
  end
end

def add_del_on_list(sc, key_value_list)
  begin
    key, to_add, to_remove = key_value_list
    sc.add_del_on_list(key, to_add, to_remove)
  rescue Scalaris::NotAListError
    $stderr.print "Error: " + $!.to_s.to_json + "\n"
    exit
  end
end

options = {}

optparse = OptionParser.new do |opts|
  options[:help] = true

  options[:replication_factor] = nil
  opts.on('-f', '--get-replication-factor', 'get the replication factor' ) do |f|
    options[:factor] = f
    options[:help] = false
  end

  options[:read] = nil
  opts.on('-r', '--read KEY', 'read key KEY' ) do |key|
    options[:read] = key
    options[:help] = false
  end

  options[:write] = nil
  opts.on('-w', '--write KEY,VALUE', Array, 'write key KEY to VALUE' ) do |list|
    raise OptionParser::InvalidOption.new(list) unless list.size == 2
    options[:write] = list
    options[:help] = false
  end

  options[:test_and_set] = nil
  opts.on('--test-and-set KEY,OLDVALUE,NEWVALUE', Array, 'write key KEY to NEWVALUE if the current value is OLDVALUE' ) do |list|
    raise OptionParser::InvalidOption.new(list) unless list.size == 3
    options[:test_and_set] = list
    options[:help] = false
  end

  options[:add_del_on_list] = nil
  opts.on('--add-del-on-list KEY,TOADD,TOREMOVE', Array, 'add and remove elements from the value at key KEY' ) do |list|
    raise OptionParser::InvalidOption.new(list) unless list.size == 3
    options[:add_del_on_list] = list
    options[:help] = false
  end

  options[:add_on_nr] = nil
  opts.on('--add-on-nr KEY,VALUE', Array, 'add VALUE to the value at key KEY' ) do |list|
    raise OptionParser::InvalidOption.new(list) unless list.size == 2
    options[:add_on_nr] = list
    options[:help] = false
  end

  opts.on_tail("-h", "--help", "Show this message") do
    puts opts
    exit
  end
end

begin
  optparse.parse!
rescue OptionParser::ParseError
  $stderr.print "Error: " + $! + "\n"
  exit
end

sc = nil
communicating_options = [options[:read], options[:write], options[:test_and_set], 
                         options[:add_on_nr], options[:add_del_on_list]]

if communicating_options.compact != []
  sc = Scalaris::TransactionSingleOp.new
end

pp sc.read(options[:read]) unless options[:read] == nil
pp write(sc, options[:write]) unless options[:write] == nil
pp test_and_set(sc, options[:test_and_set]) unless options[:test_and_set] == nil
pp add_on_nr(sc, options[:add_on_nr]) unless options[:add_on_nr] == nil
pp add_del_on_list(sc, options[:add_del_on_list]) unless options[:add_del_on_list] == nil
pp get_replication_factor() unless options[:factor] == nil
puts optparse if options[:help]
