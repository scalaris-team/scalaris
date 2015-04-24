#!/usr/bin/ruby -KU
# Copyright 2011 Zuse Institute Berlin
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

begin
  require "#{File.expand_path(File.dirname(__FILE__))}/scalaris"
rescue LoadError => e
  raise unless e.message =~ /scalaris/
  require "scalaris"
end

def read_or_write(sc, key, value, mode, binary = false)
  begin
    if (mode == :read)
      actual = sc.read(key)
      # if the expected value is a list, the returned value could by (mistakenly) a string if it is a list of integers
      # -> convert such a string to a list
      if value.is_a?(Array)
        begin
            actual = Scalaris::str_to_list(actual)
        rescue Exception => e
            puts 'fail'
            return 1
        end
      end
      if (actual == value)
        out = $stdout
        result = 0
        result_str = 'ok'
      else
        out = $stderr
        result = 1
        result_str = 'fail'
      end

      out.puts 'read(' + key.to_s + ')'
      out.puts '  expected: ' + value.inspect
      out.puts '  read raw: ' + actual.inspect
      out.puts '   read rb: ' + actual.inspect
      out.puts result_str
      return result
    elsif (mode == :write)
      puts 'write(' + key.to_s + ', ' + value.to_s + ')'
      sc.write(key, value, binary)
      return 0
    end
  rescue Scalaris::ConnectionError => instance
    puts 'failed with connection error'
    return 1
  rescue Scalaris::TimeoutError => instance
    puts 'failed with timeout'
    return 1
  rescue Scalaris::AbortError => instance
    puts 'failed with abort'
    return 1
  rescue Scalaris::NotFoundError => instance
    puts 'failed with not_found'
    return 1
  rescue Scalaris::UnknownError => instance
    puts 'failed with unknown: ' + instance.inspect
    return 1
  rescue Exception => instance
    puts 'failed with ' + instance.inspect
    return 1
  end
end

def read_write_boolean(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_bool_false", false, mode)
  failed += read_or_write(sc, basekey + "_bool_true", true, mode)
  
  return failed
end

def read_write_integer(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_int_0", 0, mode)
  failed += read_or_write(sc, basekey + "_int_1", 1, mode)
  failed += read_or_write(sc, basekey + "_int_min", -2147483648, mode)
  failed += read_or_write(sc, basekey + "_int_max", 2147483647, mode)
  failed += read_or_write(sc, basekey + "_int_max_div_2", 2147483647 / 2, mode)
  
  return failed
end

def read_write_long(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_long_0", 0, mode)
  failed += read_or_write(sc, basekey + "_long_1", 1, mode)
  failed += read_or_write(sc, basekey + "_long_min", -9223372036854775808, mode)
  failed += read_or_write(sc, basekey + "_long_max", 9223372036854775807, mode)
  failed += read_or_write(sc, basekey + "_long_max_div_2", 9223372036854775807 / 2, mode)
  
  return failed
end
  
def read_write_biginteger(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_bigint_0", 0, mode)
  failed += read_or_write(sc, basekey + "_bigint_1", 1, mode)
  failed += read_or_write(sc, basekey + "_bigint_min", -100000000000000000000, mode)
  failed += read_or_write(sc, basekey + "_bigint_max", 100000000000000000000, mode)
  failed += read_or_write(sc, basekey + "_bigint_max_div_2", 100000000000000000000 / 2, mode)
  
  return failed
end

def read_write_double(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_float_0.0", 0.0, mode)
  failed += read_or_write(sc, basekey + "_float_1.5", 1.5, mode)
  failed += read_or_write(sc, basekey + "_float_-1.5", -1.5, mode)
  failed += read_or_write(sc, basekey + "_float_min", 4.9E-324, mode)
  failed += read_or_write(sc, basekey + "_float_max", 1.7976931348623157E308, mode)
  failed += read_or_write(sc, basekey + "_float_max_div_2", 1.7976931348623157E308 / 2.0, mode)
  
  # not supported by erlang:
  #failed += read_or_write(sc, basekey + "_float_neg_inf", float('-inf'), mode)
  #failed += read_or_write(sc, basekey + "__float_pos_inf", float('+inf'), mode)
  #failed += read_or_write(sc, basekey + "_float_nan", float('nan'), mode)
  
  return failed
end

def read_write_string(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_string_empty", '', mode)
  failed += read_or_write(sc, basekey + "_string_foobar", 'foobar', mode)
  failed += read_or_write(sc, basekey + "_string_foo\\nbar", "foo\nbar", mode)
  failed += read_or_write(sc, basekey + "_string_unicode", 'foo' + [0x0180, 0x01E3, 0x11E5].pack("U*"), mode)
  
  return failed
end

def read_write_binary(basekey, sc, mode)
  failed = 0
  
  # note: binary not supported by JSON
  failed += read_or_write(sc, basekey + "_byte_empty", [].pack("c*"), mode, true)
  failed += read_or_write(sc, basekey + "_byte_0", [0].pack("c*"), mode, true)
  failed += read_or_write(sc, basekey + "_byte_0123", [0, 1, 2, 3].pack("c*"), mode, true)
  
  return failed
end

def read_write_list(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_list_empty", [], mode)
  failed += read_or_write(sc, basekey + "_list_0_1_2_3", [0, 1, 2, 3], mode)
  failed += read_or_write(sc, basekey + "_list_0_123_456_65000", [0, 123, 456, 65000], mode)
  failed += read_or_write(sc, basekey + "_list_0_123_456_0x10ffff", [0, 123, 456, 0x10ffff], mode)
  list4 = [0, 'foo', 1.5, false]
  failed += read_or_write(sc, basekey + "_list_0_foo_1.5_false", list4, mode)
  # note: binary not supported in lists
  #failed += read_or_write(sc, basekey + "_list_0_foo_1.5_<<0123>>", [0, 'foo', 1.5, bytearray([0, 1, 2, 3])], mode)
  failed += read_or_write(sc, basekey + "_list_0_foo_1.5_false_list4", [0, 'foo', 1.5, false, list4], mode)
  failed += read_or_write(sc, basekey + "_list_0_foo_1.5_false_list4_map_x=0_y=1", [0, 'foo', 1.5, false, list4, {'x' => 0, 'y' => 1}], mode)
  
  return failed
end

def read_write_map(basekey, sc, mode)
  failed = 0
  
  failed += read_or_write(sc, basekey + "_map_empty", {}, mode)
  failed += read_or_write(sc, basekey + "_map_x=0_y=1", {'x' => 0, 'y' => 1}, mode)
  failed += read_or_write(sc, basekey + "_map_a=0_b=foo_c=1.5_d=foo<nl>bar_e=list0123_f=mapx0y1",
    {'a' => 0, 'b' => 'foo', 'c' => 1.5, 'd' => "foo\nbar", 'e' => [0, 1, 2, 3], 'f' => {'x' => 0, 'y' => 1}}, mode)
  failed += read_or_write(sc, basekey + "_map_=0", {'' => 0}, mode)
  failed += read_or_write(sc, basekey + '_map_x=0_y=foo' + [0x0180, 0x01E3, 0x11E5].pack("U*"),
    {'x' => 0, 'y' => 'foo' + [0x0180, 0x01E3, 0x11E5].pack("U*")}, mode)
  failed += read_or_write(sc, basekey + '_map_x=0_foo' + [0x0180, 0x01E3, 0x11E5].pack("U*") + '=1',
    {'x' => 0, 'foo' + [0x0180, 0x01E3, 0x11E5].pack("U*") => 1}, mode)
  
  return failed
end

if (ARGV[0] == "read")
  basekey = ARGV[1]
  language = ARGV[2]
  basekey += '_' + language
  mode = :read
  puts 'Ruby-JSON-API: reading from ' + language
elsif (ARGV[0] == "write")
  basekey = ARGV[1]
  basekey += '_json_ruby'
  mode = :write
  puts 'Ruby-JSON-API: writing values'
else
  puts 'unknown commands: ' + ARGV.to_s
  exit 1
end

sc = Scalaris::TransactionSingleOp.new

failed = 0
failed += read_write_boolean(basekey, sc, mode)
failed += read_write_integer(basekey, sc, mode)
failed += read_write_long(basekey, sc, mode)
failed += read_write_biginteger(basekey, sc, mode)
failed += read_write_double(basekey, sc, mode)
failed += read_write_string(basekey, sc, mode)
failed += read_write_binary(basekey, sc, mode)
failed += read_write_list(basekey, sc, mode)
failed += read_write_map(basekey, sc, mode)
puts ''

if (failed > 0)
  puts failed.to_s + ' number of ' + mode.to_s + 's failed.'
  puts ''
  exit 1
end
