# Copyright 2011-2015 Zuse Institute Berlin
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

import scalaris
import sys

def read_or_write(sc, key, value, mode):
    try:
        if (mode == 'read'):
            actual = sc.read(key)
            # if the expected value is a list, the returned value could by (mistakenly) a string if it is a list of integers
            # -> convert such a string to a list
            if (type(value).__name__=='list'):
                try:
                    actual = scalaris.str_to_list(actual)
                except:
                    pass # will fail the comparison anyway
            if (actual == value):
                out = sys.stdout
                result = 0
                result_str = 'ok'
            else:
                out = sys.stderr
                result = 1
                result_str = 'fail'

            print >> out, 'read(' + key + ')'
            print >> out, '  expected: ' + repr(value)
            print >> out, '  read raw: ' + repr(actual)
            print >> out, '   read py: ' + repr(actual)
            print >> out, result_str
            return result
        elif (mode == 'write'):
            print 'write(' + key + ', ' + repr(value) + ')'
            sc.write(key, value)
            return 0
    except scalaris.ConnectionError as instance:
        print 'failed with connection error'
        return 1
    except scalaris.TimeoutError as instance:
        print 'failed with timeout'
        return 1
    except scalaris.AbortError as instance:
        print 'failed with abort'
        return 1
    except scalaris.NotFoundError as instance:
        print 'failed with not_found'
        return 1
    except scalaris.UnknownError as instance:
        print 'failed with unknown: ' + str(instance)
        return 1
    except Exception as instance:
        print 'failed with ' + str(instance)
        return 1

def read_write_boolean(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_bool_false", False, mode)
    failed += read_or_write(sc, basekey + "_bool_true", True, mode)
    
    return failed

def read_write_integer(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_int_0", 0, mode)
    failed += read_or_write(sc, basekey + "_int_1", 1, mode)
    failed += read_or_write(sc, basekey + "_int_min", -2147483648, mode)
    failed += read_or_write(sc, basekey + "_int_max", 2147483647, mode)
    failed += read_or_write(sc, basekey + "_int_max_div_2", 2147483647 // 2, mode)
    
    return failed

def read_write_long(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_long_0", 0l, mode)
    failed += read_or_write(sc, basekey + "_long_1", 1l, mode)
    failed += read_or_write(sc, basekey + "_long_min", -9223372036854775808l, mode)
    failed += read_or_write(sc, basekey + "_long_max", 9223372036854775807l, mode)
    failed += read_or_write(sc, basekey + "_long_max_div_2", 9223372036854775807l // 2l, mode)
    
    return failed

def read_write_biginteger(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_bigint_0", 0, mode)
    failed += read_or_write(sc, basekey + "_bigint_1", 1, mode)
    failed += read_or_write(sc, basekey + "_bigint_min", -100000000000000000000, mode)
    failed += read_or_write(sc, basekey + "_bigint_max", 100000000000000000000, mode)
    failed += read_or_write(sc, basekey + "_bigint_max_div_2", 100000000000000000000 // 2, mode)
    
    return failed

def read_write_double(basekey, sc, mode):
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

def read_write_string(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_string_empty", '', mode)
    failed += read_or_write(sc, basekey + "_string_foobar", 'foobar', mode)
    failed += read_or_write(sc, basekey + "_string_foo\\nbar", 'foo\nbar', mode)
    failed += read_or_write(sc, basekey + "_string_unicode", u'foo\u0180\u01E3\u11E5', mode)
    
    return failed

def read_write_binary(basekey, sc, mode):
    failed = 0
    
    # note: binary not supported by JSON
    failed += read_or_write(sc, basekey + "_byte_empty", bytearray(), mode)
    failed += read_or_write(sc, basekey + "_byte_0", bytearray([0]), mode)
    failed += read_or_write(sc, basekey + "_byte_0123", bytearray([0, 1, 2, 3]), mode)
    
    return failed

def read_write_list(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_list_empty", [], mode)
    failed += read_or_write(sc, basekey + "_list_0_1_2_3", [0, 1, 2, 3], mode)
    failed += read_or_write(sc, basekey + "_list_0_123_456_65000", [0, 123, 456, 65000], mode)
    failed += read_or_write(sc, basekey + "_list_0_123_456_0x10ffff", [0, 123, 456, 0x10ffff], mode)
    list4 = [0, 'foo', 1.5, False]
    failed += read_or_write(sc, basekey + "_list_0_foo_1.5_false", list4, mode)
    # note: binary not supported in lists
    #failed += read_or_write(sc, basekey + "_list_0_foo_1.5_<<0123>>", [0, 'foo', 1.5, bytearray([0, 1, 2, 3])], mode)
    failed += read_or_write(sc, basekey + "_list_0_foo_1.5_false_list4", [0, 'foo', 1.5, False, list4], mode)
    failed += read_or_write(sc, basekey + "_list_0_foo_1.5_false_list4_map_x=0_y=1", [0, 'foo', 1.5, False, list4, {'x': 0, 'y': 1}], mode)
    
    return failed

def read_write_map(basekey, sc, mode):
    failed = 0
    
    failed += read_or_write(sc, basekey + "_map_empty", {}, mode)
    failed += read_or_write(sc, basekey + "_map_x=0_y=1", {'x': 0, 'y': 1}, mode)
    failed += read_or_write(sc, basekey + "_map_a=0_b=foo_c=1.5_d=foo<nl>bar_e=list0123_f=mapx0y1",
                            {'a': 0, 'b': 'foo', 'c': 1.5, 'd': 'foo\nbar', 'e': [0, 1, 2, 3], 'f': {'x': 0, 'y': 1}}, mode)
    failed += read_or_write(sc, basekey + "_map_=0", {'': 0}, mode)
    failed += read_or_write(sc, basekey + u"_map_x=0_y=foo\u0180\u01E3\u11E5", {'x': 0, 'y': u'foo\u0180\u01E3\u11E5'}, mode)
    failed += read_or_write(sc, basekey + u"_map_x=0_foo\u0180\u01E3\u11E5=1", {'x': 0, u'foo\u0180\u01E3\u11E5': 1}, mode)
    
    return failed

if __name__ == "__main__":
    if (sys.argv[1] == "read"):
        basekey = sys.argv[2]
        language = sys.argv[3]
        basekey += '_' + language
        mode = 'read'
        print 'Python-JSON-API: reading from ' + language
    elif (sys.argv[1] == "write"):
        basekey = sys.argv[2]
        basekey += '_json_python'
        mode = 'write'
        print 'Python-JSON-API: writing values'
    else:
        print 'unknown commands: ' + str(sys.argv)
        sys.exit(1)
    
    sc = scalaris.TransactionSingleOp()
    
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
    print ''
    
    if (failed > 0):
        print str(failed) + ' number of ' + mode + 's failed.'
        print ''
        sys.exit(1)
