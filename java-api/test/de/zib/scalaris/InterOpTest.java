/*
 *  Copyright 2011 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris;

import java.io.PrintStream;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionGroup;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

/**
 * Class to test interoperability between the different Scalaris APIs.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.1
 * @since 3.1
 */
public class InterOpTest {
    private enum Mode { READ, WRITE };

    /**
     * Queries the command line options for an action to perform.
     *
     * <pre>
     * <code>
     * > java -jar scalaris.jar -help
     * usage: scalaris [Options]
     *  -h,--help                        print this message
     *  -v,--verbose                     print verbose information, e.g. the
     *                                   properties read
     *  -lh,--localhost                  gets the local host's name as known to
     *                                   Java (for debugging purposes)
     *  -r,--read <basekey>              read all items with the given basekey
     *  -w,--write <basekey>             writes items of different types to the
     *                                   given basekey
     * </code>
     * </pre>
     *
     * In order to override node and cookie to use for a connection, specify
     * the <tt>scalaris.node</tt> or <tt>scalaris.cookie</tt> system properties.
     * Their values will be used instead of the values defined in the config
     * file!
     *
     * @param args
     *            command line arguments
     */
    public static void main(final String[] args) {
        boolean verbose = false;
        final CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        final Options options = getOptions();
        try {
            line = parser.parse(options, args);
        } catch (final ParseException e) {
            Main.printException("Parsing failed", e, false);
            return; // will not be reached since printException exits
        }

        if (line.hasOption("verbose")) {
            verbose = true;
            ConnectionFactory.getInstance().printProperties();
        }

        String basekey;
        Mode mode;
        if (line.hasOption("r")) { // read
            mode = Mode.READ;
            final String[] optionValues = line.getOptionValues("read");
            Main.checkArguments(optionValues, 2, options, "r");
            basekey = optionValues[0];
            final String language = optionValues[1];
            basekey += "_" + language;
            System.out.println("Java-API: reading from " + language);
        } else if (line.hasOption("w")) { // write
            mode = Mode.WRITE;
            basekey = line.getOptionValue("write");
            Main.checkArguments(basekey, options, "w");
            basekey += "_java";
            System.out.println("Java-API: writing values");
        } else if (line.hasOption("lh")) { // get local host name
            System.out.println(ConnectionFactory.getLocalhostName());
            return;
        } else {
            // print help if no other option was given
//        if (line.hasOption("help")) {
            final HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("scalaris [Options]", getOptions());
            return;
        }
        try {
            final TransactionSingleOp sc = new TransactionSingleOp();
            int failed = 0;

            failed += read_write_boolean(basekey, sc, mode);
            failed += read_write_integer(basekey, sc, mode);
            failed += read_write_long(basekey, sc, mode);
            failed += read_write_biginteger(basekey, sc, mode);
            failed += read_write_double(basekey, sc, mode);
            failed += read_write_string(basekey, sc, mode);
            failed += read_write_binary(basekey, sc, mode);
            failed += read_write_list(basekey, sc, mode);
            failed += read_write_map(basekey, sc, mode);
            System.out.println();

            sc.closeConnection();
            if (failed > 0) {
                if (mode == Mode.WRITE) {
                    System.out.println(failed + " number of writes failed.");
                } else {
                    System.out.println(failed + " number of reads failed.");
                }
                System.out.println();
                System.exit(1);
            }
        } catch (final ConnectionException e) {
            Main.printException("failed with connection error", e, verbose);
        } catch (final UnknownException e) {
            Main.printException("failed with unknown", e, verbose);
        }
    }

    private static int read_or_write(final TransactionSingleOp sc, final String key, final Object value, final Mode mode) {
        try {
            switch (mode) {
                case READ:
                    final ErlangValue actual = sc.read(key);
                    Object jresult = null;
                    if (value instanceof Boolean) {
                        jresult = actual.boolValue();
                    } else if (value instanceof Integer) {
                        jresult = actual.intValue();
                    } else if (value instanceof Long) {
                        jresult = actual.longValue();
                    } else if (value instanceof BigInteger) {
                        jresult = actual.bigIntValue();
                    } else if (value instanceof Double) {
                        jresult = actual.doubleValue();
                    } else if (value instanceof String) {
                        jresult = actual.stringValue();
                    } else if (value instanceof byte[]) {
                        jresult = actual.binaryValue();
                    } else if (value instanceof List<?>) {
                        jresult = actual.listValue();
                    } else if (value instanceof Map<?, ?>) {
                        jresult = actual.jsonValue();
                    } else {
                        jresult = actual.value();
                    }
                    PrintStream out;
                    int result;
                    String resultStr;
                    if (compare(jresult, value)) {
                        out = System.out;
                        result = 0;
                        resultStr = "ok";
                    } else {
                        out = System.err;
                        result = 1;
                        resultStr = "fail";
                    }

                    out.println("read(" + key + ")");
                    out.println("  expected: " + valueToStr(value));
                    out.println("  read raw: " + actual.value().toString());
                    out.println(" read java: " + valueToStr(jresult));
                    out.println(resultStr);
                    return result;
                case WRITE:
                    System.out.println("write(" + key + ", " + valueToStr(value) + ")");
                    sc.write(key, value);
                    return 0;
            }
        } catch (final ConnectionException e) {
            System.out.println("failed with connection error");
        } catch (final AbortException e) {
            System.out.println("failed with abort");
        } catch (final NotFoundException e) {
            System.out.println("failed with not_found");
        } catch (final UnknownException e) {
            System.out.println("failed with unknown");
            e.printStackTrace();
        } catch (final ClassCastException e) {
            System.out.println("failed with ClassCastException");
            e.printStackTrace();
        }
        return 1;
    }

    private static boolean compare(final byte[] actual, final Object expected) {
        try {
            final byte[] expected_bytes = (byte[]) expected;
            if (expected_bytes.length != actual.length) {
                return false;
            }
            for (int i = 0; i < expected_bytes.length; ++i) {
                if (expected_bytes[i] != actual[i]) {
                    return false;
                }
            }
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

    private static boolean compare(final List<Object> actual, final Object expected) {
        try {
            @SuppressWarnings("unchecked")
            final
            List<Object> expected_list = (List<Object>) expected;
            if (expected_list.size() != actual.size()) {
                return false;
            }
            for (int i = 0; i < expected_list.size(); ++i) {
                if (!compare(actual.get(i), expected_list.get(i))) {
                    return false;
                }
            }
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

    private static boolean compare(final Map<String, Object> actual, final Object expected) {
        try {
            @SuppressWarnings("unchecked")
            final
            Map<String, Object> expected_map = (Map<String, Object>) expected;
            if (expected_map.size() != actual.size()) {
                return false;
            }
            for (final Entry<String, Object> value : expected_map.entrySet()) {
                if (!compare(actual.get(value.getKey()), value.getValue())) {
                    return false;
                }
            }
            return true;
        } catch (final Exception e) {
            return false;
        }
    }

    private static boolean compare(final Object actual, final Object expected) {
        try {
            if (expected instanceof byte[]) {
                if (actual instanceof ErlangValue) {
                    return compare(((ErlangValue) actual).binaryValue(), expected);
                } else {
                    return compare((byte[]) actual, expected);
                }
            } else if (expected instanceof List<?>) {
                if (actual instanceof ErlangValue) {
                    return compare(((ErlangValue) actual).listValue(), expected);
                } else {
                    @SuppressWarnings("unchecked")
                    final
                    List<Object> actual_list = (List<Object>) actual;
                    return compare(actual_list, expected);
                }
            } else if (expected instanceof Map<?, ?>) {
                if (actual instanceof ErlangValue) {
                    return compare(((ErlangValue) actual).jsonValue(), expected);
                } else {
                    @SuppressWarnings("unchecked")
                    final
                    Map<String, Object> actual_map = (Map<String, Object>) actual;
                    return compare(actual_map, expected);
                }
            } else if (expected instanceof Boolean) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).boolValue());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof Integer) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).intValue());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof Long) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).longValue());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof BigInteger) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).bigIntValue());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof Double) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).doubleValue());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof String) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).stringValue());
                } else {
                    return expected.equals(actual);
                }
            } else {
                return false;
            }
        } catch (final Exception e) {
            return false;
        }
    }

    private static String valueToStr(final Object value) {
        final StringBuilder sb = new StringBuilder();
        valueToStr(value, sb);
        return sb.toString();
    }

    private static void valueToStr(final Object value, final StringBuilder sb) {
        if (value instanceof String) {
          sb.append('"');
//          sb.append(((String) value).replace("\n", "\\n"));
          sb.append(value);
          sb.append('"');
      } else if (value instanceof byte[]) {
          final byte[] bytes = ((byte[]) value);
          sb.append("<<");
          for (final byte b : bytes) {
              sb.append(b);
          }
          sb.append(">>");
      } else if (value instanceof Map<?, ?>) {
          @SuppressWarnings("unchecked")
          final
          Map<String, Object> map = ((Map<String, Object>) value);
          sb.append('{');
          for (final Map.Entry<String, Object> entry : map.entrySet()) {
              valueToStr(entry.getKey(), sb);
              sb.append('=');
              valueToStr(entry.getValue(), sb);
              sb.append(',');
          }
          // remove last ","
          if (map.size() > 0) {
              sb.deleteCharAt(sb.length() - 1);
          }
          sb.append('}');
      } else if (value instanceof List<?>) {
          @SuppressWarnings("unchecked")
          final
          List<Object> list = (List<Object>) value;
          sb.append('[');
          for (final Object object : list) {
              valueToStr(object, sb);
              sb.append(',');
          }
          // remove last ","
          if (list.size() > 0) {
              sb.deleteCharAt(sb.length() - 1);
          }
          sb.append(']');
      } else if (value instanceof ErlangValue) {
          sb.append(((ErlangValue) value).value().toString());
      } else {
          sb.append(value.toString());
      }
    }

    private static int read_write_boolean(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_bool_false"; value = false;
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_bool_true"; value = true;
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_integer(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_int_0"; value = 0;
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_int_1"; value = 1;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_int_min"; lastValue = Integer.MIN_VALUE;
        key = basekey + "_int_min"; value = -2147483648;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_int_max"; lastValue = Integer.MAX_VALUE;
        key = basekey + "_int_max"; value = 2147483647;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_int_max_div_2"; lastValue = Integer.MAX_VALUE / 2;
        key = basekey + "_int_max_div_2"; value = 2147483647 / 2;
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_long(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_long_0"; value = 0l;
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_long_1"; value = 1l;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_long_min"; lastValue = Long.MIN_VALUE;
        key = basekey + "_long_min"; value = -9223372036854775808l;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_long_max"; lastValue = Long.MAX_VALUE;
        key = basekey + "_long_max"; value = 9223372036854775807l;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_long_max_div_2"; lastValue = Long.MAX_VALUE / 2l;
        key = basekey + "_long_max_div_2"; value = 9223372036854775807l / 2l;
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_biginteger(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_bigint_0"; value = new BigInteger("0");
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_bigint_1"; value = new BigInteger("1");
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_bigint_min"; value = new BigInteger("-100000000000000000000");
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_bigint_max"; value = new BigInteger("100000000000000000000");
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_bigint_max_div_2"; value = new BigInteger("100000000000000000000").divide(new BigInteger("2"));
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_double(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_float_0.0"; value = 0.0;
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_float_1.5"; value = 1.5;
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_float_-1.5"; value = -1.5;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_float_min"; lastValue = Double.MIN_VALUE;
        key = basekey + "_float_min"; value = 4.9E-324;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_float_max"; lastValue = Double.MAX_VALUE;
        key = basekey + "_float_max"; value = 1.7976931348623157E308;
        failed += read_or_write(sc, key, value, mode);

//        lastKey = basekey + "_float_max_div_2"; lastValue = Double.MAX_VALUE / 2.0;
        key = basekey + "_float_max_div_2"; value = 1.7976931348623157E308 / 2.0;
        failed += read_or_write(sc, key, value, mode);

        // not supported by OTP:
//        key = basekey + "_float_neg_inf"; value = Double.NEGATIVE_INFINITY;
//        failed += read_or_write(sc, key, value, mode);
//        key = basekey + "_float_pos_inf"; value = Double.POSITIVE_INFINITY;
//        failed += read_or_write(sc, key, value, mode);
//        key = basekey + "_float_nan"; value = Double.NaN;
//        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_string(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_string_empty"; value = "";
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_string_foobar"; value = "foobar";
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_string_foo\\nbar"; value = "foo\nbar";
        failed += read_or_write(sc, key, value, mode);

        // some (arbitrary) unicode characters
        // (please don't be offended if they actually mean something)
        key = basekey + "_string_unicode"; value = "foo\u0180\u01E3\u11E5";
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_binary(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_byte_empty"; value = new byte[0];
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_byte_0"; value = new byte[] {0};
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_byte_0123"; value = new byte[] {0, 1, 2, 3};
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_list(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_list_empty"; value = new ArrayList<Object>();
        failed += read_or_write(sc, key, value, mode);

        final ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(0); list1.add(1); list1.add(2); list1.add(3);
        key = basekey + "_list_0_1_2_3"; value = list1;
        failed += read_or_write(sc, key, value, mode);

        final ArrayList<Integer> list2 = new ArrayList<Integer>();
        list2.add(0); list2.add(123); list2.add(456); list2.add(65000);
        key = basekey + "_list_0_123_456_65000"; value = list2;
        failed += read_or_write(sc, key, value, mode);

        final ArrayList<Integer> list3 = new ArrayList<Integer>();
        list3.add(0); list3.add(123); list3.add(456); list3.add(0x10ffff);
        key = basekey + "_list_0_123_456_0x10ffff"; value = list3;
        failed += read_or_write(sc, key, value, mode);

        final ArrayList<Object> list4 = new ArrayList<Object>();
        list4.add(0);
        list4.add("foo");
        list4.add(1.5);
        list4.add(false);
        key = basekey + "_list_0_foo_1.5_false"; value = list4;
        failed += read_or_write(sc, key, value, mode);

        // note: we do not support binaries in lists anymore because JSON would
        // need special handling for each list element which introduces too
        // much overhead
//        ArrayList<Object> list5 = new ArrayList<Object>();
//        list5.add(0);
//        list5.add("foo");
//        list5.add(1.5);
//        list5.add(new byte[] {0, 1, 2, 3});
//        key = basekey + "_list_0_foo_1.5_<<0123>>"; value = list5;
//        failed += read_or_write(sc, key, value, mode);

        final ArrayList<Object> list6 = new ArrayList<Object>();
        list6.add(0);
        list6.add("foo");
        list6.add(1.5);
        list6.add(false);
        list6.add(list4);
        key = basekey + "_list_0_foo_1.5_false_list4"; value = list6;
        failed += read_or_write(sc, key, value, mode);

        final LinkedHashMap<String, Integer> map1 = new LinkedHashMap<String, Integer>();
        map1.put("x", 0);
        map1.put("y", 1);
        final ArrayList<Object> list7 = new ArrayList<Object>();
        list7.add(0);
        list7.add("foo");
        list7.add(1.5);
        list7.add(false);
        list7.add(list4);
        list7.add(map1);
        key = basekey + "_list_0_foo_1.5_false_list4_map_x=0_y=1"; value = list7;
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    // Map/JSON:
    private static int read_write_map(final String basekey, final TransactionSingleOp sc, final Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_map_empty";
        value = new LinkedHashMap<String, Object>();
        failed += read_or_write(sc, key, value, mode);

        final LinkedHashMap<String, Integer> map1 = new LinkedHashMap<String, Integer>();
        map1.put("x", 0);
        map1.put("y", 1);
        key = basekey + "_map_x=0_y=1";
        value = map1;
        failed += read_or_write(sc, key, value, mode);

        final LinkedHashMap<String, Object> map2 = new LinkedHashMap<String, Object>();
        map2.put("a", 0);
        map2.put("b", "foo");
        map2.put("c", 1.5);
        map2.put("d", "foo\nbar");
        final ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(0);
        list1.add(1);
        list1.add(2);
        list1.add(3);
        map2.put("e", list1);
        map2.put("f", map1);
        key = basekey + "_map_a=0_b=foo_c=1.5_d=foo<nl>bar_e=list0123_f=mapx0y1";
        value = map2;
        failed += read_or_write(sc, key, value, mode);

        final LinkedHashMap<String, Integer> map3 = new LinkedHashMap<String, Integer>();
        map3.put("", 0);
        key = basekey + "_map_=0";
        value = map3;
        failed += read_or_write(sc, key, value, mode);

        final LinkedHashMap<String, Object> map4 = new LinkedHashMap<String, Object>();
        // some (arbitrary) unicode characters
        // (please don't be offended if they actually mean something)
        map4.put("x", 0);
        map4.put("y", "foo\u0180\u01E3\u11E5");
        key = basekey + "_map_x=0_y=foo\u0180\u01E3\u11E5";
        value = map4;
        failed += read_or_write(sc, key, value, mode);

        final LinkedHashMap<String, Integer> map5 = new LinkedHashMap<String, Integer>();
        // some (arbitrary) unicode characters
        // (please don't be offended if they actually mean something)
        map5.put("x", 0);
        map5.put("foo\u0180\u01E3\u11E5", 1);
        key = basekey + "_map_x=0_foo\u0180\u01E3\u11E5=1";
        value = map5;
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }



    /**
     * Creates the options the command line should understand.
     *
     * @return the options the program understands
     */
    private static Options getOptions() {
        final Options options = new Options();
        final OptionGroup group = new OptionGroup();

        /* Note: arguments are set to be optional since we implement argument
         * checks on our own (commons.cli is not flexible enough and only
         * checks for the existence of a first argument)
         */

        options.addOption(new Option("h", "help", false, "print this message"));

        options.addOption(new Option("v", "verbose", false, "print verbose information, e.g. the properties read"));

        final Option read = new Option("r", "read", true, "read an item");
        read.setArgName("basekey> <language");
        read.setArgs(2);
        read.setOptionalArg(true);
        group.addOption(read);

        final Option write = new Option("w", "write", true, "write an item");
        write.setArgName("basekey");
        write.setArgs(1);
        write.setOptionalArg(true);
        group.addOption(write);

        options.addOptionGroup(group);

        options.addOption(new Option("lh", "localhost", false, "gets the local host's name as known to Java (for debugging purposes)"));

        return options;
    }
}
