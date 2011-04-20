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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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
     * the <tt>SCALARIS_JAPI_NODE</tt> or <tt>SCALARIS_JAPI_COOKIE</tt>
     * environment variables. Their values will be used instead of the values
     * defined in the config file!
     *
     * @param args
     *            command line arguments
     */
    public static void main(String[] args) {
        boolean verbose = false;
        CommandLineParser parser = new GnuParser();
        CommandLine line = null;
        Options options = getOptions();
        try {
            line = parser.parse(options, args);
        } catch (ParseException e) {
            Main.printException("Parsing failed", e, false);
        }

        if (line.hasOption("verbose")) {
            verbose = true;
            ConnectionFactory.getInstance().printProperties();
        }

        String basekey;
        Mode mode;
        if (line.hasOption("r")) { // read
            mode = Mode.READ;
            String[] optionValues = line.getOptionValues("read");
            Main.checkArguments(optionValues, 2, options, "r");
            basekey = optionValues[0];
            String language = optionValues[1];
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
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("scalaris [Options]", getOptions());
            return;
        }
        try {
            TransactionSingleOp sc = new TransactionSingleOp();
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
        } catch (ConnectionException e) {
            Main.printException("failed with connection error", e, verbose);
        } catch (UnknownException e) {
            Main.printException("failed with unknown", e, verbose);
        }
    }

    private static int read_or_write(TransactionSingleOp sc, String key, Object value, Mode mode) {
        try {
            switch (mode) {
                case READ:
                    System.out.println("read(" + key + ")");
                    ErlangValue result = sc.read(key);
                    System.out.println("  expected: " + valueToStr(value));
                    System.out.println("  read raw: " + result.value().toString());

                    Object jresult = null;
                    if (value instanceof Boolean) {
                        jresult = result.boolValue();
                    } else if (value instanceof Integer) {
                        jresult = result.intValue();
                    } else if (value instanceof Long) {
                        jresult = result.longValue();
                    } else if (value instanceof BigInteger) {
                        jresult = result.bigIntValue();
                    } else if (value instanceof Double) {
                        jresult = result.doubleValue();
                    } else if (value instanceof String) {
                        jresult = result.stringValue();
                    } else if (value instanceof byte[]) {
                        jresult = result.binaryValue();
                    } else if (value instanceof List<?>) {
                        jresult = result.listValue();
                    } else if (value instanceof Map<?, ?>) {
                        jresult = result.jsonValue();
                    }

                    System.out.println(" read java: " + valueToStr(jresult));
                    if (compare(jresult, value)) {
                        System.out.println("ok");
                        return 0;
                    } else {
                        System.out.println("fail");
                        return 1;
                    }
                case WRITE:
                    System.out.println("write(" + key + ", " + valueToStr(value) + ")");
                    sc.write(key, value);
                    return 0;
            }
        } catch (ConnectionException e) {
            System.out.println("failed with connection error");
        } catch (TimeoutException e) {
            System.out.println("failed with timeout");
        } catch (AbortException e) {
            System.out.println("failed with abort");
        } catch (NotFoundException e) {
            System.out.println("failed with not_found");
        } catch (UnknownException e) {
            System.out.println("failed with unknown");
            e.printStackTrace();
        } catch (ClassCastException e) {
            System.out.println("failed with ClassCastException");
            e.printStackTrace();
        }
        return 1;
    }

    private static boolean compare(byte[] actual, Object expected) {
        try {
            byte[] expected_bytes = (byte[]) expected;
            if (expected_bytes.length != actual.length) {
                return false;
            }
            for (int i = 0; i < expected_bytes.length; ++i) {
                if (expected_bytes[i] != actual[i]) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean compare(List<Object> actual, Object expected) {
        try {
            @SuppressWarnings("unchecked")
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
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean compare(Map<String, Object> actual, Object expected) {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> expected_map = (Map<String, Object>) expected;
            if (expected_map.size() != actual.size()) {
                return false;
            }
            for (String key : expected_map.keySet()) {
                if (!compare(actual.get(key), expected_map.get(key))) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean compare(Object actual, Object expected) {
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
                    List<Object> actual_list = (List<Object>) actual;
                    return compare(actual_list, expected);
                }
            } else if (expected instanceof Map<?, ?>) {
                if (actual instanceof ErlangValue) {
                    return compare(((ErlangValue) actual).jsonValue(), expected);
                } else {
                    @SuppressWarnings("unchecked")
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
        } catch (Exception e) {
            return false;
        }
    }

    private static String valueToStr(Object value) {
        String value_str;
        if (value instanceof String) {
//            value_str = "\"" + ((String) value).replace("\n", "\\n") + "\"";
            value_str = "\"" + value + "\"";
        } else if (value instanceof byte[]) {
            byte[] bytes = ((byte[]) value);
            value_str = "<<";
            for (byte b : bytes) {
                value_str += b;
            }
            value_str += ">>";
        } else if (value instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = ((Map<String, Object>) value);
            value_str = "{";
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                value_str += valueToStr(entry.getKey()) + "=" + valueToStr(entry.getValue()) + ",";
            }
            // remove last ","
            if (map.size() > 0) {
                value_str = value_str.substring(0, value_str.length() - 1);
            }
            value_str += "}";
        } else if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            value_str = "[";
            for (Object object : list) {
                value_str += valueToStr(object) + ",";
            }
            // remove last ","
            if (list.size() > 0) {
                value_str = value_str.substring(0, value_str.length() - 1);
            }
            value_str += "]";
        } else if (value instanceof ErlangValue) {
            value_str = ((ErlangValue) value).value().toString();
        } else {
            value_str = value.toString();
        }
        return value_str;
    }

    private static int read_write_boolean(String basekey, TransactionSingleOp sc, Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_bool_false"; value = false;
        failed += read_or_write(sc, key, value, mode);

        key = basekey + "_bool_true"; value = true;
        failed += read_or_write(sc, key, value, mode);

        return failed;
    }

    private static int read_write_integer(String basekey, TransactionSingleOp sc, Mode mode) {
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

    private static int read_write_long(String basekey, TransactionSingleOp sc, Mode mode) {
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

    private static int read_write_biginteger(String basekey, TransactionSingleOp sc, Mode mode) {
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

    private static int read_write_double(String basekey, TransactionSingleOp sc, Mode mode) {
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

    private static int read_write_string(String basekey, TransactionSingleOp sc, Mode mode) {
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

    private static int read_write_binary(String basekey, TransactionSingleOp sc, Mode mode) {
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

    private static int read_write_list(String basekey, TransactionSingleOp sc, Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_list_empty"; value = new ArrayList<Object>();
        failed += read_or_write(sc, key, value, mode);

        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(0); list1.add(1); list1.add(2); list1.add(3);
        key = basekey + "_list_0_1_2_3"; value = list1;
        failed += read_or_write(sc, key, value, mode);

        ArrayList<Integer> list2 = new ArrayList<Integer>();
        list2.add(0); list2.add(123); list2.add(456); list2.add(65000);
        key = basekey + "_list_0_123_456_65000"; value = list2;
        failed += read_or_write(sc, key, value, mode);

        ArrayList<Integer> list3 = new ArrayList<Integer>();
        list3.add(0); list3.add(123); list3.add(456); list3.add(0x10ffff);
        key = basekey + "_list_0_123_456_0x10ffff"; value = list3;
        failed += read_or_write(sc, key, value, mode);

        ArrayList<Object> list4 = new ArrayList<Object>();
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

        return failed;
    }

    // Map/JSON:
    private static int read_write_map(String basekey, TransactionSingleOp sc, Mode mode) {
        int failed = 0;
        String key;
        Object value;

        key = basekey + "_map_empty";
        value = new LinkedHashMap<String, Object>();
        failed += read_or_write(sc, key, value, mode);

        LinkedHashMap<String, Integer> map1 = new LinkedHashMap<String, Integer>();
        map1.put("x", 0);
        map1.put("y", 1);
        key = basekey + "_map_x=0_y=1";
        value = map1;
        failed += read_or_write(sc, key, value, mode);

        LinkedHashMap<String, Object> map2 = new LinkedHashMap<String, Object>();
        map2.put("a", 0);
        map2.put("b", "foo");
        map2.put("c", 1.5);
        map2.put("d", "foo\nbar");
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(0);
        list1.add(1);
        list1.add(2);
        list1.add(3);
        map2.put("e", list1);
        map2.put("f", map1);
        key = basekey + "_map_a=0_b=foo_c=1.5_d=foo<nl>bar_e=list0123_f=mapx0y1";
        value = map2;
        failed += read_or_write(sc, key, value, mode);

        LinkedHashMap<String, Integer> map3 = new LinkedHashMap<String, Integer>();
        map3.put("", 0);
        key = basekey + "_map_=0";
        value = map3;
        failed += read_or_write(sc, key, value, mode);

        LinkedHashMap<String, Object> map4 = new LinkedHashMap<String, Object>();
        // some (arbitrary) unicode characters
        // (please don't be offended if they actually mean something)
        map4.put("x", 0);
        map4.put("y", "foo\u0180\u01E3\u11E5");
        key = basekey + "_map_x=0_y=foo\u0180\u01E3\u11E5";
        value = map4;
        failed += read_or_write(sc, key, value, mode);

        LinkedHashMap<String, Integer> map5 = new LinkedHashMap<String, Integer>();
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
        Options options = new Options();
        OptionGroup group = new OptionGroup();

        /* Note: arguments are set to be optional since we implement argument
         * checks on our own (commons.cli is not flexible enough and only
         * checks for the existence of a first argument)
         */

        options.addOption(new Option("h", "help", false, "print this message"));

        options.addOption(new Option("v", "verbose", false, "print verbose information, e.g. the properties read"));

        Option read = new Option("r", "read", true, "read an item");
        read.setArgName("basekey> <language");
        read.setArgs(2);
        read.setOptionalArg(true);
        group.addOption(read);

        Option write = new Option("w", "write", true, "write an item");
        write.setArgName("basekey");
        write.setArgs(1);
        write.setOptionalArg(true);
        group.addOption(write);

        options.addOptionGroup(group);

        options.addOption(new Option("lh", "localhost", false, "gets the local host's name as known to Java (for debugging purposes)"));

        return options;
    }
}
