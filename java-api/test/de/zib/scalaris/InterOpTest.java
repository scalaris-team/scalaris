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

import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * Class to test interoperability between the different Scalaris APIs.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 3.1
 * @since 3.1
 */
public class InterOpTest {
    private static Object lastValue;
    private static String lastKey;
    
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
        
        if (line.hasOption("r")) { // read
            String basekey = line.getOptionValue("read");
            Main.checkArguments(basekey, options, "r");
            basekey += "_java";
            lastKey = null;
            lastValue = null;
            try {
                TransactionSingleOp sc = new TransactionSingleOp();
                int failed = 0;
                
                Mode mode = Mode.READ;
                failed += read_write_integer(basekey, sc, mode);
                failed += read_write_long(basekey, sc, mode);
                failed += read_write_biginteger(basekey, sc, mode);
                failed += read_write_double(basekey, sc, mode);
                failed += read_write_string(basekey, sc, mode);
                failed += read_write_binary(basekey, sc, mode);
                failed += read_write_list(basekey, sc, mode);
                failed += read_write_map(basekey, sc, mode);
                
                sc.closeConnection();
                if (failed > 0) {
                    System.out.println(failed + " number of reads failed.");
                    System.exit(-1);
                }
            } catch (ConnectionException e) {
                Main.printException("read(" + lastKey + ") failed with connection error", e, verbose);
            } catch (TimeoutException e) {
                Main.printException("read(" + lastKey + ") failed with timeout", e, verbose);
            } catch (NotFoundException e) {
                Main.printException("read(" + lastKey + ") failed with not found", e, verbose);
            } catch (UnknownException e) {
                Main.printException("read(" + lastKey + ") failed with unknown", e, verbose);
            } catch (AbortException e) {
                Main.printException("read(" + lastKey + ") failed with abort", e, verbose);
            }
        } else if (line.hasOption("w")) { // write
            String basekey = line.getOptionValue("write");
            Main.checkArguments(basekey, options, "w");
            basekey += "_java";
            lastKey = null;
            lastValue = null;
            try {
                TransactionSingleOp sc = new TransactionSingleOp();
                int failed = 0;
                
                Mode mode = Mode.WRITE;
                failed += read_write_integer(basekey, sc, mode);
                failed += read_write_long(basekey, sc, mode);
                failed += read_write_biginteger(basekey, sc, mode);
                failed += read_write_double(basekey, sc, mode);
                failed += read_write_string(basekey, sc, mode);
                failed += read_write_binary(basekey, sc, mode);
                failed += read_write_list(basekey, sc, mode);
                failed += read_write_map(basekey, sc, mode);
                
                sc.closeConnection();
                if (failed > 0) {
                    System.out.println(failed + " number of reads failed.");
                    System.exit(-1);
                }
            } catch (ConnectionException e) {
                Main.printException("write(" + lastKey + ", " + lastValue + ") failed with connection error", e, verbose);
            } catch (TimeoutException e) {
                Main.printException("write(" + lastKey + ", " + lastValue + ") failed with timeout", e, verbose);
            } catch (AbortException e) {
                Main.printException("write(" + lastKey + ", " + lastValue + ") failed with abort", e, verbose);
            } catch (UnknownException e) {
                Main.printException("write(" + lastKey + ", " + lastValue + ") failed with unknown", e, verbose);
            } catch (NotFoundException e) {
                Main.printException("write(" + lastKey + ", " + lastValue + ") failed with not_found", e, verbose);
            }
        } else if (line.hasOption("lh")) { // get local host name
            System.out.println(ConnectionFactory.getLocalhostName());
        } else {
            // print help if no other option was given
//        if (line.hasOption("help")) {
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("scalaris [Options]", getOptions());
        }
    }
    
    private static int read_or_write(TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        switch (mode) {
            case READ:
                System.out.println("read(" + lastKey + ")");
                ErlangValue result = sc.read(lastKey);
                System.out.println("  expected: " + valueToStr(lastValue));
                System.out.println("  read raw: " + result.value().toString());
                
                Object jresult = null;
                if (lastValue instanceof Integer) {
                    jresult = result.toInt();
                } else if (lastValue instanceof Long) {
                    jresult = result.toLong();
                } else if (lastValue instanceof BigInteger) {
                    jresult = result.toBigInt();
                } else if (lastValue instanceof Double) {
                    jresult = result.toDouble();
                } else if (lastValue instanceof String) {
                    jresult = result.toString();
                } else if (lastValue instanceof byte[]) {
                    jresult = result.toBinary();
                } else if (lastValue instanceof List<?>) {
                    jresult = result.toList();
                } else if (lastValue instanceof Map<?, ?>) {
                    jresult = result.toJSON();
                }
                
                System.out.println(" read java: " + valueToStr(jresult));
                OtpErlangObject result_exp = new ErlangValue(lastValue).value();
                OtpErlangObject jresult2 = new ErlangValue(jresult).value();
                if (jresult2.equals(result_exp) && compare(jresult, lastValue)) {
                    System.out.println("ok");
                    return 0;
                } else {
                    System.out.println("fail " + jresult2.toString() + " != " + result_exp.toString());
                    return 1;
                }
            case WRITE:
                System.out.println("write(" + lastKey + ", " + valueToStr(lastValue) + ")");
                sc.write(lastKey, lastValue);
                return 0;
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
                    return compare(((ErlangValue) actual).toBinary(), expected);
                } else {
                    return compare((byte[]) actual, expected);
                } 
            } else if (expected instanceof List<?>) {
                if (actual instanceof ErlangValue) {
                    return compare(((ErlangValue) actual).toList(), expected);
                } else {
                    @SuppressWarnings("unchecked")
                    List<Object> actual_list = (List<Object>) actual;
                    return compare(actual_list, expected);
                }  
            } else if (expected instanceof Map<?, ?>) {
                if (actual instanceof ErlangValue) {
                    return compare(((ErlangValue) actual).toJSON(), expected);
                } else {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> actual_map = (Map<String, Object>) actual;
                    return compare(actual_map, expected);
                }   
            } else if (expected instanceof Integer) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).toInt());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof Long) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).toLong());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof BigInteger) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).toBigInt());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof Double) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).toDouble());
                } else {
                    return expected.equals(actual);
                }
            } else if (expected instanceof String) {
                if (actual instanceof ErlangValue) {
                    return expected.equals(((ErlangValue) actual).toString());
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
    
    private static int read_write_integer(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_int_0"; lastValue = 0;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_int_1"; lastValue = 1;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_int_min"; lastValue = Integer.MIN_VALUE;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_int_max"; lastValue = Integer.MAX_VALUE;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_int_max_div_2"; lastValue = Integer.MAX_VALUE / 2;
        failed += read_or_write(sc, mode);
        
        return failed;
    }
    
    private static int read_write_long(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_long_0"; lastValue = 0l;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_long_1"; lastValue = 1l;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_long_min"; lastValue = Long.MIN_VALUE;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_long_max"; lastValue = Long.MAX_VALUE;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_long_max_div_2"; lastValue = Long.MAX_VALUE / 2l;
        failed += read_or_write(sc, mode);
        
        return failed;
    }
    
    private static int read_write_biginteger(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_bigint_0"; lastValue = new BigInteger("0");
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_bigint_1"; lastValue = new BigInteger("1");
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_bigint_min"; lastValue = new BigInteger("-100000000000000000000");
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_bigint_max"; lastValue = new BigInteger("100000000000000000000");
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_bigint_max_div_2"; lastValue = new BigInteger("-100000000000000000000").divide(new BigInteger("2"));
        failed += read_or_write(sc, mode);
        
        return failed;
    }
    
    private static int read_write_double(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_float_0.0"; lastValue = 0.0;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_float_1.5"; lastValue = 1.5;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_float_-1.5"; lastValue = -1.5;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_float_min"; lastValue = Double.MIN_VALUE;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_float_max"; lastValue = Double.MAX_VALUE;
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_float_max_div_2"; lastValue = Double.MAX_VALUE / 2.0;
        failed += read_or_write(sc, mode);
        
        // not supported by OTP:
//        key = basekey + "_float_neg_inf"; value = Double.NEGATIVE_INFINITY;
//        sc.write(key, value);
//        key = basekey + "_float_pos_inf"; value = Double.POSITIVE_INFINITY;
//        sc.write(key, value);
//        key = basekey + "_float_nan"; value = Double.NaN;
//        sc.write(key, value);
        
        return failed;
    }
    
    private static int read_write_string(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_string_empty"; lastValue = "";
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_string_foobar"; lastValue = "foobar";
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_string_foo\\nbar"; lastValue = "foo\nbar";
        failed += read_or_write(sc, mode);
        
        return failed;
    }
    
    private static int read_write_binary(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_byte_empty"; lastValue = new byte[0];
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_byte_0"; lastValue = new byte[] {0};
        failed += read_or_write(sc, mode);
        
        lastKey = basekey + "_byte_0123"; lastValue = new byte[] {0, 1, 2, 3};
        failed += read_or_write(sc, mode);
        
        return failed;
    }
    
    private static int read_write_list(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_list_empty"; lastValue = new ArrayList<Object>();
        failed += read_or_write(sc, mode);
        
        ArrayList<Integer> list1 = new ArrayList<Integer>();
        list1.add(0); list1.add(1); list1.add(2); list1.add(3);
        lastKey = basekey + "_list_0_1_2_3"; lastValue = list1;
        ArrayList<Object> list2 = new ArrayList<Object>();
        list2.add(0);
        list2.add("foo");
        list2.add(1.5);
        list2.add(new byte[] {0, 1, 2, 3});
        lastKey = basekey + "_list_0_foo_1.5_<<0123>>"; lastValue = list2;
        failed += read_or_write(sc, mode);
        
        return failed;
    }

    // Map/JSON:
    private static int read_write_map(String basekey, TransactionSingleOp sc, Mode mode)
            throws ConnectionException, TimeoutException, AbortException,
            UnknownException, NotFoundException {
        int failed = 0;
        
        lastKey = basekey + "_map_empty";
        lastValue = new LinkedHashMap<String, Object>();
        failed += read_or_write(sc, mode);
        
        LinkedHashMap<String, Integer> map1 = new LinkedHashMap<String, Integer>();
        map1.put("x", 0);
        map1.put("y", 1);
        lastKey = basekey + "_map_x0_y1";
        lastValue = map1;
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
        lastKey = basekey + "_map_a0_bfoo_c1.5_dfoo<nl>bar_elist0123_fmapx0y1";
        lastValue = map2;
        failed += read_or_write(sc, mode);
        
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
        read.setArgName("basekey");
        read.setArgs(1);
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
