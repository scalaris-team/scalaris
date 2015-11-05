/**
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

import static org.junit.Assert.*;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.LinkedList;

import org.junit.Test;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Unit tests for {@link ErlangValue}.
 *
 * TODO: implement tests verifying that the expected exceptions are thrown for
 * unsupported conversions
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class ErlangValueTest {
    private static BigInteger getRandomBigInt(final Random random) {
        return BigInteger.valueOf(random.nextLong()).multiply(BigInteger.valueOf(random.nextLong()));
    }

    private static byte[] getRandomBytes(final Random random, final int size) {
        final byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    private static String getRandomString(final Random random, final int length, final boolean onlyChars) {
        if (onlyChars) {
            return getRandomCharString(random, length);
        } else {
            try {
                return Benchmark.getRandom(length, String.class, random);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static final char[] chars = new char[26];
    private static final char[] digits = new char[10];
    private static final char[] symbols = new char[36];

    static {
        for (int i = 0; i < 26; ++i) {
            chars[i] = (char) ('a' + i);
            symbols[i] = (char) ('a' + i);
        }
        for (int i = 0; i < 10; ++i) {
            digits[i] = (char) ('0' + i);
            symbols[i + 26] = (char) ('0' + i);
        }
    }

    private static String getRandomCharString(final Random random, final int length) {
        if(length > 0) {
            final char[] result = new char[length];
            // lets always start with a character:
            result[0] = chars[random.nextInt(chars.length)];
            for (int i = 1; i < result.length; ++i) {
                result[0] = symbols[random.nextInt(symbols.length)];
            }
            return new String(result);
        }
        return "";
    }

    private static List<Object> getRandomList(final Random random, final int capacity) {
        // note: we do not generate (recursive) maps -> so there won't be keys to worry about
        return getRandomListRecursive(random, capacity, 0, false);
    }

    private static List<Object> getRandomListRecursive(final Random random, final int capacity, final int maxDepth, final boolean mapKeyOnlyChars) {
        List<Object> currentList = null;
        currentList = new ArrayList<Object>(capacity);
        final int curMaxDepth = maxDepth == 0 ? 0 : random.nextInt(maxDepth);
        final int maxType = curMaxDepth == 0 ? 6 : 8;
        switch (random.nextInt(maxType)) {
            case 0: // bool
                currentList.add(random.nextBoolean());
                break;
            case 1: // int
                currentList.add(random.nextInt());
                break;
            case 2: // long
                currentList.add(random.nextLong());
                break;
            case 3: // BigInteger
                currentList.add(getRandomBigInt(random));
                break;
            case 4: // double
                currentList.add(random.nextDouble());
                break;
            case 5: // String
                currentList.add(getRandomString(random, random.nextInt(10), false));
                break;
            case 6: // List
                currentList.add(getRandomListRecursive(random, capacity, curMaxDepth, mapKeyOnlyChars));
                break;
            case 7: // Map
                currentList.add(getRandomMapRecursive(random, capacity, curMaxDepth, mapKeyOnlyChars));
                break;
            default:
                throw new RuntimeException("unexpected random number");
        }
        return currentList;
    }

    private static Map<String, Object> getRandomMapRecursive(final Random random, final int capacity, final int maxDepth, final boolean keyOnlyChars) {
        Map<String, Object> currentMap = null;
        currentMap = new LinkedHashMap<String, Object>(capacity);
        for (int i = 0; i < capacity; ++i) {
            // key:
            final String key = getRandomString(random, random.nextInt(10), keyOnlyChars);
            // value:
            final int curMaxDepth = maxDepth == 0 ? 0 : random.nextInt(maxDepth);
            final int maxType = curMaxDepth == 0 ? 6 : 8;
            switch (random.nextInt(maxType)) {
                case 0: // bool
                    currentMap.put(key, random.nextBoolean());
                    break;
                case 1: // int
                    currentMap.put(key, random.nextInt());
                    break;
                case 2: // long
                    currentMap.put(key, random.nextLong());
                    break;
                case 3: // BigInteger
                    currentMap.put(key, getRandomBigInt(random));
                    break;
                case 4: // double
                    currentMap.put(key, random.nextDouble());
                    break;
                case 5: // String
                    currentMap.put(key, getRandomString(random, random.nextInt(10), false));
                    break;
                case 6: // List
                    currentMap.put(key, getRandomListRecursive(random, capacity, curMaxDepth, keyOnlyChars));
                    break;
                case 7: // Map
                    currentMap.put(key, getRandomMapRecursive(random, capacity, curMaxDepth, keyOnlyChars));
                    break;
                default:
                    throw new RuntimeException("unexpected random number");
            }
        }
        return currentMap;
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#boolValue()}.
     */
    @Test
    public final void testBoolValue() {
        final ErlangValue trueVal = new ErlangValue(true);
        final ErlangValue trueValOtp = new ErlangValue(new OtpErlangBoolean(true));
        final ErlangValue falseVal = new ErlangValue(false);
        final ErlangValue falseValOtp = new ErlangValue(new OtpErlangBoolean(false));

        assertEquals(true, trueVal.boolValue());
        assertEquals(true, trueValOtp.boolValue());
        assertEquals(false, falseVal.boolValue());
        assertEquals(false, falseValOtp.boolValue());
        assertEquals(trueVal, trueValOtp);
        assertEquals(trueValOtp, trueVal);
        assertEquals(falseVal, falseValOtp);
        assertEquals(falseValOtp, falseVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#intValue()}.
     *
     * @throws Exception if a test with a random integer failed
     */
    @Test
    public final void testIntValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Integer currentInt = null;
            try {
                currentInt = random.nextInt();
                testIntValue(currentInt);
            } catch (final ClassCastException e) {
                throw new Exception("testIntValue(" + currentInt + ") failed", e);
            }
        }
    }

    private final void testIntValue(final int value) {
        final ErlangValue eVal = new ErlangValue(value);
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangInt(value));

        assertEquals(value, eVal.intValue());
        assertEquals(value, eValOtp.intValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#longValue()}.
     *
     * @throws Exception if a test with a random long failed
     */
    @Test
    public final void testLongValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Long currentLong = null;
            try {
                currentLong = random.nextLong();
                testLongValue(currentLong);
            } catch (final ClassCastException e) {
                throw new Exception("testLongValue(" + currentLong + ") failed", e);
            }
        }
    }

    private final void testLongValue(final long value) {
        final ErlangValue eVal = new ErlangValue(value);
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangLong(value));

        assertEquals(value, eVal.longValue());
        assertEquals(value, eValOtp.longValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#bigIntValue()}.
     *
     * @throws Exception if a test with a random big integer failed
     */
    @Test
    public final void testBigIntValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            BigInteger currentBigInt = null;
            try {
                currentBigInt = getRandomBigInt(random);
                testBigIntValue(currentBigInt);
            } catch (final ClassCastException e) {
                throw new Exception("testBigIntValue(" + currentBigInt + ") failed", e);
            }
        }
    }

    private final void testBigIntValue(final BigInteger value) {
        final ErlangValue eVal = new ErlangValue(value);
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangLong(value));

        assertEquals(value, eVal.bigIntValue());
        assertEquals(value, eValOtp.bigIntValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#doubleValue()}.
     *
     * @throws Exception if a test with a random double failed
     */
    @Test
    public final void testDoubleValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Double currentDouble = null;
            try {
                currentDouble = random.nextDouble();
                testDoubleValue(currentDouble);
            } catch (final ClassCastException e) {
                throw new Exception("testDoubleValue(" + currentDouble + ") failed", e);
            }
        }
    }

    private final void testDoubleValue(final double value) {
        final ErlangValue eVal = new ErlangValue(value);
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangDouble(value));

        assertEquals(value, eVal.doubleValue(), 0.0);
        assertEquals(value, eValOtp.doubleValue(), 0.0);
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#stringValue()}.
     *
     * @throws Exception if a test with a random double failed
     */
    @Test
    public final void testStringValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            String currentString = null;
            try {
                currentString = getRandomString(random, random.nextInt(1000), false);
                testStringValue(currentString);
            } catch (final ClassCastException e) {
                throw new Exception("testStringValue(" + currentString + ") failed", e);
            }
        }
    }

    private final void testStringValue(final String value) {
        final ErlangValue eVal = new ErlangValue(value);
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangString(value));

        assertEquals(value, eVal.stringValue());
        assertEquals(value, eValOtp.stringValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#binaryValue()}.
     *
     * @throws Exception if a test with a random byte array failed
     */
    @Test
    public final void testBinaryValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 5000; ++i) {
            try {
                testBinaryValue(getRandomBytes(random, random.nextInt(1000)));
            } catch (final ClassCastException e) {
                // do not print generated bytes (probably not useful)
                throw new Exception("testBinaryValue(...) failed", e);
            }
        }
    }

    private final void testBinaryValue(final byte[] value) {
        final ErlangValue eVal = new ErlangValue(value);
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangBinary(value));

        assertArrayEquals(value, eVal.binaryValue());
        assertArrayEquals(value, eValOtp.binaryValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#listValue()}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testListValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<Object> currentList = null;
            try {
                final int capacity = random.nextInt(1000);
                currentList = getRandomList(random, capacity);
                testListValue(currentList);
            } catch (final ClassCastException e) {
                throw new Exception("testListValue(" + currentList + ") failed", e);
            }
        }
    }

    private final void testListValue(final List<Object> value) {
        final ErlangValue eVal = new ErlangValue(value);
        final OtpErlangObject[] valueOtp = new OtpErlangObject[value.size()];
        int i = 0;
        for (final Object value_i : value) {
            OtpErlangObject valueOtp_i;

            if (value_i instanceof Boolean) {
                valueOtp_i = new OtpErlangBoolean((Boolean) value_i);
            } else if (value_i instanceof Integer) {
                valueOtp_i = new OtpErlangInt((Integer) value_i);
            } else if (value_i instanceof Long) {
                valueOtp_i = new OtpErlangLong((Long) value_i);
            } else if (value_i instanceof BigInteger) {
                valueOtp_i = new OtpErlangLong((BigInteger) value_i);
            } else if (value_i instanceof Double) {
                valueOtp_i = new OtpErlangDouble((Double) value_i);
            } else if (value_i instanceof String) {
                valueOtp_i = new OtpErlangString((String) value_i);
            } else {
                fail("unsupported (expected) value: " + value_i);
                return; // so the Java-compiler does not complain about the following assignment
            }
            valueOtp[i++] = valueOtp_i;
        }
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        compareList(value, eVal.listValue());
        compareList(value, eValOtp.listValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    private static void compareList(final List<Object> expected, final List<ErlangValue> actual) {
        assertEquals(expected.size(), actual.size());
        for (int j = 0; j < actual.size(); ++j) {
            final Object expected_j = expected.get(j);
            final ErlangValue actual_j = actual.get(j);
            if (expected_j instanceof Boolean) {
                assertEquals(expected_j, actual_j.boolValue());
            } else if (expected_j instanceof Integer) {
                assertEquals(expected_j, new Integer(actual_j.intValue()));
            } else if (expected_j instanceof Long) {
                assertEquals(expected_j, new Long(actual_j.longValue()));
            } else if (expected_j instanceof BigInteger) {
                assertEquals(expected_j, actual_j.bigIntValue());
            } else if (expected_j instanceof Double) {
                assertEquals(expected_j, new Double(actual_j.doubleValue()));
            } else if (expected_j instanceof String) {
                assertEquals(expected_j, actual_j.stringValue());
            } else {
                fail("unsupported (expected) value: " + expected_j);
            }
        }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#longListValue()}.
     *
     * @throws Exception if a test with a random list of longs failed
     */
    @Test
    public final void testLongListValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<Long> currentList = null;
            try {
                final int capacity = random.nextInt(1000);
                currentList = new ArrayList<Long>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(random.nextLong());
                }
                testLongListValue(currentList);
            } catch (final ClassCastException e) {
                throw new Exception("testLongListValue(" + currentList + ") failed", e);
            }
        }
    }

    private final void testLongListValue(final List<Long> value) {
        final ErlangValue eVal = new ErlangValue(value);
        final OtpErlangLong[] valueOtp = new OtpErlangLong[value.size()];
        int i = 0;
        for (final Long long2 : value) {
            final Long long1 = long2;
            valueOtp[i++] = new OtpErlangLong(long1);
        }
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        assertEquals(value, eVal.longListValue());
        assertEquals(value, eValOtp.longListValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#doubleListValue()}.
     *
     * @throws Exception if a test with a random list of doubles failed
     */
    @Test
    public final void testDoubleListValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<Double> currentList = null;
            try {
                final int capacity = random.nextInt(1000);
                currentList = new ArrayList<Double>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(random.nextDouble());
                }
                testDoubleListValue(currentList);
            } catch (final ClassCastException e) {
                throw new Exception("testDoubleListValue(" + currentList + ") failed", e);
            }
        }
    }

    private final void testDoubleListValue(final List<Double> value) {
        final ErlangValue eVal = new ErlangValue(value);
        final OtpErlangDouble[] valueOtp = new OtpErlangDouble[value.size()];
        int i = 0;
        for (final Double double2 : value) {
            final Double double1 = double2;
            valueOtp[i++] = new OtpErlangDouble(double1);
        }
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        assertEquals(value, eVal.doubleListValue());
        assertEquals(value, eValOtp.doubleListValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#stringListValue()}.
     *
     * @throws Exception if a test with a random list of strings failed
     */
    @Test
    public final void testStringListValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<String> currentList = null;
            try {
                final int capacity = random.nextInt(10);
                currentList = new ArrayList<String>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(getRandomString(random, random.nextInt(1000), false));
                }
                testStringListValue(currentList);
            } catch (final ClassCastException e) {
                throw new Exception("testStringListValue(" + currentList + ") failed", e);
            }
        }
    }

    private final void testStringListValue(final List<String> value) {
        final ErlangValue eVal = new ErlangValue(value);
        final OtpErlangString[] valueOtp = new OtpErlangString[value.size()];
        int i = 0;
        for (final String string : value) {
            final String string1 = string;
            valueOtp[i++] = new OtpErlangString(string1);
        }
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        assertEquals(value, eVal.stringListValue());
        assertEquals(value, eValOtp.stringListValue());
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#binaryListValue()}.
     *
     * @throws Exception if a test with a random list of binaries failed
     */
    @Test
    public final void testBinaryListValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 1000; ++i) {
            try {
                final int capacity = random.nextInt(10);
                final List<byte[]> currentList = new ArrayList<byte[]>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(getRandomBytes(random, random.nextInt(1000)));
                }
                testBinaryListValue(currentList);
            } catch (final ClassCastException e) {
                // do not print generated bytes (probably not useful)
                throw new Exception("testBinaryListValue(...) failed", e);
            }
        }
    }

    private final void testBinaryListValue(final List<byte[]> value) {
        final ErlangValue eVal = new ErlangValue(value);
        final OtpErlangBinary[] valueOtp = new OtpErlangBinary[value.size()];
        int i = 0;
        for (final byte[] b : value) {
            final byte[] binary1 = b;
            valueOtp[i++] = new OtpErlangBinary(binary1);
        }
        final ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        final List<byte[]> actual = eVal.binaryListValue();
        final List<byte[]> actualOtp = eValOtp.binaryListValue();
        assertEquals(value.size(), actual.size());
        assertEquals(value.size(), actualOtp.size());
        for (int j = 0; j < actual.size(); ++j) {
            assertArrayEquals(value.get(j), actual.get(j));
            assertArrayEquals(value.get(j), actualOtp.get(j));
        }
        assertEquals(eVal, eValOtp);
        assertEquals(eValOtp, eVal);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue()}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValue() throws Exception {
        final Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Map<String, Object> currentMap = null;
            try {
                currentMap = getRandomMapRecursive(random, random.nextInt(10), 3, false);
                testJsonValue(currentMap);
            } catch (final ClassCastException e) {
                throw new Exception("testJsonValue(" + currentMap + ") failed", e);
            }
        }
    }

    private final void testJsonValue(final Map<String, Object> value) {
        final ErlangValue eVal = new ErlangValue(value);
        final Map<String, Object> actual = eVal.jsonValue();
        compareMap(value, actual);
        final ErlangValue eVal2 = new ErlangValue(value);
        assertEquals(eVal, eVal2);
        assertEquals(eVal2, eVal);
    }

    private final void compareMap(final Map<String, Object> expected, final Map<String, Object> actual) {
        assertEquals(expected.size(), actual.size());
        for (final Entry<String, Object> entry : expected.entrySet()) {
            final String expected_key_i = entry.getKey();
            final Object expected_value_i = entry.getValue();

            assertTrue(actual.containsKey(expected_key_i));
            final Object actual_value_i = actual.get(expected_key_i);
            assertEquals(expected_value_i, actual_value_i);
        }
    }

    private static class JSONBeanTest1 {
        private boolean a = true;
        private int b = 0;
        private long c = 0;
        private BigInteger d = new BigInteger("0");
        private double e = 0.0;
        private String f = "";

        public JSONBeanTest1() {}

        public boolean getA() { return a; }
        public int getB() { return b; }
        public long getC() { return c; }
        public BigInteger getD() { return d; }
        public double getE() { return e; }
        public String getF() { return f; }

        public void setA(final boolean a_) { this.a = a_; }
        public void setB(final int b_) { this.b = b_; }
        public void setC(final long c_) { this.c = c_; }
        public void setD(final BigInteger d_) { this.d = d_; }
        public void setE(final double e_) { this.e = e_; }
        public void setF(final String f_) { this.f = f_; }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}
     * writing a {@link Map} and reading a {@link JSONBeanTest1}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean1a() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < 5000; ++i) {
            final Map<String, Object> map = new LinkedHashMap<String, Object>(6);
            map.put("a", random.nextBoolean());
            map.put("b", random.nextInt());
            map.put("c", random.nextLong());
            map.put("d", getRandomBigInt(random));
            map.put("e", random.nextDouble());
            map.put("f", getRandomString(random, random.nextInt(100), false));
            final ErlangValue value = new ErlangValue(map);

            final JSONBeanTest1 actual = value.jsonValue(JSONBeanTest1.class);
            assertEquals(map.get("a"), actual.getA());
            assertEquals(map.get("b"), actual.getB());
            assertEquals(map.get("c"), actual.getC());
            assertEquals(map.get("d"), actual.getD());
            assertEquals(map.get("e"), actual.getE());
            assertEquals(map.get("f"), actual.getF());
            compareScalarisJSON(value, new ErlangValue(actual));
        }
    }

    /**
     * Compares two erlang values which represent JSON tuples.
     *
     * The order of the JSON objects' properties does not matter.
     *
     * @param expected
     * @param actual
     */
    private final void compareScalarisJSON(final ErlangValue expected,
            final ErlangValue actual) {
        compareScalarisJSON(expected.value(), actual.value(), actual.toString());
    }

    /**
     * Compares the two erlang object assuming they are both JSON tuples.
     *
     * @param expected
     * @param actual
     */
    private final void compareScalarisJSON(final OtpErlangObject expected,
            final OtpErlangObject actual, final String actualOriginal) {
        // verify: tuples with arity 2 and "struct" as the first element
        final OtpErlangTuple expectedT = (OtpErlangTuple) expected;
        assert (expectedT.arity() == 2);
        String msg = "Checking '" + actual + "' in " + actualOriginal;
        assertTrue(msg, actual instanceof OtpErlangTuple);
        final OtpErlangTuple actualT = (OtpErlangTuple) actual;
        assertTrue(msg, actualT.arity() == 2);
        assert (expectedT.elementAt(0).equals(new OtpErlangString("struct")));
        assertEquals("Checking '" + actualT.elementAt(0) + "' in "
                + actualOriginal, CommonErlangObjects.structAtom, actualT.elementAt(0));

        // verify: second element in the struct-tuple is a list of properties
        // ({Key, Value} tuples with string keys)
        final OtpErlangList expectedPropsL = (OtpErlangList) expectedT
                .elementAt(1);
        msg = "Checking '" + actualT.elementAt(1) + "' in " + actualOriginal;
        assertTrue(msg, actualT.elementAt(1) instanceof OtpErlangList);
        final OtpErlangList actualPropsL = (OtpErlangList) actualT.elementAt(1);
        assertEquals(msg, expectedPropsL.arity(), actualPropsL.arity());
        final HashMap<String, OtpErlangObject> actualProperties = new HashMap<String, OtpErlangObject>(
                actualPropsL.arity());
        // put the actual object's properties into a hash map for quick access
        // while on it, verify the types, too
        for (int i = 0; i < actualPropsL.arity(); ++i) {
            final OtpErlangObject element = actualPropsL.elementAt(i);
            msg = "Checking '" + element + "' in " + actualOriginal;
            assertTrue(msg, element instanceof OtpErlangTuple);
            final OtpErlangTuple actualPropT = (OtpErlangTuple) element;
            assertTrue(msg, actualPropT.arity() == 2);

            final OtpErlangObject actualPropKey = actualPropT.elementAt(0);
            final OtpErlangObject actualPropVal = actualPropT.elementAt(1);
            assertTrue("Checking property '" + actualPropKey + "' in "
                    + actualOriginal, actualPropKey instanceof OtpErlangString);
            actualProperties.put(
                    ((OtpErlangString) actualPropKey).stringValue(),
                    actualPropVal);
        }

        // verify: all properties from expected exist in actual and no more
        for (int i = 0; i < expectedPropsL.arity(); ++i) {
            final OtpErlangTuple expectedPropT = (OtpErlangTuple) expectedPropsL
                    .elementAt(i);
            assert (expectedPropT.arity() == 2);
            final String expectedPropKey = ((OtpErlangString) expectedPropT
                    .elementAt(0)).stringValue();
            final OtpErlangObject expectedPropVal = expectedPropT.elementAt(1);
            assertTrue("Checking property '" + expectedPropKey + "' in "
                    + actualOriginal,
                    actualProperties.containsKey(expectedPropKey));
            final OtpErlangObject actualPropVal = actualProperties
                    .get(expectedPropKey);
            if (actualPropVal instanceof OtpErlangTuple) {
                final OtpErlangTuple expectedPropValT = (OtpErlangTuple) expectedPropVal;
                final OtpErlangTuple actualPropValT = (OtpErlangTuple) actualPropVal;
                if ((expectedPropValT.arity() == 2)
                        && (actualPropValT.arity() == 2)
                        && expectedPropValT.elementAt(0).equals(CommonErlangObjects.arrayAtom)
                        && actualPropValT.elementAt(0).equals(CommonErlangObjects.arrayAtom)
                        && (expectedPropValT.elementAt(1) instanceof OtpErlangList)
                        && (actualPropValT.elementAt(1) instanceof OtpErlangList)) {
                    // these lists must be equal including their order
                    assertEquals("Checking '" + actualPropValT.elementAt(1) + "' in " + actualOriginal,
                            actualPropValT.elementAt(1), actualPropValT.elementAt(1));
                } else {
                    compareScalarisJSON(expectedPropVal, actualPropVal,
                            actualOriginal);
                }
            } else {
                assertEquals("Checking value of property '" + expectedPropKey
                        + "' in " + actualOriginal, expectedPropVal,
                        actualPropVal);
            }
        }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}
     * writing a {@link JSONBeanTest1} and reading a {@link JSONBeanTest1}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean1b() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < 5000; ++i) {
            final JSONBeanTest1 bean1 = new JSONBeanTest1();
            bean1.setA(random.nextBoolean());
            bean1.setB(random.nextInt());
            bean1.setC(random.nextLong());
            bean1.setD(getRandomBigInt(random));
            bean1.setE(random.nextDouble());
            bean1.setF(getRandomString(random, random.nextInt(100), false));
            final ErlangValue value = new ErlangValue(bean1);

            final JSONBeanTest1 actual = value.jsonValue(JSONBeanTest1.class);
            assertEquals(bean1.getA(), actual.getA());
            assertEquals(bean1.getB(), actual.getB());
            assertEquals(bean1.getC(), actual.getC());
            assertEquals(bean1.getD(), actual.getD());
            assertEquals(bean1.getE(), actual.getE(), 0.0);
            assertEquals(bean1.getF(), actual.getF());
            compareScalarisJSON(value, new ErlangValue(actual));
        }
    }

    private static class JSONBeanTest2 {
        private boolean a2 = true;
        private int b2 = 0;
        private long c2 = 0;
        private BigInteger d2 = new BigInteger("0");
        private double e2 = 0.0;
        private String f2 = "";
        private List<Object> g2 = new ArrayList<Object>();
        private JSONBeanTest1 h2 = new JSONBeanTest1();
        private Map<String, Object> i2 = new LinkedHashMap<String, Object>();

        public JSONBeanTest2() {}

        public boolean getA2() { return a2; }
        public int getB2() { return b2; }
        public long getC2() { return c2; }
        public BigInteger getD2() { return d2; }
        public double getE2() { return e2; }
        public String getF2() { return f2; }
        public List<Object> getG2() { return g2; }
        public JSONBeanTest1 getH2() { return h2; }
        public Map<String, Object> getI2() { return i2; }

        public void setA2(final boolean a_) { this.a2 = a_; }
        public void setB2(final int b_) { this.b2 = b_; }
        public void setC2(final long c_) { this.c2 = c_; }
        public void setD2(final BigInteger d_) { this.d2 = d_; }
        public void setE2(final double e_) { this.e2 = e_; }
        public void setF2(final String f_) { this.f2 = f_; }
        public void setG2(final List<Object> g_) { this.g2 = g_; }
        public void setH2(final JSONBeanTest1 h_) { this.h2 = h_; }
        public void setI2(final Map<String, Object> i_) { this.i2 = i_; }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}
     * writing a {@link Map} and reading a {@link JSONBeanTest2}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean2a() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < 5000; ++i) {
            final Map<String, Object> map = new LinkedHashMap<String, Object>(7);
            map.put("a2", random.nextBoolean());
            map.put("b2", random.nextInt());
            map.put("c2", random.nextLong());
            map.put("d2", getRandomBigInt(random));
            map.put("e2", random.nextDouble());
            map.put("f2", getRandomString(random, random.nextInt(100), false));
            map.put("g2", getRandomList(random, 100));
            final Map<String, Object> bean1 = new LinkedHashMap<String, Object>(6);
            bean1.put("a", random.nextBoolean());
            bean1.put("b", random.nextInt());
            bean1.put("c", random.nextLong());
            bean1.put("d", getRandomBigInt(random));
            bean1.put("e", random.nextDouble());
            bean1.put("f", getRandomString(random, 10, false));
            map.put("h2", bean1);
            final Map<String, Object> map2 = getRandomMapRecursive(random, 10, 3, true);
            map.put("i2", map2);
            final ErlangValue value = new ErlangValue(map);

            final JSONBeanTest2 actual = value.jsonValue(JSONBeanTest2.class);
            assertEquals(map.get("a2"), actual.getA2());
            assertEquals(map.get("b2"), actual.getB2());
            assertEquals(map.get("c2"), actual.getC2());
            assertEquals(map.get("d2"), actual.getD2());
            assertEquals(map.get("e2"), actual.getE2());
            assertEquals(map.get("f2"), actual.getF2());
            assertEquals(map.get("g2"), actual.getG2());
            final JSONBeanTest1 bean1_act = actual.getH2();
            assertEquals(bean1.get("a"), bean1_act.getA());
            assertEquals(bean1.get("b"), bean1_act.getB());
            assertEquals(bean1.get("c"), bean1_act.getC());
            assertEquals(bean1.get("d"), bean1_act.getD());
            assertEquals(bean1.get("e"), bean1_act.getE());
            assertEquals(bean1.get("f"), bean1_act.getF());
            compareMap(map2, actual.getI2());
            compareScalarisJSON(value, new ErlangValue(actual));
        }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}
     * writing a {@link JSONBeanTest2} and reading a {@link JSONBeanTest2}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean2b() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < 5000; ++i) {
            final JSONBeanTest2 bean2 = new JSONBeanTest2();
            bean2.setA2(random.nextBoolean());
            bean2.setB2(random.nextInt());
            bean2.setC2(random.nextLong());
            bean2.setD2(getRandomBigInt(random));
            bean2.setE2(random.nextDouble());
            bean2.setF2(getRandomString(random, random.nextInt(100), false));
            bean2.setG2(getRandomList(random, 100));
            final JSONBeanTest1 bean1 = new JSONBeanTest1();
            bean1.setA(random.nextBoolean());
            bean1.setB(random.nextInt());
            bean1.setC(random.nextLong());
            bean1.setD(getRandomBigInt(random));
            bean1.setE(random.nextDouble());
            bean1.setF(getRandomString(random, 10, false));
            bean2.setH2(bean1);
            final Map<String, Object> map2 = getRandomMapRecursive(random, 10, 3, true);
            bean2.setI2(map2);
            final ErlangValue value = new ErlangValue(bean2);

            final JSONBeanTest2 actual = value.jsonValue(JSONBeanTest2.class);
            assertEquals(bean2.getA2(), actual.getA2());
            assertEquals(bean2.getB2(), actual.getB2());
            assertEquals(bean2.getC2(), actual.getC2());
            assertEquals(bean2.getD2(), actual.getD2());
            assertEquals(bean2.getE2(), actual.getE2(), 0.0);
            assertEquals(bean2.getF2(), actual.getF2());
            assertEquals(bean2.getG2(), actual.getG2());
            final JSONBeanTest1 bean1_act = actual.getH2();
            assertEquals(bean1.getA(), bean1_act.getA());
            assertEquals(bean1.getB(), bean1_act.getB());
            assertEquals(bean1.getC(), bean1_act.getC());
            assertEquals(bean1.getD(), bean1_act.getD());
            assertEquals(bean1.getE(), bean1_act.getE(), 0.0);
            assertEquals(bean1.getF(), bean1_act.getF());
            compareMap(map2, actual.getI2());
            compareScalarisJSON(value, new ErlangValue(actual));
        }
    }


    private static class JSONBeanTest3 {
        private List<JSONBeanTest1> a3 = new ArrayList<JSONBeanTest1>();
        private Map<String, JSONBeanTest1> b3 = new LinkedHashMap<String, JSONBeanTest1>();

        public JSONBeanTest3() {}

        public List<JSONBeanTest1> getA3() { return a3; }
        public Map<String, JSONBeanTest1> getB3() { return b3; }

        public void setA3(final List<JSONBeanTest1> a_) { this.a3 = a_; }
        public void setB3(final Map<String, JSONBeanTest1> b_) { this.b3 = b_; }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}
     * writing a {@link Map} and reading a {@link JSONBeanTest3}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean3a() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < 5000; ++i) {
            final Map<String, Object> map = new LinkedHashMap<String, Object>(7);
            final Map<String, Object> bean1a = new LinkedHashMap<String, Object>(6);
            bean1a.put("a", random.nextBoolean());
            bean1a.put("b", random.nextInt());
            bean1a.put("c", random.nextLong());
            bean1a.put("d", getRandomBigInt(random));
            bean1a.put("e", random.nextDouble());
            bean1a.put("f", getRandomString(random, 10, false));
            final Map<String, Object> bean1b = new LinkedHashMap<String, Object>(6);
            bean1b.put("a", random.nextBoolean());
            bean1b.put("b", random.nextInt());
            bean1b.put("c", random.nextLong());
            bean1b.put("d", getRandomBigInt(random));
            bean1b.put("e", random.nextDouble());
            bean1b.put("f", getRandomString(random, 10, false));
            final List<Map<String, Object>> list1 = new LinkedList<Map<String,Object>>();
            list1.add(bean1a);
            list1.add(bean1b);
            map.put("a3", list1);
            final Map<String, Map<String, Object>> map1 = new LinkedHashMap<String, Map<String,Object>>();
            map1.put("a4", bean1a);
            map1.put("b4", bean1b);
            map.put("b3", map1);
            final ErlangValue value = new ErlangValue(map);

            final JSONBeanTest3 actual = value.jsonValue(JSONBeanTest3.class);
            final List<JSONBeanTest1> actual_a3 = actual.getA3();
            assertEquals(list1.size(), actual_a3.size());
            for (int j = 0; j < list1.size(); ++j) {
                assertEquals(list1.get(j).get("a"), actual_a3.get(j).getA());
                assertEquals(list1.get(j).get("b"), actual_a3.get(j).getB());
                assertEquals(list1.get(j).get("c"), actual_a3.get(j).getC());
                assertEquals(list1.get(j).get("d"), actual_a3.get(j).getD());
                assertEquals(list1.get(j).get("e"), actual_a3.get(j).getE());
                assertEquals(list1.get(j).get("f"), actual_a3.get(j).getF());
            }
            final Map<String, JSONBeanTest1> actual_b3 = actual.getB3();
            assertEquals(map1.size(), actual_b3.size());
            for (final Entry<String, Map<String, Object>> key : map1.entrySet()) {
                assertEquals(key.getValue().get("a"), actual_b3.get(key.getKey()).getA());
                assertEquals(key.getValue().get("b"), actual_b3.get(key.getKey()).getB());
                assertEquals(key.getValue().get("c"), actual_b3.get(key.getKey()).getC());
                assertEquals(key.getValue().get("d"), actual_b3.get(key.getKey()).getD());
                assertEquals(key.getValue().get("e"), actual_b3.get(key.getKey()).getE());
                assertEquals(key.getValue().get("f"), actual_b3.get(key.getKey()).getF());
            }
            compareScalarisJSON(value, new ErlangValue(actual));
        }
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}
     * writing a {@link JSONBeanTest2} and reading a {@link JSONBeanTest2}.
     *
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean3b() throws Exception {
        final Random random = new Random();

        for (int i = 0; i < 10000; ++i) {
            final JSONBeanTest3 bean3 = new JSONBeanTest3();
            final JSONBeanTest1 bean1a = new JSONBeanTest1();
            bean1a.setA(random.nextBoolean());
            bean1a.setB(random.nextInt());
            bean1a.setC(random.nextLong());
            bean1a.setD(getRandomBigInt(random));
            bean1a.setE(random.nextDouble());
            bean1a.setF(getRandomString(random, 10, false));
            final JSONBeanTest1 bean1b = new JSONBeanTest1();
            bean1b.setA(random.nextBoolean());
            bean1b.setB(random.nextInt());
            bean1b.setC(random.nextLong());
            bean1b.setD(getRandomBigInt(random));
            bean1b.setE(random.nextDouble());
            bean1b.setF(getRandomString(random, 10, false));
            final List<JSONBeanTest1> list1 = new LinkedList<JSONBeanTest1>();
            list1.add(bean1a);
            list1.add(bean1b);
            bean3.setA3(list1);
            final Map<String, JSONBeanTest1> map1 = new LinkedHashMap<String, JSONBeanTest1>();
            map1.put("a4", bean1a);
            map1.put("b4", bean1b);
            bean3.setB3(map1);
            final ErlangValue value = new ErlangValue(bean3);

            final JSONBeanTest3 actual = value.jsonValue(JSONBeanTest3.class);
            final List<JSONBeanTest1> actual_a3 = actual.getA3();
            assertEquals(list1.size(), actual_a3.size());
            for (int j = 0; j < list1.size(); ++j) {
                assertEquals(list1.get(j).getA(), actual_a3.get(j).getA());
                assertEquals(list1.get(j).getB(), actual_a3.get(j).getB());
                assertEquals(list1.get(j).getC(), actual_a3.get(j).getC());
                assertEquals(list1.get(j).getD(), actual_a3.get(j).getD());
                assertEquals(list1.get(j).getE(), actual_a3.get(j).getE(), 0.0);
                assertEquals(list1.get(j).getF(), actual_a3.get(j).getF());
            }
            final Map<String, JSONBeanTest1> actual_b3 = actual.getB3();
            assertEquals(map1.size(), actual_b3.size());
            for (final Entry<String, JSONBeanTest1> key : map1.entrySet()) {
                assertEquals(key.getValue().getA(), actual_b3.get(key.getKey()).getA());
                assertEquals(key.getValue().getB(), actual_b3.get(key.getKey()).getB());
                assertEquals(key.getValue().getC(), actual_b3.get(key.getKey()).getC());
                assertEquals(key.getValue().getD(), actual_b3.get(key.getKey()).getD());
                assertEquals(key.getValue().getE(), actual_b3.get(key.getKey()).getE(), 0.0);
                assertEquals(key.getValue().getF(), actual_b3.get(key.getKey()).getF());
            }
            compareScalarisJSON(value, new ErlangValue(actual));
        }
    }
}
