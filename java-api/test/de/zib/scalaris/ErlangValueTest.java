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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;

import org.junit.Test;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Unit tests for {@link ErlangValue}.
 * 
 * TODO: implement tests verifying that the expected exceptions are thrown for
 * unsupported conversions
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ErlangValueTest {
    
    private static BigInteger getRandomBigInt(Random random) {
        return BigInteger.valueOf(random.nextLong()).multiply(BigInteger.valueOf(random.nextLong()));
    }
    
    private static byte[] getRandomBytes(Random random, int size) {
        byte[] bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }
    
    private static String getRandomString(Random random, int length, boolean onlyChars) {
        if (onlyChars) {
            return getRandomCharString(random, length);
        } else {
            return new String(getRandomBytes(random, length));
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
    
    private static String getRandomCharString(Random random, int length) {
        if(length > 0) {
            char[] result = new char[length];
            // lets always start with a character:
            result[0] = chars[random.nextInt(chars.length)];
            for (int i = 1; i < result.length; ++i) {
                result[0] = symbols[random.nextInt(symbols.length)];
            }
            return new String(result);
        }
        return "";
    }
    
    private static List<Object> getRandomList(Random random, int capacity) {
        // note: we do not generate (recursive) maps -> so there won't be keys to worry about
        return getRandomListRecursive(random, capacity, 0, false);
    }
    
    private static List<Object> getRandomListRecursive(Random random, int capacity, int maxDepth, boolean mapKeyOnlyChars) {
        List<Object> currentList = null;
        currentList = new ArrayList<Object>(capacity);
        int curMaxDepth = maxDepth == 0 ? 0 : random.nextInt(maxDepth);
        int maxType = curMaxDepth == 0 ? 6 : 8;
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
    
    private static Map<String, Object> getRandomMapRecursive(Random random, int capacity, int maxDepth, boolean keyOnlyChars) {
        Map<String, Object> currentMap = null;
        currentMap = new LinkedHashMap<String, Object>(capacity);
        for (int i = 0; i < capacity; ++i) {
            // key:
            String key = getRandomString(random, random.nextInt(10), keyOnlyChars);
            // value:
            int curMaxDepth = maxDepth == 0 ? 0 : random.nextInt(maxDepth);
            int maxType = curMaxDepth == 0 ? 6 : 8;
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
        ErlangValue trueVal = new ErlangValue(true);
        ErlangValue trueValOtp = new ErlangValue(new OtpErlangBoolean(true));
        ErlangValue falseVal = new ErlangValue(false);
        ErlangValue falseValOtp = new ErlangValue(new OtpErlangBoolean(false));
        
        assertEquals(true, trueVal.boolValue());
        assertEquals(true, trueValOtp.boolValue());
        assertEquals(false, falseVal.boolValue());
        assertEquals(false, falseValOtp.boolValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#intValue()}.
     * 
     * @throws Exception if a test with a random integer failed
     */
    @Test
    public final void testIntValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Integer currentInt = null;
            try {
                currentInt = random.nextInt();
                testIntValue(currentInt);
            } catch (ClassCastException e) {
                throw new Exception("testIntValue(" + currentInt + ") failed", e);
            }
        }
    }
    
    private final void testIntValue(int value) {
        ErlangValue eVal = new ErlangValue(value);
        ErlangValue eValOtp = new ErlangValue(new OtpErlangInt(value));
        
        assertEquals(value, eVal.intValue());
        assertEquals(value, eValOtp.intValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#longValue()}.
     * 
     * @throws Exception if a test with a random long failed
     */
    @Test
    public final void testLongValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Long currentLong = null;
            try {
                currentLong = random.nextLong();
                testLongValue(currentLong);
            } catch (ClassCastException e) {
                throw new Exception("testLongValue(" + currentLong + ") failed", e);
            }
        }
    }
    
    private final void testLongValue(long value) {
        ErlangValue eVal = new ErlangValue(value);
        ErlangValue eValOtp = new ErlangValue(new OtpErlangLong(value));
        
        assertEquals(value, eVal.longValue());
        assertEquals(value, eValOtp.longValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#bigIntValue()}.
     * 
     * @throws Exception if a test with a random big integer failed
     */
    @Test
    public final void testBigIntValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            BigInteger currentBigInt = null;
            try {
                currentBigInt = getRandomBigInt(random);
                testBigIntValue(currentBigInt);
            } catch (ClassCastException e) {
                throw new Exception("testBigIntValue(" + currentBigInt + ") failed", e);
            }
        }
    }
    
    private final void testBigIntValue(BigInteger value) {
        ErlangValue eVal = new ErlangValue(value);
        ErlangValue eValOtp = new ErlangValue(new OtpErlangLong(value));
        
        assertEquals(value, eVal.bigIntValue());
        assertEquals(value, eValOtp.bigIntValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#doubleValue()}.
     * 
     * @throws Exception if a test with a random double failed
     */
    @Test
    public final void testDoubleValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Double currentDouble = null;
            try {
                currentDouble = random.nextDouble();
                testDoubleValue(currentDouble);
            } catch (ClassCastException e) {
                throw new Exception("testDoubleValue(" + currentDouble + ") failed", e);
            }
        }
    }
    
    private final void testDoubleValue(double value) {
        ErlangValue eVal = new ErlangValue(value);
        ErlangValue eValOtp = new ErlangValue(new OtpErlangDouble(value));
        
        assertEquals(value, eVal.doubleValue(), 0.0);
        assertEquals(value, eValOtp.doubleValue(), 0.0);
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#stringValue()}.
     * 
     * @throws Exception if a test with a random double failed
     */
    @Test
    public final void testStringValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            String currentString = null;
            try {
                currentString = getRandomString(random, random.nextInt(1000), false);
                testStringValue(currentString);
            } catch (ClassCastException e) {
                throw new Exception("testStringValue(" + currentString + ") failed", e);
            }
        }
    }
    
    private final void testStringValue(String value) {
        ErlangValue eVal = new ErlangValue(value);
        ErlangValue eValOtp = new ErlangValue(new OtpErlangString(value));
        
        assertEquals(value, eVal.stringValue());
        assertEquals(value, eValOtp.stringValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#binaryValue()}.
     * 
     * @throws Exception if a test with a random byte array failed
     */
    @Test
    public final void testBinaryValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 5000; ++i) {
            try {
                testBinaryValue(getRandomBytes(random, random.nextInt(1000)));
            } catch (ClassCastException e) {
                // do not print generated bytes (probably not useful)
                throw new Exception("testBinaryValue(...) failed", e);
            }
        }
    }
    
    private final void testBinaryValue(byte[] value) {
        ErlangValue eVal = new ErlangValue(value);
        ErlangValue eValOtp = new ErlangValue(new OtpErlangBinary(value));
        
        assertArrayEquals(value, eVal.binaryValue());
        assertArrayEquals(value, eValOtp.binaryValue());
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
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<Object> currentList = null;
            try {
                int capacity = random.nextInt(1000);
                currentList = getRandomList(random, capacity);
                testListValue(currentList);
            } catch (ClassCastException e) {
                throw new Exception("testListValue(" + currentList + ") failed", e);
            }
        }
    }
    
    private final void testListValue(List<Object> value) {
        ErlangValue eVal = new ErlangValue(value);
        OtpErlangObject[] valueOtp = new OtpErlangObject[value.size()];
        int i = 0;
        for (Iterator<Object> iterator = value.iterator(); iterator.hasNext();) {
            Object value_i = iterator.next();
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
        ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        compareList(value, eVal.listValue());
        compareList(value, eValOtp.listValue());
    }
    
    private static void compareList(List<Object> expected, List<ErlangValue> actual) {
        assertEquals(expected.size(), actual.size());
        for (int j = 0; j < actual.size(); ++j) {
            Object expected_j = expected.get(j);
            ErlangValue actual_j = actual.get(j);
            if (expected_j instanceof Boolean) {
                assertEquals((Boolean) expected_j, actual_j.boolValue());
            } else if (expected_j instanceof Integer) {
                assertEquals((Integer) expected_j, new Integer(actual_j.intValue()));
            } else if (expected_j instanceof Long) {
                assertEquals((Long) expected_j, new Long(actual_j.longValue()));
            } else if (expected_j instanceof BigInteger) {
                assertEquals((BigInteger) expected_j, actual_j.bigIntValue());
            } else if (expected_j instanceof Double) {
                assertEquals((Double) expected_j, new Double(actual_j.doubleValue()));
            } else if (expected_j instanceof String) {
                assertEquals((String) expected_j, actual_j.stringValue());
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
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<Long> currentList = null;
            try {
                int capacity = random.nextInt(1000);
                currentList = new ArrayList<Long>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(random.nextLong());
                }
                testLongListValue(currentList);
            } catch (ClassCastException e) {
                throw new Exception("testLongListValue(" + currentList + ") failed", e);
            }
        }
    }
    
    private final void testLongListValue(List<Long> value) {
        ErlangValue eVal = new ErlangValue(value);
        OtpErlangLong[] valueOtp = new OtpErlangLong[value.size()];
        int i = 0;
        for (Iterator<Long> iterator = value.iterator(); iterator.hasNext();) {
            Long long1 = (Long) iterator.next();
            valueOtp[i++] = new OtpErlangLong(long1);
        }
        ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));
        
        assertEquals(value, eVal.longListValue());
        assertEquals(value, eValOtp.longListValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#doubleListValue()}.
     * 
     * @throws Exception if a test with a random list of doubles failed
     */
    @Test
    public final void testDoubleListValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<Double> currentList = null;
            try {
                int capacity = random.nextInt(1000);
                currentList = new ArrayList<Double>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(random.nextDouble());
                }
                testDoubleListValue(currentList);
            } catch (ClassCastException e) {
                throw new Exception("testDoubleListValue(" + currentList + ") failed", e);
            }
        }
    }
    
    private final void testDoubleListValue(List<Double> value) {
        ErlangValue eVal = new ErlangValue(value);
        OtpErlangDouble[] valueOtp = new OtpErlangDouble[value.size()];
        int i = 0;
        for (Iterator<Double> iterator = value.iterator(); iterator.hasNext();) {
            Double double1 = (Double) iterator.next();
            valueOtp[i++] = new OtpErlangDouble(double1);
        }
        ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));
        
        assertEquals(value, eVal.doubleListValue());
        assertEquals(value, eValOtp.doubleListValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#stringListValue()}.
     * 
     * @throws Exception if a test with a random list of strings failed
     */
    @Test
    public final void testStringListValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            List<String> currentList = null;
            try {
                int capacity = random.nextInt(10);
                currentList = new ArrayList<String>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(getRandomString(random, random.nextInt(1000), false));
                }
                testStringListValue(currentList);
            } catch (ClassCastException e) {
                throw new Exception("testStringListValue(" + currentList + ") failed", e);
            }
        }
    }
    
    private final void testStringListValue(List<String> value) {
        ErlangValue eVal = new ErlangValue(value);
        OtpErlangString[] valueOtp = new OtpErlangString[value.size()];
        int i = 0;
        for (Iterator<String> iterator = value.iterator(); iterator.hasNext();) {
            String string1 = (String) iterator.next();
            valueOtp[i++] = new OtpErlangString(string1);
        }
        ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));
        
        assertEquals(value, eVal.stringListValue());
        assertEquals(value, eValOtp.stringListValue());
    }

    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#binaryListValue()}.
     * 
     * @throws Exception if a test with a random list of binaries failed
     */
    @Test
    public final void testBinaryListValue() throws Exception {
        Random random = new Random();
        for (int i = 0; i < 1000; ++i) {
            try {
                int capacity = random.nextInt(10);
                List<byte[]> currentList = new ArrayList<byte[]>(capacity);
                for (int j = 0; j < capacity; ++j) {
                    currentList.add(getRandomBytes(random, random.nextInt(1000)));
                }
                testBinaryListValue(currentList);
            } catch (ClassCastException e) {
                // do not print generated bytes (probably not useful)
                throw new Exception("testBinaryListValue(...) failed", e);
            }
        }
    }
    
    private final void testBinaryListValue(List<byte[]> value) {
        ErlangValue eVal = new ErlangValue(value);
        OtpErlangBinary[] valueOtp = new OtpErlangBinary[value.size()];
        int i = 0;
        for (Iterator<byte[]> iterator = value.iterator(); iterator.hasNext();) {
            byte[] binary1 = (byte[]) iterator.next();
            valueOtp[i++] = new OtpErlangBinary(binary1);
        }
        ErlangValue eValOtp = new ErlangValue(new OtpErlangList(valueOtp));

        List<byte[]> actual = eVal.binaryListValue();
        List<byte[]> actualOtp = eValOtp.binaryListValue();
        assertEquals(value.size(), actual.size());
        assertEquals(value.size(), actualOtp.size());
        for (int j = 0; j < actual.size(); ++j) {
            assertArrayEquals(value.get(j), actual.get(j));
            assertArrayEquals(value.get(j), actualOtp.get(j));
        }
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
        Random random = new Random();
        for (int i = 0; i < 10000; ++i) {
            Map<String, Object> currentMap = null;
            try {
                currentMap = getRandomMapRecursive(random, random.nextInt(10), 3, false);
                testJsonValue(currentMap);
            } catch (ClassCastException e) {
                throw new Exception("testJsonValue(" + currentMap + ") failed", e);
            }
        }
    }
    
    private final void testJsonValue(Map<String, Object> value) {
        ErlangValue eVal = new ErlangValue(value);
        Map<String, Object> actual = eVal.jsonValue();
        compareMap(value, actual);
    }
    
    private final void compareMap(Map<String, Object> expected, Map<String, Object> actual) {
        assertEquals(expected.size(), actual.size());
        for (Entry<String, Object> entry : expected.entrySet()) {
            String expected_key_i = entry.getKey();
            Object expected_value_i = entry.getValue();

            assertTrue(actual.containsKey(expected_key_i));
            Object actual_value_i = actual.get(expected_key_i);
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
        
        public void setA(boolean a_) { this.a = a_; }
        public void setB(int b_) { this.b = b_; }
        public void setC(long c_) { this.c = c_; }
        public void setD(BigInteger d_) { this.d = d_; }
        public void setE(double e_) { this.e = e_; }
        public void setF(String f_) { this.f = f_; }
    }
    
    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}.
     * 
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean1() throws Exception {
        Random random = new Random();

        for (int i = 0; i < 5000; ++i) {
            Map<String, Object> map = new LinkedHashMap<String, Object>();
            map.put("a", random.nextBoolean());
            map.put("b", random.nextInt());
            map.put("c", random.nextLong());
            map.put("d", getRandomBigInt(random));
            map.put("e", random.nextDouble());
            map.put("f", getRandomString(random, random.nextInt(100), false));
            ErlangValue value = new ErlangValue(map);

            JSONBeanTest1 actual = value.jsonValue(JSONBeanTest1.class);
            assertEquals(map.get("a"), actual.getA());
            assertEquals(map.get("b"), actual.getB());
            assertEquals(map.get("c"), actual.getC());
            assertEquals(map.get("d"), actual.getD());
            assertEquals(map.get("e"), actual.getE());
            assertEquals(map.get("f"), actual.getF());
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
        
        public void setA2(boolean a_) { this.a2 = a_; }
        public void setB2(int b_) { this.b2 = b_; }
        public void setC2(long c_) { this.c2 = c_; }
        public void setD2(BigInteger d_) { this.d2 = d_; }
        public void setE2(double e_) { this.e2 = e_; }
        public void setF2(String f_) { this.f2 = f_; }
        public void setG2(List<Object> g_) { this.g2 = g_; }
        public void setH2(JSONBeanTest1 h_) { this.h2 = h_; }
        public void setI2(Map<String, Object> i_) { this.i2 = i_; }
    }
    
    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}.
     * 
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean2a() throws Exception {
        Random random = new Random();

        for (int i = 0; i < 10000; ++i) {
            Map<String, Object> map = new LinkedHashMap<String, Object>(7);
            map.put("a2", random.nextBoolean());
            map.put("b2", random.nextInt());
            map.put("c2", random.nextLong());
            map.put("d2", getRandomBigInt(random));
            map.put("e2", random.nextDouble());
            map.put("f2", getRandomString(random, random.nextInt(100), false));
            map.put("g2", getRandomList(random, 100));
            Map<String, Object> bean1 = new LinkedHashMap<String, Object>(5);
            bean1.put("a", random.nextBoolean());
            bean1.put("b", random.nextInt());
            bean1.put("c", random.nextLong());
            bean1.put("d", getRandomBigInt(random));
            bean1.put("e", random.nextDouble());
            bean1.put("f", getRandomString(random, 10, false));
            map.put("h2", bean1);
            Map<String, Object> map2 = getRandomMapRecursive(random, 10, 3, true);
            map.put("i2", map2);
            ErlangValue value = new ErlangValue(map);

            JSONBeanTest2 actual = value.jsonValue(JSONBeanTest2.class);
            assertEquals(map.get("a2"), actual.getA2());
            assertEquals(map.get("b2"), actual.getB2());
            assertEquals(map.get("c2"), actual.getC2());
            assertEquals(map.get("d2"), actual.getD2());
            assertEquals(map.get("e2"), actual.getE2());
            assertEquals(map.get("f2"), actual.getF2());
            assertEquals(map.get("g2"), actual.getG2());
            JSONBeanTest1 bean1_act = actual.getH2();
            assertEquals(bean1.get("a"), bean1_act.getA());
            assertEquals(bean1.get("b"), bean1_act.getB());
            assertEquals(bean1.get("c"), bean1_act.getC());
            assertEquals(bean1.get("d"), bean1_act.getD());
            assertEquals(bean1.get("e"), bean1_act.getE());
            assertEquals(bean1.get("f"), bean1_act.getF());
            compareMap(map2, actual.getI2());
        }
    }
    
    /**
     * Test method for {@link de.zib.scalaris.ErlangValue#jsonValue(Class)}.
     * 
     * @throws Exception
     *             if a test with a random list of mixed objects (bool, int,
     *             long, BigInteger, double, String) failed
     */
    @Test
    public final void testJsonValueBean2b() throws Exception {
        Random random = new Random();

        for (int i = 0; i < 10000; ++i) {
            JSONBeanTest2 bean2 = new JSONBeanTest2();
            bean2.setA2(random.nextBoolean());
            bean2.setB2(random.nextInt());
            bean2.setC2(random.nextLong());
            bean2.setD2(getRandomBigInt(random));
            bean2.setE2(random.nextDouble());
            bean2.setF2(getRandomString(random, random.nextInt(100), false));
            bean2.setG2(getRandomList(random, 100));
            JSONBeanTest1 bean1 = new JSONBeanTest1();
            bean1.setA(random.nextBoolean());
            bean1.setB(random.nextInt());
            bean1.setC(random.nextLong());
            bean1.setD(getRandomBigInt(random));
            bean1.setE(random.nextDouble());
            bean1.setF(getRandomString(random, 10, false));
            bean2.setH2(bean1);
            Map<String, Object> map2 = getRandomMapRecursive(random, 10, 3, true);
            bean2.setI2(map2);
            ErlangValue value = new ErlangValue(bean2);

            JSONBeanTest2 actual = value.jsonValue(JSONBeanTest2.class);
            assertEquals(bean2.getA2(), actual.getA2());
            assertEquals(bean2.getB2(), actual.getB2());
            assertEquals(bean2.getC2(), actual.getC2());
            assertEquals(bean2.getD2(), actual.getD2());
            assertEquals(bean2.getE2(), actual.getE2(), 0.0);
            assertEquals(bean2.getF2(), actual.getF2());
            assertEquals(bean2.getG2(), actual.getG2());
            JSONBeanTest1 bean1_act = actual.getH2();
            assertEquals(bean1.getA(), bean1_act.getA());
            assertEquals(bean1.getB(), bean1_act.getB());
            assertEquals(bean1.getC(), bean1_act.getC());
            assertEquals(bean1.getD(), bean1_act.getD());
            assertEquals(bean1.getE(), bean1_act.getE(), 0.0);
            assertEquals(bean1.getF(), bean1_act.getF());
            compareMap(map2, actual.getI2());
        }
    }

}
