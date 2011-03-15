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

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Encapsulates a result from a read operation on scalaris.
 * See {@link #ErlangValue(Object)} for a list of compatible types.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 3.0
 * @since 3.0
 */
public class ErlangValue {
    /**
     * The (internal representation of the) wrapped erlang value.
     */
    private OtpErlangObject value;
    
    /**
     * Creates a new object wrapping the given erlang value.
     * 
     * @param value  a value from erlang
     */
    public ErlangValue(OtpErlangObject value) {
        this.value = value;
    }
    
    /**
     * Creates a new object from a given set of Java types.
     * The following types are supported:
     * <ul>
     *  <li>{@link Long} - {@link OtpErlangLong}</li>
     *  <li>{@link Integer} - {@link OtpErlangLong}</li>
     *  <li>{@link BigInteger} - {@link OtpErlangLong}</li>
     *  <li>{@link Double} - {@link OtpErlangDouble}</li>
     *  <li>{@link String} - {@link OtpErlangString}</li>
     *  <li><tt>byte[]</tt> - {@link OtpErlangBinary}</li>
     *  <li>{@link List} with one of the supported types - {@link OtpErlangList}</li>
     *  <li>{@link Map}<String, Object> representing a JSON object - {@link OtpErlangTuple}</li>
     *  <li>{@link OtpErlangObject} - an arbitrary erlang value</li>
     *  <li>{@link ErlangValue}</li>
     *  </ul>
     * 
     * @param <T>    the type of the value
     * @param value  the value to convert to an erlang type
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public <T> ErlangValue(T value) throws ClassCastException {
        this.value = convertToErlang(value);
    }
    
    /**
     * Converts a (supported) Java type to an {@link OtpErlangObject}.
     * 
     * @param <T>    the type of the value
     * @param value  the value to convert to an erlang type
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    private static <T> OtpErlangObject convertToErlang(T value)
            throws ClassCastException {
        if (value instanceof Integer) {
            return new OtpErlangLong((Integer) value);
        } else if (value instanceof Long) {
            return new OtpErlangLong((Long) value);
        } else if (value instanceof BigInteger) {
            return new OtpErlangLong((BigInteger) value);
        } else if (value instanceof Double) {
            return new OtpErlangDouble((Double) value);
        } else if (value instanceof String) {
            return new OtpErlangString((String) value);
        } else if (value instanceof byte[]) {
            return new OtpErlangBinary((byte[]) value);
        } else if (value instanceof List<?>) {
            List<?> list = (List<?>) value;
            final int listSize = list.size();
            OtpErlangObject[] erlValue = new OtpErlangObject[listSize];
            int i = 0;
            // TODO: optimise for specific lists?
            for (Object iter : list) {
                erlValue[i] = convertToErlang(iter);
                ++i;
            }
            return new OtpErlangList(erlValue);
        } else if (value instanceof Map<?, ?>) {
            // map to JSON object notation of Scalaris
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            return convertJavaToScalarisJSON_object(map);
        } else if (value instanceof ErlangValue) {
            return ((ErlangValue) value).value();
        } else if (value instanceof OtpErlangObject) {
            return (OtpErlangObject) value;
        } else {
            throw new ClassCastException("Unsupported type (value: " + value.toString() + ")");
        }
    }

    /**
     * Returns the Java int value of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported or the value is too big
     */
    public int toInt() throws ClassCastException {
        try {
            return ((OtpErlangLong) value).intValue();
        } catch (OtpErlangRangeException e) {
            throw new ClassCastException("Cannot cast to int - value is too big (use toLong() or toBigInt() instead).");
        }
    }

    /**
     * Returns the Java long value of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported or the value is too big
     */
    public long toLong() throws ClassCastException {
        OtpErlangLong longValue = (OtpErlangLong) value;
        if (longValue.isLong()) {
            return longValue.longValue();
        } else {
            throw new ClassCastException("Cannot cast to long - value is too big (use toBigInt() instead).");
        }
    }

    /**
     * Returns the Java BigInteger value of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public BigInteger toBigInt() throws ClassCastException {
        return ((OtpErlangLong) value).bigIntegerValue();
    }

    /**
     * Returns the Java double value of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public double toDouble() throws ClassCastException {
        return ((OtpErlangDouble) value).doubleValue();
    }

    /**
     * Returns the Java {@link String} value of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public String toString() throws ClassCastException {
        // note: need special handling for empty strings:
        if (value instanceof OtpErlangList) {
            OtpErlangList value_list = (OtpErlangList) value;
            if (value_list.arity() == 0) {
                return "";
            } else {
                throw new ClassCastException("com.ericsson.otp.erlang.OtpErlangList cannot be cast to com.ericsson.otp.erlang.OtpErlangString");
            }
        } else {
            return ((OtpErlangString) value).stringValue();
        }
    }

    /**
     * Returns the Java byte[] value of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public byte[] toBinary() throws ClassCastException {
        return ((OtpErlangBinary) value).binaryValue();
    }

    /**
     * Converts an {@link OtpErlangObject} to a {@link OtpErlangList} taking
     * special care if the OTP library converted a list to an
     * {@link OtpErlangString}.
     * 
     * @param value
     *            the value to convert
     * 
     * @return the value as a OtpErlangList
     * 
     * @throws ClassCastException
     *             if the conversion fails
     */
    private static OtpErlangList otpObjectToOtpList(OtpErlangObject value)
            throws ClassCastException {
        // need special handling if OTP thought that the value is a string
        if (value instanceof OtpErlangString) {
            OtpErlangString value_string = (OtpErlangString) value;
            return new OtpErlangList(value_string.stringValue());
        } else {
            return (OtpErlangList) value;
        }
    }

    /**
     * Returns a list of mixed Java values (wrapped in {@link ErlangValue}
     * objects) of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public List<ErlangValue> toList() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<ErlangValue> result = new ArrayList<ErlangValue>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(new ErlangValue(i));
        }
        return result;
    }

    /**
     * Returns a list of {@link Long} values of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public List<Long> toLongList() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<Long> result = new ArrayList<Long>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(((OtpErlangLong) i).longValue());
        }
        return result;
    }

    /**
     * Returns a list of {@link Double} values of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public List<Double> toDoubleList() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<Double> result = new ArrayList<Double>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(((OtpErlangDouble) i).doubleValue());
        }
        return result;
    }

    /**
     * Returns a list of {@link String} values of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public List<String> toStringList() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<String> result = new ArrayList<String>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(((OtpErlangString) i).stringValue());
        }
        return result;
    }

    /**
     * Returns a list of <tt>byte[]</tt> values of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public List<byte[]> toBinaryList() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<byte[]> result = new ArrayList<byte[]>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(((OtpErlangBinary) i).binaryValue());
        }
        return result;
    }

    /**
     * Returns a JSON object (as {@link String}) of the wrapped erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public Map<String, Object> toJSON() throws ClassCastException {
        /*
         * object(): {struct, [{key::string() | atom(), value()}]}
         * array():  {array, [value()]}
         * value():  number(), string(), object(), array(), 'true', 'false', 'null'
         * 
         * first term must be an object!
         */
        OtpErlangTuple value_tpl = (OtpErlangTuple) value;
        if (value_tpl.arity() == 2
                && value_tpl.elementAt(0).equals(CommonErlangObjects.structAtom)) {
            return convertScalarisJSONtoJava_object((OtpErlangList) value_tpl.elementAt(1));
        } else {
            throw new ClassCastException("wrong tuple arity");
        }
    }
    
    /**
     * Converts an unknown JSON value to a Java object.
     * 
     * @param value
     *            a JSON value as stored by Scalaris' JSON API
     * 
     * @return a Java object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    private static Object convertScalarisJSONtoJava_value(OtpErlangObject value)
            throws ClassCastException {
        if (value instanceof OtpErlangLong) {
            OtpErlangLong value_int = (OtpErlangLong) value;
            try {
                return value_int.intValue();
            } catch (OtpErlangRangeException e) {
                if (value_int.isLong()) {
                    return value_int.longValue();
                } else {
                    return value_int.bigIntegerValue();
                }
            }
        } else if (value instanceof OtpErlangDouble) {
            return ((OtpErlangDouble) value).doubleValue();
        } else if (value instanceof OtpErlangString) {
            return ((OtpErlangString) value).stringValue();
        } else if (value instanceof OtpErlangTuple) {
            OtpErlangTuple value_tpl = (OtpErlangTuple) value;
            if (value_tpl.arity() == 2) {
                OtpErlangObject tag = value_tpl.elementAt(0);
                if (tag.equals(CommonErlangObjects.structAtom)) {
                    return convertScalarisJSONtoJava_object((OtpErlangList) value_tpl
                            .elementAt(1));
                } else if (tag.equals(CommonErlangObjects.arrayAtom)) {
                    return convertScalarisJSONtoJava_array(
                            otpObjectToOtpList(value_tpl.elementAt(1)));
                } else {
                    throw new ClassCastException("unknown JSON tag");
                }
            } else {
                throw new ClassCastException("wrong tuple arity");
            }
        } else if (value.equals(CommonErlangObjects.trueAtom)) {
            return true;
        } else if (value.equals(CommonErlangObjects.falseAtom)) {
            return false;
        } else if (value.equals(CommonErlangObjects.nullAtom)) {
            return null;
        } else {
            throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
        }
    }

    /**
     * Converts a Java object to a JSON value as expected by Scalaris.
     * 
     * @param value
     *            a Java object supported by JSON
     * 
     * @return a JSON object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    private static OtpErlangObject convertJavaToScalarisJSON_value(Object value)
            throws ClassCastException {
        if (value == null) {
            return CommonErlangObjects.nullAtom;
        } else if (value instanceof Integer) {
            return new OtpErlangInt((Integer) value);
        } else if (value instanceof Long) {
            return new OtpErlangLong((Long) value);
        } else if (value instanceof BigInteger) {
            return new OtpErlangLong((BigInteger) value);
        } else if (value instanceof Double) {
            return new OtpErlangDouble((Double) value);
        } else if (value instanceof String) {
            return new OtpErlangString((String) value);
        } else if (value instanceof List<?>) {
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) value;
            return convertJavaToScalarisJSON_array(list);
        } else if (value instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> map = (Map<String, Object>) value;
            return convertJavaToScalarisJSON_object(map);
        } else if (value.equals(true)) {
            return CommonErlangObjects.trueAtom;
        } else if (value.equals(false)) {
            return CommonErlangObjects.falseAtom;
        } else {
            throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
        }
    }

    /**
     * Converts a JSON object value (a list of key/value pairs) to a Java Map.
     * 
     * @param value
     *            a list of key/value pairs with JSON values and string keys as
     *            stored by Scalaris' JSON API
     * 
     * @return a Java object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    private static Map<String, Object> convertScalarisJSONtoJava_object(
            OtpErlangList value) throws ClassCastException {
        Map<String, Object> result = new LinkedHashMap<String, Object>(
                value.arity());
        for (OtpErlangObject iter : value) {
            OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
            if (iter_tpl.arity() == 2) {
                OtpErlangObject key_erl = iter_tpl.elementAt(0);
                String key;
                if (key_erl instanceof OtpErlangAtom) {
                    key = ((OtpErlangAtom) key_erl).atomValue();
                } else if (key_erl instanceof OtpErlangString) {
                    key = ((OtpErlangString) key_erl).stringValue();
                } else {
                    throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
                }
                result.put(key,
                        convertScalarisJSONtoJava_value(iter_tpl.elementAt(1)));
            } else {
                throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
            }
        }
        return result;
    }

    /**
     * Converts a Java Map to a JSON object as expected by Scalaris.
     * 
     * @param value
     *            a Java Map with String-keys and supported JSON types as values
     * 
     * @return a JSON object representing the value
     * 
     * @throws ClassCastException
     *             if thrown if a conversion is not possible, i.e. the type is
     *             not supported
     */
    private static OtpErlangTuple convertJavaToScalarisJSON_object(
            Map<String, Object> value) throws ClassCastException {
        OtpErlangTuple[] resultList = new OtpErlangTuple[value.size()];
        int i = 0;

        for (Map.Entry<String, Object> entry : value.entrySet()) {
            resultList[i] = new OtpErlangTuple(new OtpErlangObject[] {
                    new OtpErlangString(entry.getKey()),
                    convertJavaToScalarisJSON_value(entry.getValue()) });
            ++i;
        }
        OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.structAtom, new OtpErlangList(resultList) });
        return resultTpl;
    }

    /**
     * Converts a JSON array value (a list of values) to a Java List.
     * 
     * @param value
     *            a list of JSON values as stored by Scalaris' JSON API
     * 
     * @return a Java object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    private static List<Object> convertScalarisJSONtoJava_array(
            OtpErlangList value) throws ClassCastException {
        List<Object> result = new ArrayList<Object>(value.arity());
        for (OtpErlangObject iter : value) {
            result.add(convertScalarisJSONtoJava_value(iter));
        }
        return result;
    }

    /**
     * Converts a list of Java Objects to a JSON array value as expected by
     * Scalaris.
     * 
     * @param value
     *            a list of supported JSON values
     * 
     * @return a JSON object representing the value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    private static OtpErlangTuple convertJavaToScalarisJSON_array(
            List<Object> value) throws ClassCastException {
        OtpErlangObject[] resultList = new OtpErlangObject[value.size()];
        int i = 0;
        for (Object iter : value) {
            resultList[i] = convertJavaToScalarisJSON_value(iter);
            ++i;
        }
        OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.arrayAtom, new OtpErlangList(resultList) });
        return resultTpl;
    }

    /**
     * Gets the original erlang value.
     * 
     * @return the value as reported by erlang
     */
    public OtpErlangObject value() {
        return value;
    }
}
