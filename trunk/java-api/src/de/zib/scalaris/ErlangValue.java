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
import java.util.List;
import java.util.Map;

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangException;
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
 * @version 3.4
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
     * native types:
     * <ul>
     *  <li>{@link Boolean} - {@link OtpErlangBoolean}</li>
     *  <li>{@link Long} - {@link OtpErlangLong}</li>
     *  <li>{@link Integer} - {@link OtpErlangLong}</li>
     *  <li>{@link BigInteger} - {@link OtpErlangLong}</li>
     *  <li>{@link Double} - {@link OtpErlangDouble}</li>
     *  <li>{@link String} - {@link OtpErlangString}</li>
     *  <li><tt>byte[]</tt> - {@link OtpErlangBinary}</li>
     *  </ul>
     * composite types:
     * <ul>
     *  <li>{@link List}<Object> with one of the native types - {@link OtpErlangList}</li>
     *  <li>{@link Map}<String, Object> representing a JSON object - {@link OtpErlangTuple}</li>
     *  </ul>
     * custom types:
     * <ul>
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
    public static <T> OtpErlangObject convertToErlang(T value)
            throws ClassCastException {
        if (value instanceof Boolean) {
            return new OtpErlangBoolean((Boolean) value);
        } else if (value instanceof Integer) {
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
            ErlangValueJSONToMap json_converter = new ErlangValueJSONToMap();
            return json_converter.toScalarisJSON(map);
        } else if (value instanceof ErlangValue) {
            return ((ErlangValue) value).value();
        } else if (value instanceof OtpErlangObject) {
            return (OtpErlangObject) value;
        } else {
            // map to JSON object notation of Scalaris
            @SuppressWarnings("unchecked")
            ErlangValueJSONToBean<T> json_converter = new ErlangValueJSONToBean<T>((Class<T>) value.getClass());
            return json_converter.toScalarisJSON(value);
//            throw new ClassCastException("Unsupported type (value: " + value.toString() + ")");
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
     * 
     * @since 3.3
     */
    public boolean boolValue() throws ClassCastException {
        if (value.equals(CommonErlangObjects.falseAtom)) {
            return false;
        } else if (value.equals(CommonErlangObjects.trueAtom)) {
            return true;
        } else {
            throw new ClassCastException("No boolean.");
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
    public int intValue() throws ClassCastException {
        try {
            return ((OtpErlangLong) value).intValue();
        } catch (OtpErlangRangeException e) {
            throw new ClassCastException("Cannot cast to int - value is too big (use longValue() or bigIntValue() instead).");
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
    public long longValue() throws ClassCastException {
        OtpErlangLong longValue = (OtpErlangLong) value;
        if (longValue.isLong()) {
            return longValue.longValue();
        } else {
            throw new ClassCastException("Cannot cast to long - value is too big (use bigIntValue() instead).");
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
    public BigInteger bigIntValue() throws ClassCastException {
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
    public double doubleValue() throws ClassCastException {
        return ((OtpErlangDouble) value).doubleValue();
    }

    /**
     * Converts an {@link OtpErlangObject} to a {@link String} taking
     * special care of empty lists which can not be converted to strings using
     * the OTP library .
     * 
     * @param value
     *            the value to convert
     * 
     * @return the value as a String
     * 
     * @throws ClassCastException
     *             if the conversion fails
     */
    static String otpObjectToString(OtpErlangObject value)
            throws ClassCastException {
        // need special handling if OTP returned an empty list
        if (value instanceof OtpErlangList) {
            try {
                return new OtpErlangString((OtpErlangList) value).stringValue();
            } catch (OtpErlangException e) {
                throw new ClassCastException("com.ericsson.otp.erlang.OtpErlangList cannot be cast to com.ericsson.otp.erlang.OtpErlangString: " + e.getMessage());
            }
        } else {
            return ((OtpErlangString) value).stringValue();
        }
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
    public String stringValue() throws ClassCastException {
        return otpObjectToString(value);
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
    public byte[] binaryValue() throws ClassCastException {
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
    static OtpErlangList otpObjectToOtpList(OtpErlangObject value)
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
    public List<ErlangValue> listValue() throws ClassCastException {
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
    public List<Long> longListValue() throws ClassCastException {
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
    public List<Double> doubleListValue() throws ClassCastException {
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
    public List<String> stringListValue() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<String> result = new ArrayList<String>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(otpObjectToString(i));
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
    public List<byte[]> binaryListValue() throws ClassCastException {
        OtpErlangList list = otpObjectToOtpList(value);
        ArrayList<byte[]> result = new ArrayList<byte[]>(list.arity());
        for (OtpErlangObject i : list) {
            result.add(((OtpErlangBinary) i).binaryValue());
        }
        return result;
    }

    /**
     * Returns a JSON object (as {@link Map}<String, Object>) of the wrapped
     * erlang value.
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    public Map<String, Object> jsonValue() throws ClassCastException {
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
            ErlangValueJSONToMap json_converter = new ErlangValueJSONToMap();
            return json_converter.toJava((OtpErlangList) value_tpl.elementAt(1));
        } else {
            throw new ClassCastException("wrong tuple arity");
        }
    }

    /**
     * Returns a JSON object (as an instance of the given class) of the wrapped
     * erlang value.
     * 
     * @param <T>
     *            the type of the object to create
     * 
     * @param c
     *            the class of the created object
     * 
     * @return the converted value
     * 
     * @throws ClassCastException
     *             if thrown if a conversion is not possible, i.e. the type is
     *             not supported
     */
    public <T> T jsonValue(Class<T> c) throws ClassCastException {
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
            ErlangValueJSONToBean<T> json_converter = new ErlangValueJSONToBean<T>(c);
            return json_converter.toJava((OtpErlangList) value_tpl.elementAt(1));
        } else {
            throw new ClassCastException("wrong tuple arity");
        }
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
