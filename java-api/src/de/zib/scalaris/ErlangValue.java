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

import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Encapsulates a result from a read operation on scalaris.
 * See {@link #ErlangValue(Object)} for a list of compatible types.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.9
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
     *  <li>{@link OtpErlangObject}</li>
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
    private static <T> OtpErlangObject convertToErlang(T value) throws ClassCastException {
        OtpErlangObject result;
        if (value instanceof Long) {
            result = new OtpErlangLong((Long) value);
        } else if (value instanceof Integer) {
            result = new OtpErlangLong((Integer) value);
        } else if (value instanceof BigInteger) {
            result = new OtpErlangLong((BigInteger) value);
        } else if (value instanceof Double) {
            result = new OtpErlangDouble((Double) value);
        } else if (value instanceof String) {
            result = new OtpErlangString((String) value);
        } else if (value instanceof byte[]) {
            result = new OtpErlangBinary((byte[]) value);
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
            result = new OtpErlangList(erlValue);
        } else if (value instanceof ErlangValue) {
            result = ((ErlangValue) value).value();
        } else if (value instanceof OtpErlangObject) {
            result = (OtpErlangObject) value;
        } else {
            throw new ClassCastException("Unsupported type");
        }
        return result;
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
    public long toInt() throws ClassCastException {
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
        // TODO: remove '$s' here, or in erlang?
        return ((OtpErlangString) value).stringValue();
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
        OtpErlangList list = (OtpErlangList) value;
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
        OtpErlangList list = (OtpErlangList) value;
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
        OtpErlangList list = (OtpErlangList) value;
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
        // TODO: tagged strings here as well?
        OtpErlangList list = (OtpErlangList) value;
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
        OtpErlangList list = (OtpErlangList) value;
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
    public String toJSON() throws ClassCastException {
        // TODO: is JSON stored as a string, internally?
        return ((OtpErlangString) value).stringValue();
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
