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
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Base class for Scalaris-JSON to Java and Java to Scalaris-JSON converters.
 *
 * Provides common methods for sub-classes.
 *
 * @author Nico Kruber, kruber@zib.de
 */
abstract class ErlangValueJSONBase {
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
    protected abstract OtpErlangTuple convertJavaToScalarisJSON_object(
            Object value) throws ClassCastException;

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
    protected OtpErlangObject convertJavaToScalarisJSON_value(final Object value)
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
            final
            List<Object> list = (List<Object>) value;
            return convertJavaToScalarisJSON_array(list);
        } else if (value.equals(true)) {
            return CommonErlangObjects.trueAtom;
        } else if (value.equals(false)) {
            return CommonErlangObjects.falseAtom;
        } else {
            return convertJavaToScalarisJSON_object(value);
        }
//            throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
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
    protected OtpErlangTuple convertJavaToScalarisJSON_array(
            final List<Object> value) throws ClassCastException {
        final OtpErlangObject[] resultList = new OtpErlangObject[value.size()];
        int i = 0;
        for (final Object iter : value) {
            resultList[i] = convertJavaToScalarisJSON_value(iter);
            ++i;
        }
        final OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.arrayAtom, new OtpErlangList(resultList) });
        return resultTpl;
    }

    /**
     * Converts an unknown JSON value to a Java object (no composite types!).
     *
     * @param value
     *            a non-composite JSON value as stored by Scalaris' JSON API,
     *            i.e. string, number, true, false, null
     *
     * @return a Java object representing the value
     *
     * @throws ClassCastException
     *                if thrown if a conversion is not possible, i.e. the type
     *                is not supported
     */
    protected Object convertScalarisJSONtoJava_value_simple(final OtpErlangObject value)
            throws ClassCastException {
        if (value instanceof OtpErlangLong) {
            final OtpErlangLong value_int = (OtpErlangLong) value;
            try {
                return value_int.intValue();
            } catch (final OtpErlangRangeException e) {
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
        } else if (value instanceof OtpErlangList) {
            try {
                return ErlangValue.otpObjectToString(value);
            } catch (final ClassCastException e) {
                throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
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
     * Converts the given Scalaris erlang object (as part of an object's key) to
     * a string.
     *
     * @param key_erl
     *            the erlang object containing the key
     *
     * @return the key
     *
     * @throws ClassCastException
     *             if the key is not a valid key object
     */
    protected String convertScalarisJSONtoJava_key(
            final OtpErlangObject key_erl) throws ClassCastException {
        if (key_erl instanceof OtpErlangAtom) {
            return ((OtpErlangAtom) key_erl).atomValue();
        } else {
            try {
                return ErlangValue.otpObjectToString(key_erl);
            } catch (final ClassCastException e) {
                throw new ClassCastException("Unsupported JSON key ("
                        + key_erl.toString() + "): " + e.getMessage());
            }
        }
    }
}
