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
 * @param <T> the class to convert to/from
 * 
 * @author Nico Kruber, kruber@zib.de
 */
abstract class ErlangValueJSONBase<T> {

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
    public OtpErlangTuple toScalarisJSON(T value) throws ClassCastException {
        return convertJavaToScalarisJSON_object(value);
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
    @SuppressWarnings("unchecked")
    public T toJava(OtpErlangList value) throws ClassCastException {
        return (T) convertScalarisJSONtoJava_object(value);
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
    protected abstract OtpErlangTuple convertJavaToScalarisJSON_object(
            Object value) throws ClassCastException;

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
    protected abstract Object convertScalarisJSONtoJava_object(
            OtpErlangList value) throws ClassCastException;
    
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
    protected Object convertScalarisJSONtoJava_value(OtpErlangObject value)
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
        } else if (value instanceof OtpErlangList) {
            try {
                return ErlangValue.otpObjectToString(value);
            } catch (ClassCastException e) {
                throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
            }
        } else if (value instanceof OtpErlangTuple) {
            OtpErlangTuple value_tpl = (OtpErlangTuple) value;
            if (value_tpl.arity() == 2) {
                OtpErlangObject tag = value_tpl.elementAt(0);
                if (tag.equals(CommonErlangObjects.structAtom)) {
                    return convertScalarisJSONtoJava_object((OtpErlangList) value_tpl
                            .elementAt(1));
                } else if (tag.equals(CommonErlangObjects.arrayAtom)) {
                    return convertScalarisJSONtoJava_array(
                            ErlangValue.otpObjectToOtpList(value_tpl.elementAt(1)));
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
    protected OtpErlangObject convertJavaToScalarisJSON_value(Object value)
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
    protected List<Object> convertScalarisJSONtoJava_array(
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
    protected OtpErlangTuple convertJavaToScalarisJSON_array(
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
}
