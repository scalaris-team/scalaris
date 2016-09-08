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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Converts Scalaris-JSON to {@link Map}&lt;String, Object&gt; and such maps to
 * Scalaris-JSON.
 *
 * @author Nico Kruber, kruber@zib.de
 */
class ErlangValueJSONToMap extends ErlangValueJSONBase
        implements ErlangValueJSONInterface<Map<String, Object>> {
    /* (non-Javadoc)
     * @see de.zib.scalaris.ErlangValueJSON#toScalarisJSON(T)
     */
    @Override
    public OtpErlangTuple toScalarisJSON(final Map<String, Object> value)
            throws ClassCastException {
        return convertJavaToScalarisJSON_object(value);
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.ErlangValueJSON#toJava(com.ericsson.otp.erlang.OtpErlangList)
     */
    @Override
    @SuppressWarnings("unchecked")
    public Map<String, Object> toJava(final OtpErlangList value)
            throws ClassCastException {
        return (Map<String, Object>) convertScalarisJSONtoJava_object(value);
    }

    /**
     * Converts a Java Map to a JSON object as expected by Scalaris.
     *
     * @param value_
     *            a Java Map with String-keys and supported JSON types as values
     *
     * @return a JSON object representing the value
     *
     * @throws ClassCastException
     *             if thrown if a conversion is not possible, i.e. the type is
     *             not supported
     */
    @Override
    protected OtpErlangTuple convertJavaToScalarisJSON_object(final Object value_)
            throws ClassCastException {
        try {
            @SuppressWarnings("unchecked")
            final
            Map<String, Object> value = (Map<String, Object>) value_;
            final OtpErlangTuple[] resultList = new OtpErlangTuple[value.size()];
            int i = 0;

            for (final Map.Entry<String, Object> entry : value.entrySet()) {
                resultList[i] = new OtpErlangTuple(new OtpErlangObject[] {
                        new OtpErlangString(entry.getKey()),
                        convertJavaToScalarisJSON_value(entry.getValue()) });
                ++i;
            }
            final OtpErlangTuple resultTpl = new OtpErlangTuple(new OtpErlangObject[] {
                    CommonErlangObjects.structAtom, new OtpErlangList(resultList) });
            return resultTpl;
        } catch (final ClassCastException e) {
            throw new ClassCastException("Unsupported JSON type (value: " + value_.toString() + ")");
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
    protected Object convertScalarisJSONtoJava_object(final OtpErlangList value)
            throws ClassCastException {
        final Map<String, Object> result = new LinkedHashMap<String, Object>(
                value.arity());
        for (final OtpErlangObject iter : value) {
            final OtpErlangTuple iter_tpl = (OtpErlangTuple) iter;
            if (iter_tpl.arity() == 2) {
                final String key = convertScalarisJSONtoJava_key(iter_tpl.elementAt(0));
                result.put(key,
                        convertScalarisJSONtoJava_value(iter_tpl.elementAt(1)));
            } else {
                throw new ClassCastException("Unsupported JSON type (value: "
                        + value.toString() + ")");
            }
        }
        return result;
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
    protected Object convertScalarisJSONtoJava_value(final OtpErlangObject value)
            throws ClassCastException {
        if (value instanceof OtpErlangTuple) {
            final OtpErlangTuple value_tpl = (OtpErlangTuple) value;
            if (value_tpl.arity() == 2) {
                final OtpErlangObject tag = value_tpl.elementAt(0);
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
        } else {
            return super.convertScalarisJSONtoJava_value_simple(value);
        }
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
            final OtpErlangList value) throws ClassCastException {
        final List<Object> result = new ArrayList<Object>(value.arity());
        for (final OtpErlangObject iter : value) {
            result.add(convertScalarisJSONtoJava_value(iter));
        }
        return result;
    }
}
