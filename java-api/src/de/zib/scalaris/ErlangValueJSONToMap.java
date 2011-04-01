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

import java.util.LinkedHashMap;
import java.util.Map;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Converts Scalaris-JSON to {@link Map}<String, Object> and such maps to
 * Scalaris-JSON.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
class ErlangValueJSONToMap extends ErlangValueJSONBase<Map<String, Object>> {

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
    @Override
    protected Map<String, Object> convertScalarisJSONtoJava_object(
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
                } else {
                    try {
                        key = ErlangValue.otpObjectToString(key_erl);
                    } catch (ClassCastException e) {
                        throw new ClassCastException("Unsupported JSON type (value: " + value.toString() + ")");
                    }
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
    protected OtpErlangTuple convertJavaToScalarisJSON_object(
            Object value_) throws ClassCastException {
        try {
            @SuppressWarnings("unchecked")
            Map<String, Object> value = (Map<String, Object>) value_;
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
        } catch (ClassCastException e) {
            throw new ClassCastException("Unsupported JSON type (value: " + value_.toString() + ")");
        }
    }
}
