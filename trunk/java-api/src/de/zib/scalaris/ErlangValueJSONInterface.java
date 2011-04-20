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

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Interface for Scalaris-JSON to Java and Java to Scalaris-JSON converters.
 *
 * @param <T> the class to convert to/from
 *
 * @author Nico Kruber, kruber@zib.de
 */
interface ErlangValueJSONInterface<T> {
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
    public abstract OtpErlangTuple toScalarisJSON(T value)
            throws ClassCastException;

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
    public abstract T toJava(OtpErlangList value) throws ClassCastException;

}