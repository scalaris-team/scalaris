/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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
package de.zib.scalaris.examples;

import java.nio.charset.Charset;
import com.ericsson.otp.erlang.OtpErlangBitstr;
import com.ericsson.otp.erlang.OtpErlangObject;

import de.zib.scalaris.ErlangValue;

/**
 * Implements a faster {@link String} storage mechanism using erlang bitstrings.
 *
 * <p>
 * Run a benchmark of the different String implementations with
 * <code>java -cp scalaris-examples.jar de.zib.scalaris.examples.FastStringBenchmark</code>
 * </p>
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.9
 * @since 2.9
 */
public class ErlangValueBitString extends ErlangValue {
    /**
     * UTF-8 charset object.
     *
     * StandardCharsets.UTF_8 is only available for Java >= 7
     */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Creates an object with the given (Java) value.
     *
     * @param value
     *            the value to use
     */
    public ErlangValueBitString(final String value) {
        super(new OtpErlangBitstr(value.getBytes(UTF_8)));
    }

    /**
     * Creates an object with the given (erlang) value.
     *
     * @param otpValue
     *            the value to use
     */
    public ErlangValueBitString(final OtpErlangObject otpValue) {
        super(otpValue);
    }

    /**
     * Creates an object with the given (erlang) value.
     * Provided for convenience.
     *
     * @param value
     *            the value to use
     *
     * @see ErlangValue
     */
    public ErlangValueBitString(final ErlangValue value) {
        super(value.value());
    }

    /**
     * Converts the stored erlang value created by this object to a Java
     * {@link String}.
     *
     * @throws ClassCastException
     *             if the conversion fails
     */
    @Override
    public String stringValue() {
        return new String(((OtpErlangBitstr) value()).binaryValue(), UTF_8);
    }
}
