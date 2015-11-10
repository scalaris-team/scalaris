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
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.ErlangValue;

/**
 * Implements a faster {@link String} storage mechanism if only Java access to
 * scalaris is used.
 *
 * <p>
 * Uses {@link OtpErlangBinary} objects that store the array of bytes a
 * {@link String} consists of and writes this binary to scalaris wrapped into a
 * {@link OtpErlangTuple}. Trying to read strings will convert a returned
 * {@link OtpErlangBinary} to a {@link String} or return the value of a
 * {@link OtpErlangString} result.
 * </p>
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
public class ErlangValueFastString extends ErlangValue {
    /**
     * UTF-8 charset object.
     *
     * StandardCharsets.UTF_8 is only available for Java >= 7
     */
    private static final Charset UTF_8 = Charset.forName("UTF-8");

    /**
     * Identifies the tuple of the stored erlang object.
     */
    private static OtpErlangAtom identifier = new OtpErlangAtom("$fs");

    /**
     * Creates an object with the given (Java) value.
     *
     * @param value
     *            the value to use
     */
    public ErlangValueFastString(final String value) {
        super(new OtpErlangTuple(new OtpErlangObject[] {
                identifier,
                new OtpErlangBinary(value.getBytes(UTF_8)) }));
    }

    /**
     * Creates an object with the given (erlang) value.
     *
     * @param otpValue
     *            the value to use
     */
    public ErlangValueFastString(final OtpErlangObject otpValue) {
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
    public ErlangValueFastString(final ErlangValue value) {
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
        final OtpErlangTuple otpTuple = (OtpErlangTuple) value();
        if (otpTuple.elementAt(0).equals(identifier)) {
            return new String(
                    ((OtpErlangBinary) otpTuple.elementAt(0)).binaryValue(),
                    UTF_8);
        }

        throw new ClassCastException("Unexpected result type: "
                + value().getClass());
    }
}
