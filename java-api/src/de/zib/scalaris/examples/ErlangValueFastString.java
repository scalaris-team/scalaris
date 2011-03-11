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
     * Identifies the tuple of the stored erlang object.
     */
    private static OtpErlangAtom identifier = new OtpErlangAtom("$fs");

    /**
     * Creates an object with the given (Java) value.
     * 
     * @param value
     *            the value to use
     */
    public ErlangValueFastString(String value) {
        super(new OtpErlangTuple(new OtpErlangObject[] {
                identifier,
                new OtpErlangBinary(value.getBytes()) }));
    }

    /**
     * Creates an object with the given (erlang) value.
     * 
     * @param otpValue
     *            the value to use
     */
    public ErlangValueFastString(OtpErlangObject otpValue) {
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
    public ErlangValueFastString(ErlangValue value) {
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
    public String toString() {
        OtpErlangTuple otpTuple = (OtpErlangTuple) value();
        if (otpTuple.elementAt(0).equals(identifier)) {
            return new String(
                    ((OtpErlangBinary) otpTuple.elementAt(0)).binaryValue());
        }

        throw new ClassCastException("Unexpected result type: "
                + value().getClass());
    }
}
