/**
 *  Copyright 2007-2008 Konrad-Zuse-Zentrum für Informationstechnik Berlin
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

import de.zib.scalaris.CustomOtpObject;

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
 * Run with <code>java -cp scalaris-examples.jar de.zib.scalaris.examples.FastStringBenchmark</code>
 * </p>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.1
 * @since 2.1
 */
class CustomOtpFastStringObject extends CustomOtpObject<String> {
	/**
	 * Identifies the tuple of the stored erlang object.
	 */
	private OtpErlangAtom identifier = new OtpErlangAtom("custom_fast_string");

	/**
	 * Creates an empty object.
	 */
	public CustomOtpFastStringObject() {
		super();
	}

	/**
	 * Creates an object with the given (Java) value.
	 * 
	 * @param value
	 *            the value to use
	 */
	public CustomOtpFastStringObject(String value) {
		super(value);
	}

	/**
	 * Creates an object with the given (erlang) value.
	 * 
	 * @param otpValue
	 *            the value to use
	 */
	public CustomOtpFastStringObject(OtpErlangObject otpValue) {
		super(otpValue);
	}

	/**
	 * Converts {@link #value} to an erlang object and saves it to
	 * {@link #otpValue}.
	 */
	@Override
	public void convertToOtpObject() {
		otpValue = new OtpErlangTuple(new OtpErlangObject[] { identifier,
				new OtpErlangBinary(value.getBytes()) });
	}

	/**
	 * Converts {@link #otpValue} to a Java object and saves it to
	 * {@link #value}.
	 * 
	 * {@link #otpValue} is assumed to be either a tuple with its first element
	 * being {@link #identifier} and its second the binary containing a string
	 * representation or a {@link OtpErlangString}.
	 * 
	 * @throws ClassCastException
	 *             if the conversion fails
	 */
	@Override
	public void convertFromOtpObject() {
		if (otpValue instanceof OtpErlangTuple) {
			OtpErlangTuple otpTuple = (OtpErlangTuple) otpValue;
			if (otpTuple.elementAt(0).equals(identifier)) {
				value = new String(((OtpErlangBinary) otpTuple.elementAt(0))
						.binaryValue());
				return;
			}
		} else if (otpValue instanceof OtpErlangString) {
			value = ((OtpErlangString) otpValue).stringValue();
			return;
		}

		throw new ClassCastException("Unexpected result type: "
				+ otpValue.getClass());
	}
}
