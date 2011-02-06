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
package de.zib.scalaris;

import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

/**
 * Implements a {@link CustomOtpObject} that allows {@link String} objects to be
 * written to scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.1
 * @since 2.1
 */
public class CustomOtpStringObject extends CustomOtpObject<String> {
	/**
	 * Creates a new and empty object.
	 */
	public CustomOtpStringObject() {
		super();
	}

	/**
	 * Creates an object with the given (Java) value.
	 * 
	 * @param value
	 *            the value to use
	 */
	public CustomOtpStringObject(String value) {
		super(value);
	}

	/**
	 * Creates an object with the given (erlang) value.
	 * 
	 * @param otpValue
	 *            the value to use
	 */
	public CustomOtpStringObject(OtpErlangObject otpValue) {
		super(otpValue);
	}

	/**
	 * Converts {@link #value} to an erlang object and saves it to
	 * {@link #otpValue}.
	 */
	@Override
	public void convertToOtpObject() {
		otpValue = new OtpErlangString(value);
	}

	/**
	 * Converts {@link #otpValue} to a Java object and saves it to
	 * {@link #value}.
	 * 
	 * @throws ClassCastException
	 *             if the conversion fails
	 */
	@Override
	public void convertFromOtpObject() {
		value = ((OtpErlangString) otpValue).stringValue();
	}
}
