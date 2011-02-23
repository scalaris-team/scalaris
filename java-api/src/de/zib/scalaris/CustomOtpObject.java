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
package de.zib.scalaris;

import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * Wrapper class that allows to write custom objects to scalaris.
 * 
 * Provides methods to convert a given Java type to another erlang type.
 * 
 * @param <JavaType>
 *            java type to use
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.1
 * @since 2.1
 */
public abstract class CustomOtpObject<JavaType> {
	/**
	 * The contained value as a Java type.
	 */
	protected JavaType value = null;
	/**
	 * The contained value as an erlang type.
	 */
	protected OtpErlangObject otpValue = null;

	/**
	 * Creates a new and empty object.
	 */
	public CustomOtpObject() {
	}

	/**
	 * Creates an object with the given (Java) value.
	 * 
	 * @param value
	 *            the value to use
	 */
	public CustomOtpObject(JavaType value) {
		this.value = value;
	}

	/**
	 * Creates an object with the given (erlang) value.
	 * 
	 * @param otpValue
	 *            the value to use
	 */
	public CustomOtpObject(OtpErlangObject otpValue) {
		this.otpValue = otpValue;
	}

	/**
	 * Returns the stored value as a Java type. If the value stored is an erlang
	 * object it is converted to a java value once.
	 * 
	 * Future calls will not convert the value.
	 * 
	 * @return the stored value
	 * 
	 * @throws ClassCastException
	 *             if the conversion fails
	 */
	public JavaType getValue() throws ClassCastException {
		if (value == null) {
			convertFromOtpObject();
		}
		return value;
	}

	/**
	 * Returns the stored value as an erlang type. If the value stored is a Java
	 * object it is converted to an erlang value once.
	 * 
	 * Future calls will not convert the value.
	 * 
	 * @return the stored value
	 */
	public OtpErlangObject getOtpValue() {
		if (otpValue == null) {
			convertToOtpObject();
		}
		return otpValue;
	}

	/**
	 * Converts {@link #value} to an erlang object and saves it to
	 * {@link #otpValue}.
	 */
	public abstract void convertToOtpObject();

	/**
	 * Converts {@link #otpValue} to a Java object and saves it to
	 * {@link #value}.
	 * 
	 * @throws ClassCastException
	 *             if the conversion fails
	 */
	public abstract void convertFromOtpObject() throws ClassCastException;

	/**
	 * Sets the stored Java value.
	 * 
	 * @param value
	 *            the value to set
	 */
	public void setValue(JavaType value) {
		this.value = value;
	}

	/**
	 * Sets the stored erlang value.
	 * 
	 * @param otpValue
	 *            the otpValue to set
	 */
	public void setOtpValue(OtpErlangObject otpValue) {
		this.otpValue = otpValue;
	}

	/**
	 * 
	 */
	public void clear() {
		value = null;
		otpValue = null;
	}
}
