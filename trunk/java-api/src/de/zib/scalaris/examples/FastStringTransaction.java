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

import com.ericsson.otp.erlang.OtpConnection;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

/**
 * Implements a faster {@link String} storage mechanism if only Java access to
 * scalaris is used.
 * 
 * <p>
 * Uses {@link OtpErlangBinary} objects that store the array of bytes a
 * {@link String} consists of and writes this binary to scalaris. Trying to read
 * strings will convert a returned {@link OtpErlangBinary} to a {@link String}
 * or return the value of a {@link OtpErlangString} result.
 * </p>
 * 
 * <p>
 * Run with {@code java -cp scalaris-examples.jar de.zib.scalaris.examples.FastStringTransaction}
 * </p>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class FastStringTransaction extends Transaction {

	/**
	 * Constructor using the default connection.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public FastStringTransaction() throws ConnectionException {
		super();
	}

	/**
	 * Constructor using the given connection to an erlang node.
	 * 
	 * @param conn
	 *            connection to use for the transaction
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public FastStringTransaction(OtpConnection conn) throws ConnectionException {
		super(conn);
	}

	/**
	 * Gets the value stored under the given {@code key}.
	 * 
	 * {@link OtpErlangString}s are converted to {@link String}s and
	 * {@link OtpErlangBinary} objects are assumed to be strings as created by
	 * {@link #write(String, String)} and converted to {@link String}s
	 * accordingly.
	 * 
	 * @param key
	 *            the key to look up
	 * 
	 * @return the (string) value stored under the given {@code key}
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to fetch the value
	 * @throws NotFoundException
	 *             if the requested key does not exist
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @see #readObject(OtpErlangString)
	 */
	@Override
	public String read(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		OtpErlangObject result = super.readObject(new OtpErlangString(key));
		if (result instanceof OtpErlangBinary) {
			return new String(((OtpErlangBinary) result).binaryValue());
		} else if (result instanceof OtpErlangString) {
			return ((OtpErlangString) result).stringValue();
		} else {
			throw new UnknownException("Unexpected result type: "
					+ result.getClass());
		}
	}

	/**
	 * Stores the given {@code key}/{@code value} pair.
	 * 
	 * The {@code value} is converted to an {@link OtpErlangBinary} using the
	 * bytes of the string ({@link String#getBytes()}.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 * 
	 * @see #writeObject(OtpErlangString, OtpErlangObject)
	 */
	@Override
	public void write(String key, String value) throws ConnectionException,
			TimeoutException, UnknownException {
		OtpErlangString otpKey = new OtpErlangString(key);
		OtpErlangBinary otpValue = new OtpErlangBinary(value.getBytes());
		
		super.writeObject(otpKey, otpValue);
	}
}
