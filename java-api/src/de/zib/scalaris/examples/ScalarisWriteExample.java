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

import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.Scalaris;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the <tt>write</tt> methods of the
 * {@link Scalaris} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class ScalarisWriteExample {
	/**
	 * Writes a key/value pair given on the command line with the <tt>write</tt>
	 * methods of {@link Scalaris}.<br />
	 * If no value or key is given, the default key <tt>"key"</tt> and the
	 * default value <tt>"value"</tt> is used.
	 * 
	 * @param args
	 *            command line arguments (first argument can be an optional key
	 *            and the second an optional value)
	 */
	public static void main(String[] args) {
		String key;
		String value;

		if (args.length == 0) {
			key = "key";
			value = "value";
		} else if (args.length == 1) {
			key = args[0];
			value = "value";
		} else {
			key = args[0];
			value = args[1];
		}

		OtpErlangString otpKey = new OtpErlangString(key);
		OtpErlangString otpValue = new OtpErlangString(value);

		System.out.println("Writing values with the class `Scalaris`:");

		try {
			System.out.println("  creating object...");
			Scalaris sc = new Scalaris();
			System.out
					.println("    `void writeObject(OtpErlangString, OtpErlangObject)`...");
			sc.writeObject(otpKey, otpValue);
			System.out.println("      write(" + otpKey.stringValue() + ", "
					+ otpValue.stringValue() + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      write(" + otpKey.stringValue() + ", "
					+ otpValue.stringValue() + ") failed: " + e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("      write(" + otpKey.stringValue() + ", "
					+ otpValue.stringValue() + ") failed with timeout: "
					+ e.getMessage());
		} catch (UnknownException e) {
			System.out.println("      write(" + otpKey.stringValue() + ", "
					+ otpValue.stringValue() + ") failed with unknown: "
					+ e.getMessage());
		}

		try {
			System.out.println("  creating object...");
			Scalaris sc = new Scalaris();
			System.out.println("    `void write(String, String)`...");
			sc.write(key, value);
			System.out.println("      write(" + key + ", " + value
					+ ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      write(" + key + ", " + value
					+ ") failed: " + e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("      write(" + key + ", " + value
					+ ") failed with timeout: " + e.getMessage());
		} catch (UnknownException e) {
			System.out.println("      write(" + key + ", " + value
					+ ") failed with unknown: " + e.getMessage());
		}
	}
}
