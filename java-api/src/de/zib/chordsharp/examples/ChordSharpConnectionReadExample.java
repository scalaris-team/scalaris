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
package de.zib.chordsharp.examples;

import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.chordsharp.ChordSharpConnection;
import de.zib.chordsharp.ConnectionException;
import de.zib.chordsharp.NotFoundException;
import de.zib.chordsharp.TimeoutException;
import de.zib.chordsharp.UnknownException;

/**
 * Provides an example for using the {@code read} methods of the
 * {@link ChordSharpConnection} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.0
 */
public class ChordSharpConnectionReadExample {
	/**
	 * Reads a key given on the command line with the {@code read} methods of
	 * {@link ChordSharpConnection}.<br />
	 * If no key is given, the default key {@code "key"} is used.
	 * 
	 * @param args
	 *            command line arguments (first argument can be an optional key
	 *            to look up)
	 */
	public static void main(String[] args) {
		String key;
		String value;

		if (args.length != 1) {
			key = "key";
		} else {
			key = args[0];
		}

		OtpErlangString otpKey = new OtpErlangString(key);
		OtpErlangString otpValue;

		System.out
				.println("Reading values with the class `ChordSharpConnection`:");

		// static:
		try {
			System.out
					.println("  `static OtpErlangString read(OtpErlangString)`...");
			otpValue = ChordSharpConnection.readString(otpKey);
			System.out.println("    read(" + otpKey.stringValue() + ") == "
					+ otpValue.stringValue());
		} catch (ConnectionException e) {
			System.out.println("    read(" + otpKey.stringValue()
					+ ") failed: " + e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("    read(" + otpKey.stringValue()
					+ ") failed with timeout: " + e.getMessage());
		} catch (UnknownException e) {
			System.out.println("    read(" + otpKey.stringValue()
					+ ") failed with unknown: " + e.getMessage());
		} catch (NotFoundException e) {
			System.out.println("    read(" + otpKey.stringValue()
					+ ") failed with not found: " + e.getMessage());
		}

		try {
			System.out.println("  `static String read(String)`...");
			value = ChordSharpConnection.readString(key);
			System.out.println("    read(" + key + ") == " + value);
		} catch (ConnectionException e) {
			System.out.println("    read(" + key + ") failed: "
					+ e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("    read(" + key + ") failed with timeout: "
					+ e.getMessage());
		} catch (UnknownException e) {
			System.out.println("    read(" + key + ") failed with unknown: "
					+ e.getMessage());
		} catch (NotFoundException e) {
			System.out.println("    read(" + key + ") failed with not found: "
					+ e.getMessage());
		}

		// non-static:
		try {
			System.out.println("  creating object...");
			ChordSharpConnection cs = new ChordSharpConnection();
			System.out
					.println("    `OtpErlangString singleRead(OtpErlangString)`...");
			otpValue = cs.singleReadString(otpKey);
			System.out.println("      read(" + otpKey.stringValue() + ") == "
					+ otpValue.stringValue());
		} catch (ConnectionException e) {
			System.out.println("      read(" + otpKey.stringValue()
					+ ") failed: " + e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("      read(" + otpKey.stringValue()
					+ ") failed with timeout: " + e.getMessage());
		} catch (UnknownException e) {
			System.out.println("      read(" + otpKey.stringValue()
					+ ") failed with unknown: " + e.getMessage());
		} catch (NotFoundException e) {
			System.out.println("      read(" + otpKey.stringValue()
					+ ") failed with not found: " + e.getMessage());
		}

		try {
			System.out.println("  creating object...");
			ChordSharpConnection cs = new ChordSharpConnection();
			System.out.println("    `String singleRead(String)`...");
			value = cs.singleReadString(key);
			System.out.println("      read(" + key + ") == " + value);
		} catch (ConnectionException e) {
			System.out.println("      read(" + key + ") failed: "
					+ e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("      read(" + key + ") failed with timeout: "
					+ e.getMessage());
		} catch (UnknownException e) {
			System.out.println("      read(" + key + ") failed with unknown: "
					+ e.getMessage());
		} catch (NotFoundException e) {
			System.out.println("      read(" + key
					+ ") failed with not found: " + e.getMessage());
		}
	}
}
