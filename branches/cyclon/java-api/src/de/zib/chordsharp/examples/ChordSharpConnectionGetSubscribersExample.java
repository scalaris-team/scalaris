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

import java.util.Iterator;
import java.util.Vector;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.chordsharp.ChordSharpConnection;
import de.zib.chordsharp.ConnectionException;
import de.zib.chordsharp.UnknownException;

/**
 * Provides an example for using the {@code getSubscribers} methods of the
 * {@link ChordSharpConnection} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.1
 */
public class ChordSharpConnectionGetSubscribersExample {
	/**
	 * Prints a list of all subscribers to a given topic, provided on the
	 * command line with the {@code getSubscribers} methods of
	 * {@link ChordSharpConnection}.<br />
	 * If no topic is given, the default topic {@code "topic"} is used.
	 * 
	 * @param args
	 *            command line arguments (first argument can be an optional
	 *            topic to get the subscribers for)
	 */
	public static void main(String[] args) {
		String topic;

		if (args.length != 1) {
			topic = "topic";
		} else {
			topic = args[0];
		}

		OtpErlangString otpTopic = new OtpErlangString(topic);

		OtpErlangList otpSubscribers;
		Vector<String> subscribers;

		System.out
				.println("Reading values with the class `ChordSharpConnection`:");

		// static:
		try {
			System.out
					.println("  `static OtpErlangString getSubscribers(OtpErlangString)`...");
			otpSubscribers = ChordSharpConnection.getSubscribers(otpTopic);
			System.out.println("    getSubscribers(" + otpTopic.stringValue()
					+ ") == " + getSubscribers(otpSubscribers));
		} catch (ConnectionException e) {
			System.out.println("    getSubscribers(" + otpTopic.stringValue()
					+ ") failed: " + e.getMessage());
		} catch (UnknownException e) {
			System.out.println("    getSubscribers(" + otpTopic.stringValue()
					+ ") failed: " + e.getMessage());
		}

		try {
			System.out.println("  `static String getSubscribers(String)`...");
			subscribers = ChordSharpConnection.getSubscribers(topic);
			System.out.println("    getSubscribers(" + topic + ") == "
					+ getSubscribers(subscribers));
		} catch (ConnectionException e) {
			System.out.println("    getSubscribers(" + topic + ") failed: "
					+ e.getMessage());
		} catch (UnknownException e) {
			System.out.println("    getSubscribers(" + topic
					+ ") failed: " + e.getMessage());
		}

		// non-static:
		try {
			System.out.println("  creating object...");
			ChordSharpConnection cs = new ChordSharpConnection();
			System.out
					.println("    `OtpErlangString singleGetSubscribers(OtpErlangString)`...");
			otpSubscribers = cs.singleGetSubscribers(otpTopic);
			System.out.println("      getSubscribers(" + otpTopic.stringValue()
					+ ") == " + getSubscribers(otpSubscribers));
		} catch (ConnectionException e) {
			System.out.println("      getSubscribers(" + otpTopic.stringValue()
					+ ") failed: " + e.getMessage());
		} catch (UnknownException e) {
			System.out.println("    getSubscribers(" + otpTopic.stringValue()
					+ ") failed: " + e.getMessage());
		}

		try {
			System.out.println("  creating object...");
			ChordSharpConnection cs = new ChordSharpConnection();
			System.out.println("    `String singleGetSubscribers(String)`...");
			subscribers = cs.singleGetSubscribers(topic);
			System.out.println("      getSubscribers(" + topic + ") == "
					+ getSubscribers(subscribers));
		} catch (ConnectionException e) {
			System.out.println("      getSubscribers(" + topic + ") failed: "
					+ e.getMessage());
		} catch (UnknownException e) {
			System.out.println("    getSubscribers(" + topic
					+ ") failed: " + e.getMessage());
		}
	}

	/**
	 * converts the list of strings to a comma-separated list of strings
	 * 
	 * @param subscribers
	 *            the list of subscribers to convert
	 * @return a comma-separated list of subscriber URLs
	 */
	private static String getSubscribers(Vector<String> subscribers) {
		StringBuffer result = new StringBuffer();
		for (Iterator<String> iterator = subscribers.iterator(); iterator
				.hasNext();) {
			result.append(iterator.next());
			result.append(", ");
		}
		if (result.length() > 2) {
			return "[" + result.substring(0, result.length() - 3) + "]";
		} else {
			return "[" + result.toString() + "]";
		}
	}

	/**
	 * converts the list of erlang strings to a comma-separated list of strings
	 * 
	 * @param subscribers
	 *            the list of subscribers to convert
	 * @return a comma-separated list of subscriber URLs
	 */
	private static String getSubscribers(OtpErlangList subscribers) {
		StringBuffer result = new StringBuffer();
		for (int i = 0; i < subscribers.arity(); ++i) {
			result.append(((OtpErlangString) subscribers.elementAt(i))
					.stringValue());
			result.append(", ");
		}
		if (result.length() > 2) {
			return "[" + result.substring(0, result.length() - 3) + "]";
		} else {
			return "[" + result.toString() + "]";
		}
	}
}
