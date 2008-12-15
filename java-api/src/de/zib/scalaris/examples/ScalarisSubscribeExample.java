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

import de.zib.scalaris.Scalaris;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the {@code subscribe} methods of the
 * {@link Scalaris} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class ScalarisSubscribeExample {
	/**
	 * Subscribes a given URL to a given topic, both provided on the command
	 * line, with the {@code subscribe} methods of {@link Scalaris}.<br />
	 * If no URL or topic is given, the default URL {@code "url"} and the
	 * default topic {@code "topic"} is used.
	 * 
	 * @param args
	 *            command line arguments (first argument can be an optional
	 *            topic and the second an optional URL)
	 */
	public static void main(String[] args) {
		String topic;
		String URL;

		if (args.length == 0) {
			topic = "topic";
			URL = "url";
		} else if (args.length == 1) {
			topic = args[0];
			URL = "url";
		} else {
			topic = args[0];
			URL = args[1];
		}

		OtpErlangString otpTopic = new OtpErlangString(topic);
		OtpErlangString otpURL = new OtpErlangString(URL);

		System.out
				.println("Subscribing a URL to a topic with the class `Scalaris`:");
		
		try {
			System.out.println("  creating object...");
			Scalaris sc = new Scalaris();
			System.out
					.println("    `void subscribe(OtpErlangString, OtpErlangString)`...");
			sc.subscribe(otpTopic, otpURL);
			System.out.println("      subscribe(" + otpTopic.stringValue()
					+ ", " + otpURL.stringValue() + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      subscribe(" + otpTopic.stringValue()
					+ ", " + otpURL.stringValue() + ") failed: "
					+ e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("      subscribe(" + otpTopic.stringValue()
					+ ", " + otpURL.stringValue() + ") failed with timeout: "
					+ e.getMessage());
		} catch (UnknownException e) {
			System.out.println("      subscribe(" + otpTopic.stringValue()
					+ ", " + otpURL.stringValue() + ") failed with unknown: "
					+ e.getMessage());
		}

		try {
			System.out.println("  creating object...");
			Scalaris sc = new Scalaris();
			System.out.println("    `void subscribe(String, String)`...");
			sc.subscribe(topic, URL);
			System.out.println("      subscribe(" + topic + ", " + URL
					+ ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      subscribe(" + topic + ", " + URL
					+ ") failed: " + e.getMessage());
		} catch (TimeoutException e) {
			System.out.println("      subscribe(" + topic + ", " + URL
					+ ") failed with timeout: " + e.getMessage());
		} catch (UnknownException e) {
			System.out.println("      subscribe(" + topic + ", " + URL
					+ ") failed with unknown: " + e.getMessage());
		}
	}
}
