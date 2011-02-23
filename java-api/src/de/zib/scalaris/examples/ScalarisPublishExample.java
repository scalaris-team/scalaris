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

import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.Scalaris;

/**
 * Provides an example for using the <tt>publish</tt> methods of the
 * {@link Scalaris} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class ScalarisPublishExample {
	/**
	 * Publishes content under a given topic, both provided on the command line,
	 * with the <tt>publish</tt> methods of {@link Scalaris}.<br />
	 * If no content or topic is given, the default key <tt>"key"</tt> and the
	 * default value <tt>"value"</tt> is used.
	 * 
	 * @param args
	 *            command line arguments (first argument can be an optional
	 *            topic and the second an optional content)
	 */
	public static void main(String[] args) {
		String topic;
		String content;

		if (args.length == 0) {
			topic = "topic";
			content = "content";
		} else if (args.length == 1) {
			topic = args[0];
			content = "content";
		} else {
			topic = args[0];
			content = args[1];
		}
		
		OtpErlangString otpTopic = new OtpErlangString(topic);
		OtpErlangString otpContent = new OtpErlangString(content);

		System.out
				.println("Publishing content under a topic with the class `Scalaris`:");

		try {
			System.out.println("  creating object...");
			Scalaris sc = new Scalaris();
			System.out
					.println("    `void publish(OtpErlangString, OtpErlangString)`...");
			sc.publish(otpTopic, otpContent);
			System.out.println("      publish(" + otpTopic.stringValue() + ", "
					+ otpContent.stringValue() + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      publish(" + otpTopic.stringValue() + ", "
					+ otpContent.stringValue() + ") failed: " + e.getMessage());
		}

		try {
			System.out.println("  creating object...");
			Scalaris sc = new Scalaris();
			System.out.println("    `void publish(String, String)`...");
			sc.publish(topic, content);
			System.out.println("      publish(" + topic + ", " + content
					+ ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      publish(" + topic + ", " + content
					+ ") failed: " + e.getMessage());
		}
	}
}
