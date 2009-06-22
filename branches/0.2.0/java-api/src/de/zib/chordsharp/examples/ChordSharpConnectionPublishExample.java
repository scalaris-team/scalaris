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

/**
 * Provides an example for using the {@code publish} methods of the
 * {@link ChordSharpConnection} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.0
 */
@Deprecated
public class ChordSharpConnectionPublishExample {
	/**
	 * Publishes content under a given topic, both provided on the command line,
	 * with the {@code publish} methods of {@link ChordSharpConnection}.<br />
	 * If no content or topic is given, the default key {@code "key"} and the
	 * default value {@code "value"} is used.
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
		
		System.out.println("Publishing content under a topic with the class `ChordSharpConnection`:");

		// static:
		try {
			System.out.println("  `static void publish(OtpErlangString, OtpErlangString)`...");
			ChordSharpConnection.publish(otpTopic, otpContent);
			System.out.println("    publish(" + otpTopic.stringValue() + ", " + otpContent.stringValue() + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("    publish(" + otpTopic.stringValue() + ", " + otpContent.stringValue() + ") failed: " + e.getMessage());
		}
		
		try {
			System.out.println("  `static void publish(String, String)`...");
			ChordSharpConnection.publish(topic, content);
			System.out.println("    publish(" + topic + ", " + content + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("    publish(" + topic + ", " + content + ") failed: " + e.getMessage());
		}

		// non-static:
		try {
			System.out.println("  creating object...");
			ChordSharpConnection cs = new ChordSharpConnection();
			System.out.println("    `void singlePublish(OtpErlangString, OtpErlangString)`...");
			cs.singlePublish(otpTopic, otpContent);
			System.out.println("      publish(" + otpTopic.stringValue() + ", " + otpContent.stringValue() + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      publish(" + otpTopic.stringValue() + ", " + otpContent.stringValue() + ") failed: " + e.getMessage());
		}
		
		try {
			System.out.println("  creating object...");
			ChordSharpConnection cs = new ChordSharpConnection();
			System.out.println("    `void singlePublish(String, String)`...");
			cs.singlePublish(topic, content);
			System.out.println("      publish(" + topic + ", " + content + ") succeeded");
		} catch (ConnectionException e) {
			System.out.println("      publish(" + topic + ", " + content + ") failed: " + e.getMessage());
		}
	}
}
