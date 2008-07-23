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
package de.zib.chordsharp;

import java.util.Vector;

/**
 * Public ChordSharp Interface.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.1
 */
public class ChordSharp {

	/**
	 * Gets the value stored with the given {@code key}.
	 * 
	 * @param key
	 *            the key to look up
	 * @return the value stored under the given {@code key}
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
	 */
	public static String read(String key) throws ConnectionException,
			TimeoutException, UnknownException, NotFoundException {
		return ChordSharpConnection.readString(key);
	}

	/**
	 * Stores the given {@code key}/{@code value} pair.
	 * 
	 * @param key
	 *            the key to store the value for
	 * @param value
	 *            the value to store
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public static void write(String key, String value)
			throws ConnectionException, TimeoutException, UnknownException {
		ChordSharpConnection.write(key, value);
	}

	/**
	 * Publishes an event under a given {@code topic}.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 */
	public static void publish(String topic, String content) throws ConnectionException {
		ChordSharpConnection.publish(topic, content);
	}

	/**
	 * Subscribes a url for a {@code topic}.
	 * 
	 * @param topic
	 *            the topic to subscribe the url for
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 */
	public static void subscribe(String topic, String url)
			throws ConnectionException {
		ChordSharpConnection.subscribe(topic, url);
	}

	/**
	 * Gets a list of subscribers of a {@code topic}.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 * @return the subscriber URLs
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 */
	public static Vector<String> getSubscribers(String topic)
			throws ConnectionException, UnknownException {
		return ChordSharpConnection.getSubscribers(topic);
	}
}