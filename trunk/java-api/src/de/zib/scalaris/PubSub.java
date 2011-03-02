/*
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

import java.io.IOException;
import java.util.ArrayList;

import com.ericsson.otp.erlang.OtpAuthException;
import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangExit;
import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Provides methods to subscribe to topics and publish data.
 * 
 * <p>
 * Instances of this class can be generated using a given connection to a
 * scalaris node using {@link #PubSub(Connection)} or without a
 * connection ({@link #PubSub()}) in which case a new connection is
 * created using {@link ConnectionFactory#createConnection()}.
 * </p>
 * 
 * <h3>Publishing topics</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String topic;
 *   String content;
 *   OtpErlangString otpTopic;
 *   OtpErlangString otpContent;
 *   
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   sc.publish(topic, content);       // {@link #publish(String, String)}
 *   sc.publish(otpTopic, otpContent); // {@link #publish(OtpErlangString, OtpErlangString)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see {@link de.zib.scalaris.examples.PubSubPublishExample}</p>
 * 
 * <h3>Subscribing to topics</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String topic;
 *   String URL;
 *   OtpErlangString otpTopic;
 *   OtpErlangString otpURL;
 *   
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   sc.subscribe(topic, URL);       // {@link #subscribe(String, String)}
 *   sc.subscribe(otpTopic, otpURL); // {@link #subscribe(OtpErlangString, OtpErlangString)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see {@link de.zib.scalaris.examples.PubSubSubscribeExample}</p>
 * 
 * <h3>Unsubscribing from topics</h3>
 * 
 * Unsubscribing from topics works like subscribing to topics with the exception
 * of a {@link NotFoundException} being thrown if either the topic does not
 * exist or the URL is not subscribed to the topic.
 * 
 * <pre>
 * <code style="white-space:pre;">
 *   String topic;
 *   String URL;
 *   OtpErlangString otpTopic;
 *   OtpErlangString otpURL;
 *   
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   sc.unsubscribe(topic, URL);       // {@link #unsubscribe(String, String)}
 *   sc.unsubscribe(otpTopic, otpURL); // {@link #unsubscribe(OtpErlangString, OtpErlangString)}
 * </code>
 * </pre>
 * 
 * <h3>Getting a list of subscribers to a topic</h3>
 * <pre>
 * <code style="white-space:pre;">
 *   String topic;
 *   OtpErlangString otpTopic;
 *   
 *   Vector&lt;String&gt; subscribers;
 *   OtpErlangList otpSubscribers;
 *   
 *   // non-static:
 *   TransactionSingleOp sc = new TransactionSingleOp();
 *   subscribers = sc.getSubscribers(topic);             // {@link #getSubscribers(String)}
 *   otpSubscribers = sc.singleGetSubscribers(otpTopic); // {@link #getSubscribers(OtpErlangString)}
 * </code>
 * </pre>
 * 
 * <p>For the full example, see {@link de.zib.scalaris.examples.PubSubGetSubscribersExample}</p>
 * 
 * <h3>Connection errors</h3>
 * 
 * Errors when setting up connections or trying to send/receive RPCs will be
 * handed to the {@link ConnectionPolicy} that has been set when the connection
 * was created. By default, {@link ConnectionFactory} uses
 * {@link DefaultConnectionPolicy} which implements automatic connection-retries
 * by classifying nodes as good or bad depending on their previous state. The
 * number of automatic retries is adjustable (default: 3).
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.5
 * @since 2.5
 */
public class PubSub {
	/**
	 * Connection to a TransactionSingleOp node.
	 */
	private Connection connection;
	
	/**
	 * Constructor, uses the default connection returned by
	 * {@link ConnectionFactory#createConnection()}.
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public PubSub() throws ConnectionException {
		connection = ConnectionFactory.getInstance().createConnection();
	}

	/**
	 * Constructor, uses the given connection to an erlang node.
	 * 
	 * @param conn
	 *            connection to use for the transaction
	 * 
	 * @throws ConnectionException
	 *             if the connection fails
	 */
	public PubSub(Connection conn) throws ConnectionException {
		connection = conn;
	}

	// /////////////////////////////
	// publish methods
	// /////////////////////////////
	
	/**
	 * Publishes an event under a given <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 *
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 */
	public void publish(OtpErlangString topic, OtpErlangString content)
			throws ConnectionException {
		try {
			/**
             * The specification of <tt>api_pubsub:publish/2</tt> states
             * that the only returned value is <tt>ok</tt>, so no further evaluation is
             * necessary.
			 */
			connection
					.doRPC("api_pubsub", "publish", new OtpErlangList(
							new OtpErlangObject[] { topic, content }));
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		}
	}
	
	/**
	 * Publishes an event under a given <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to publish the content under
	 * @param content
	 *            the content to publish
	 *
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 */
	public void publish(String topic, String content)
			throws ConnectionException {
		publish(new OtpErlangString(topic), new OtpErlangString(content));
	}

	// /////////////////////////////
	// subscribe methods
	// /////////////////////////////

	/**
	 * Subscribes a url to a <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
     * @throws AbortException
     *             if the commit of the subscribe failed
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public void subscribe(OtpErlangString topic, OtpErlangString url) throws ConnectionException,
			TimeoutException, AbortException, UnknownException {
		OtpErlangObject received_raw = null;
		try {
			received_raw = connection.doRPC("api_pubsub", "subscribe",
					new OtpErlangList(new OtpErlangObject[] { topic, url }));
			OtpErlangObject received = received_raw;

			/*
			 * possible return values:
			 *   {ok} | {fail, abort | timeout}.
			 */
			if (received.equals(CommonErlangObjects.okTupleAtom)) {
				return;
			} else {
				// {fail, Reason}
				OtpErlangTuple returnValue = (OtpErlangTuple) received;
				OtpErlangAtom failReason = (OtpErlangAtom) returnValue.elementAt(1);

				if (failReason.equals(CommonErlangObjects.timeoutAtom)) {
					throw new TimeoutException(received_raw);
				} else if (failReason.equals(CommonErlangObjects.abortAtom)) {
                    throw new AbortException(received_raw);
                } else {
					throw new UnknownException(received_raw);
				}
			}
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			// received_raw is not null since the first class cast is after the RPC!
			throw new UnknownException(e, received_raw);
		}
	}
	
	/**
	 * Subscribes a url to a <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to subscribe the url to
	 * @param url
	 *            the url of the subscriber (this is where the events are send
	 *            to)
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
     * @throws AbortException
     *             if the commit of the subscribe failed
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public void subscribe(String topic, String url) throws ConnectionException,
			TimeoutException, AbortException, UnknownException {
		subscribe(new OtpErlangString(topic), new OtpErlangString(url));
	}
	
	// /////////////////////////////
	// unsubscribe methods
	// /////////////////////////////

	/**
	 * Unsubscribes a url from a <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
     * @throws AbortException
     *             if the commit of the subscribe failed
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public void unsubscribe(OtpErlangString topic, OtpErlangString url)
			throws ConnectionException, TimeoutException, NotFoundException,
			AbortException, UnknownException {
		OtpErlangObject received_raw = null;
		try {
			received_raw = connection.doRPC("api_pubsub", "unsubscribe",
					new OtpErlangList(new OtpErlangObject[] { topic, url }));
			OtpErlangObject received = received_raw;

            /*
             * possible return values:
             *   {ok} | {fail, abort | timeout | not_found}.
             */
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else {
                // {fail, Reason}
                OtpErlangTuple returnValue = (OtpErlangTuple) received;
                OtpErlangAtom failReason = (OtpErlangAtom) returnValue.elementAt(1);

                if (failReason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (failReason.equals(CommonErlangObjects.abortAtom)) {
                    throw new AbortException(received_raw);
                } else if (failReason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(received_raw);
                } else {
                    throw new UnknownException(received_raw);
                }
            }
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			// received_raw is not null since the first class cast is after the RPC!
			throw new UnknownException(e, received_raw);
		}
	}
	
	/**
	 * Unsubscribes a url from a <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to unsubscribe the url from
	 * @param url
	 *            the url of the subscriber
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws TimeoutException
	 *             if a timeout occurred while trying to write the value
	 * @throws NotFoundException
	 *             if the topic does not exist or the given subscriber is not
	 *             subscribed to the given topic
     * @throws AbortException
     *             if the commit of the subscribe failed
	 * @throws UnknownException
	 *             if any other error occurs
	 */
	public void unsubscribe(String topic, String url)
			throws ConnectionException, TimeoutException, NotFoundException,
			AbortException, UnknownException {
		unsubscribe(new OtpErlangString(topic), new OtpErlangString(url));
	}

	// /////////////////////////////
	// utility methods
	// /////////////////////////////
	
	/**
	 * Converts the given erlang <tt>list</tt> of erlang strings to a Java {@link ArrayList}.
	 */
	private static ArrayList<String> erlStrListToStrArrayList(OtpErlangList list) {
		ArrayList<String> result = new ArrayList<String>(list.arity());
		for (int i = 0; i < list.arity(); ++i) {
			OtpErlangString elem = (OtpErlangString) list.elementAt(i);
			result.add(elem.stringValue());
		}
		return result;
	}

	// /////////////////////////////
	// get subscribers methods
	// /////////////////////////////
	
	/**
	 * Gets a list of subscribers to a <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 *
	 * @return the subscriber URLs
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 */
	public OtpErlangList getSubscribers(
			OtpErlangString topic) throws ConnectionException, UnknownException {
		OtpErlangObject received_raw = null;
		try {
			// return value: [string()]
			received_raw = connection.doRPC("api_pubsub", "get_subscribers",
					new OtpErlangList(topic));
			OtpErlangList received = (OtpErlangList) received_raw;
			return received;
		} catch (OtpErlangExit e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (OtpAuthException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (IOException e) {
			// e.printStackTrace();
			throw new ConnectionException(e);
		} catch (ClassCastException e) {
			// e.printStackTrace();
			// received_raw is not null since the first class cast is after the RPC!
			throw new UnknownException(e, received_raw);
		}
	}
	
	/**
	 * Gets a list of subscribers to a <tt>topic</tt>.
	 * 
	 * @param topic
	 *            the topic to get the subscribers for
	 *
	 * @return the subscriber URLs
	 * 
	 * @throws ConnectionException
	 *             if the connection is not active or a communication error
	 *             occurs or an exit signal was received or the remote node
	 *             sends a message containing an invalid cookie
	 * @throws UnknownException
	 *             is thrown if the return type of the erlang method does not
	 *             match the expected one
	 */
	public ArrayList<String> getSubscribers(
			String topic) throws ConnectionException, UnknownException {
		return erlStrListToStrArrayList(getSubscribers(new OtpErlangString(topic)));
	}
	
	/**
	 * Closes the transaction's connection to a scalaris node.
	 * 
	 * Note: Subsequent calls to the other methods will throw
	 * {@link ConnectionException}s!
	 */
	public void closeConnection() {
		connection.close();
	}
}
