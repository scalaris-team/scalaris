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
/**
 * This package contains means to communicate with the erlang scalaris ring from Java.
 * 
 * <h3>The Scalaris class</h3>
 * <p>
 * The {@link de.zib.scalaris.Scalaris} class provides methods for reading,
 * writing and deleting values, with both,
 * erlang objects ({@link com.ericsson.otp.erlang.OtpErlangObject}) and
 * Java {@link java.lang.String} objects.
 * </p>
 * 
 * <h4>Example:</h4>
 * <pre>
 * <code style="white-space:pre;">
 *   try {
 *     Scalaris sc = new Scalaris();
 *     String value = sc.read("key");
 *   } catch (ConnectionException e) {
 *     System.err.println("read failed: " + e.getMessage());
 *   } catch (TimeoutException e) {
 *     System.err.println("read failed with timeout: " + e.getMessage());
 *   } catch (UnknownException e) {
 *     System.err.println("read failed with unknown: " + e.getMessage());
 *   } catch (NotFoundException e) {
 *     System.err.println("read failed with not found: " + e.getMessage());
 *   }
 * </code>
 * </pre>
 * 
 * <p>See the {@link de.zib.scalaris.Scalaris} class documentation for more details.</p>
 * 
 * <h3>The Transaction class</h3>
 * <p>
 * The {@link de.zib.scalaris.Transaction} class provides means to realise a scalaris transaction
 * from Java. After starting a transaction, there are methods to read and write values with both
 * erlang objects ({@link com.ericsson.otp.erlang.OtpErlangObject}) and Java {@link java.lang.String}
 * objects. The transaction can then be committed, aborted or reset.
 * </p>
 * 
 * <h4>Example:</h4>
 * <pre>
 * <code style="white-space:pre;">
 *   try {
 *     Transaction transaction = new Transaction();
 *     transaction.start();
 *     String value = transaction.read("key");
 *     transaction.write("key", "value");
 *     transaction.commit();
 *   } catch (ConnectionException e) {
 *     System.err.println("read failed: " + e.getMessage());
 *   } catch (TimeoutException e) {
 *     System.err.println("read failed with timeout: " + e.getMessage());
 *   } catch (UnknownException e) {
 *     System.err.println("read failed with unknown: " + e.getMessage());
 *   } catch (NotFoundException e) {
 *     System.err.println("read failed with not found: " + e.getMessage());
 *   } catch (TransactionNotFinishedException e) {
 *     System.out.println("failed: " + e.getMessage());
 *     return;
 *   }
 * </code>
 * </pre>
 * 
 * <p>See the {@link de.zib.scalaris.Transaction} class documentation for more details.</p>
 * 
 * <h3>The PubSub class</h3>
 * <p>
 * The {@link de.zib.scalaris.PubSub} class provides methods for publishing
 * topics, subscribing to urls and getting a list of subscribers with both,
 * erlang strings ({@link com.ericsson.otp.erlang.OtpErlangString}) and
 * Java {@link java.lang.String} strings.
 * </p>
 * 
 * <h4>Example:</h4>
 * <pre>
 * <code style="white-space:pre;">
 *   try {
 *     PubSub sc = new PubSub();
 *     PubSub value = sc.subscribe("topic", "http://localhost:8000");
 *   } catch (ConnectionException e) {
 *     System.err.println("read failed: " + e.getMessage());
 *   } catch (TimeoutException e) {
 *     System.err.println("read failed with timeout: " + e.getMessage());
 *   } catch (UnknownException e) {
 *     System.err.println("read failed with unknown: " + e.getMessage());
 *   }
 * </code>
 * </pre>
 * 
 * <p>See the {@link de.zib.scalaris.PubSub} class documentation for more details.</p>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.5
 * @since 2.0
 */
package de.zib.scalaris;
