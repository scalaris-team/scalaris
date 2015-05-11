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
 * <h3>The TransactionSingleOp class</h3>
 * <p>
 * The {@link de.zib.scalaris.TransactionSingleOp} class provides methods for
 * reading and writing values, with both,
 * erlang objects ({@link com.ericsson.otp.erlang.OtpErlangObject}) and
 * Java objects like {@link java.lang.String}.
 * </p>
 *
 * <h4>Example:</h4>
 * <pre>
 * <code style="white-space:pre;">
 *   try {
 *     TransactionSingleOp sc = new TransactionSingleOp();
 *     String value = sc.read("key").stringValue();
 *   } catch (ConnectionException e) {
 *     System.err.println("read failed: " + e.getMessage());
 *   } catch (NotFoundException e) {
 *     System.err.println("read failed with not found: " + e.getMessage());
 *   } catch (ClassCastException e) {
 *     System.err.println("read failed with unexpected return type: " + e.getMessage());
 *   } catch (UnknownException e) {
 *     System.err.println("read failed with unknown: " + e.getMessage());
 *   }
 * </code>
 * </pre>
 *
 * <p>See the {@link de.zib.scalaris.TransactionSingleOp} class documentation
 * for more details.</p>
 *
 * <h3>The Transaction class</h3>
 * <p>
 * The {@link de.zib.scalaris.Transaction} class provides means to realise a
 * scalaris transaction from Java. There are methods to read and write values
 * with both erlang objects ({@link com.ericsson.otp.erlang.OtpErlangObject})
 * and Java objects like {@link java.lang.String}. The transaction can then be
 * committed or aborted.
 * </p>
 *
 * <h4>Example:</h4>
 * <pre>
 * <code style="white-space:pre;">
 *   try {
 *     Transaction transaction = new Transaction();
 *     String value = transaction.read("key").stringValue();
 *     transaction.write("key", "value");
 *     transaction.commit();
 *   } catch (ConnectionException e) {
 *     System.err.println("read failed: " + e.getMessage());
 *   } catch (NotFoundException e) {
 *     System.err.println("read failed with not found: " + e.getMessage());
 *   } catch (UnknownException e) {
 *     System.err.println("read failed with unknown: " + e.getMessage());
 *   }
 * </code>
 * </pre>
 *
 * <p>See the {@link de.zib.scalaris.Transaction} class documentation for more
 * details.</p>
 *
 * <h3>The ReplicatedDHT class</h3>
 * <p>
 * The {@link de.zib.scalaris.ReplicatedDHT} class provides methods for
 * working inconsistently on replicated key/value pairs, e.g. to delete
 * replicas. It supports both, erlang strings
 * ({@link com.ericsson.otp.erlang.OtpErlangString}) and Java strings
 * ({@link java.lang.String}).
 * </p>
 *
 * <h4>Example:</h4>
 * <pre>
 * <code style="white-space:pre;">
 *   try {
 *     ReplicatedDHT sc = new ReplicatedDHT();
 *     long deleted = sc.delete("key");
 *     DeleteResult delRes = sc.getLastDeleteResult();
 *   } catch (ConnectionException e) {
 *     System.err.println("delete failed: " + e.getMessage());
 *   } catch (TimeoutException e) {
 *     System.err.println("delete failed with timeout: " + e.getMessage());
 *   } catch (NodeNotFoundException e) {
 *     System.err.println("delete failed with node not found: " + e.getMessage());
 *   } catch (UnknownException e) {
 *     System.err.println("delete failed with unknown: " + e.getMessage());
 *   }
 * </code>
 * </pre>
 *
 * <p>See the {@link de.zib.scalaris.ReplicatedDHT} class documentation for
 * more details.</p>
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.9
 * @since 2.0
 */
package de.zib.scalaris;
