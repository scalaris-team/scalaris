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
/**
 * This package contains means to communicate with the erlang chordsharp ring from Java.
 * 
 * <h3>The ChordSharp class</h3>
 * <p>
 * The {@link de.zib.chordsharp.ChordSharp} class provides simple access methods for the most common use cases.
 * It provides static methods for reading, writing, publishing, subscribing and getting a list of subscribers with
 * normal Java types.
 * </p>
 * 
 * <h4>Example:</h4>
 * <code style="white-space:pre;">
 *   try {
 *     String value = ChordSharp.readString("key");
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
 * 
 * <p>See the {@link de.zib.chordsharp.ChordSharp} class documentation for more details.</p>
 * 
 * <h3>The ChordSharpConnection class</h3>
 * <p>
 * The {@link de.zib.chordsharp.ChordSharpConnection} class provides more sophisticated access methods:
 * It provides methods for reading, writing, publishing, subscribing and getting a list of subscribers with
 * normal Java types and OtpErlang-types. There are static methods which operate on a single static connection to the chordsharp ring and there are also non-static methods which use instance-specific connections.
 * </p>
 * 
 * <h4>Example:</h4>
 * <code style="white-space:pre;">
 *   try {
 *     ChordSharpConnection cs = new ChordSharpConnection();
 *     String value = cs.singleReadString("key");
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
 * 
 * <p>See the {@link de.zib.chordsharp.ChordSharpConnection} class documentation for more details.</p>
 * 
 * <h3>The Transaction class</h3>
 * <p>
 * The {@link de.zib.chordsharp.Transaction} class provides means to realise a chordsharp transaction
 * from Java. After starting a transaction, there are methods to read and write values. The transaction
 * can then be committed or aborted.
 * </p>
 * 
 * <h4>Example:</h4>
 * <code style="white-space:pre;">
 *   try {
 *     Transaction transaction = new Transaction();
 *     transaction.start();
 *     String value = transaction.readString("key");
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
 * 
 * <p>See the {@link de.zib.chordsharp.Transaction} class documentation for more details.</p>
 */
package de.zib.chordsharp;

