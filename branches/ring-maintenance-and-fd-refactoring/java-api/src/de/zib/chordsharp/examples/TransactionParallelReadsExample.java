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

import java.util.Vector;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.chordsharp.ConnectionException;
import de.zib.chordsharp.Transaction;
import de.zib.chordsharp.TransactionNotFinishedException;
import de.zib.chordsharp.UnknownException;

/**
 * Provides an example for using the {@code parallelReads} method of the
 * {@link Transaction} class.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.0
 */
public class TransactionParallelReadsExample {
	/**
	 * Reads all keys given on the command line (given as "key1 key2 ...") with
	 * the {@link Transaction#parallelReads(String[])} and
	 * {@link Transaction#parallelReads(OtpErlangList)} methods in a single
	 * transaction.<br />
	 * If no keys are given, the default keys {@code "key1"}, {@code "key2"},
	 * {@code "key3"} are used.
	 * 
	 * @param args
	 *            command line arguments (optional keys to look up)
	 */
	public static void main(String[] args) {
		String[] keys;
		Vector<String> values;
		
		if (args.length == 0) {
			keys = new String[] { "key1", "key2", "key3" };
		} else {
			keys = args;
		}

		OtpErlangString[] otpKeys_temp = new OtpErlangString[keys.length];
		for (int i = 0; i < keys.length; ++i) {
			otpKeys_temp[i] = new OtpErlangString(keys[i]);
		}
		OtpErlangList otpKeys = (new OtpErlangList(otpKeys_temp));
		OtpErlangList otpValues;

		System.out
				.println("Reading values with the class `Transaction`:");
		
		System.out.print("    Initialising Transaction object... ");
		try {
			Transaction transaction = new Transaction();
			System.out.println("done");
			
			System.out.print("    Starting transaction... ");
			transaction.start();
			System.out.println("done");
			
			System.out.println("    `OtpErlangList parallelReads(OtpErlangList)`...");
			try {
				otpValues = transaction.parallelReads(otpKeys);
				System.out.println("      parallelReads(" + otpKeys.toString()
						+ ") == " + otpValues.toString());
			} catch (ConnectionException e) {
				System.out.println("      parallelReads(" + otpKeys.toString()
						+ ") failed: " + e.getMessage());
			} catch (UnknownException e) {
				System.out.println("      parallelReads(" + otpKeys.toString()
						+ ") failed with unknown: " + e.getMessage());
			}
			
			System.out.println("    `Vector<String> parallelReads(String[])`...");
			try {
				values = transaction.parallelReads(keys);
				System.out.println("      parallelReads(" + keys + ") == " + values);
			} catch (ConnectionException e) {
				System.out.println("      parallelReads(" + keys + ") failed: "
						+ e.getMessage());
			} catch (UnknownException e) {
				System.out.println("      parallelReads(" + keys
						+ ") failed with unknown: " + e.getMessage());
			}
			
			System.out.print("    Committing transaction... ");
			transaction.commit();
			System.out.println("done");
		} catch (ConnectionException e) {
			System.out.println("failed: " + e.getMessage());
			return;
		} catch (TransactionNotFinishedException e) {
			System.out.println("failed: " + e.getMessage());
			return;
		} catch (UnknownException e) {
			System.out.println("failed: " + e.getMessage());
			return;
		}
	}

}
