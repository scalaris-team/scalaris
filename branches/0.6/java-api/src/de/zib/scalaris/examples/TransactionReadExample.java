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

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the <code>read</code> method of the
 * {@link Transaction} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class TransactionReadExample {
    /**
     * Reads all keys given on the command line (given as "key1 key2 ...") with
     * the {@link Transaction#read(String)} and
     * {@link Transaction#read(OtpErlangString)} methods in a single
     * transaction.<br />
     * If no keys are given, the default keys <code>"key1"</code>, <code>"key2"</code>,
     * <code>"key3"</code> are used.
     *
     * @param args
     *            command line arguments (optional keys to look up)
     */
    public static void main(final String[] args) {
        String[] keys;
        String value;

        if (args.length == 0) {
            keys = new String[] { "key1", "key2", "key3" };
        } else {
            keys = args;
        }

        final OtpErlangString[] otpKeys_temp = new OtpErlangString[keys.length];
        for (int i = 0; i < keys.length; ++i) {
            otpKeys_temp[i] = new OtpErlangString(keys[i]);
        }
        final OtpErlangList otpKeys = (new OtpErlangList(otpKeys_temp));
        OtpErlangString otpValue;

        System.out.println("Reading values with the class `Transaction`:");

        System.out.print("    Initialising Transaction object... ");
        try {
            final Transaction transaction = new Transaction();
            System.out.println("done");

            System.out.print("    Starting transaction... ");
            System.out.println("done");

            System.out
                    .println("    `OtpErlangObject readObject(OtpErlangString)`...");
            for (int i = 0; i < otpKeys.arity(); ++i) {
                final OtpErlangString otpKey = (OtpErlangString) otpKeys.elementAt(i);
                try {
                    otpValue = (OtpErlangString) transaction.read(otpKey).value();
                    System.out.println("      read(" + otpKey.stringValue()
                            + ") == " + otpValue.stringValue());
                } catch (final ConnectionException e) {
                    System.out.println("      read(" + otpKey.stringValue()
                            + ") failed: " + e.getMessage());
                } catch (final UnknownException e) {
                    System.out.println("      read(" + otpKey.stringValue()
                            + ") failed with unknown: " + e.getMessage());
                } catch (final NotFoundException e) {
                    System.out.println("      read(" + otpKey.stringValue()
                            + ") failed with not found: " + e.getMessage());
                } catch (final ClassCastException e) {
                    System.out.println("      read(" + otpKey.stringValue()
                            + ") failed with unknown return type: " + e.getMessage());
                }
            }

            System.out.println("    `String read(String)`...");
            for (final String key : keys) {
                try {
                    value = transaction.read(key).stringValue();
                    System.out.println("      read(" + key + ") == " + value);
                } catch (final ConnectionException e) {
                    System.out.println("      read(" + key + ") failed: "
                            + e.getMessage());
                } catch (final UnknownException e) {
                    System.out.println("      read(" + key
                            + ") failed with unknown: " + e.getMessage());
                } catch (final NotFoundException e) {
                    System.out.println("      read(" + key
                            + ") failed with not found: " + e.getMessage());
                }
            }

            System.out.print("    Committing transaction... ");
            transaction.commit();
            System.out.println("done");
        } catch (final ConnectionException e) {
            System.out.println("failed: " + e.getMessage());
            return;
        } catch (final AbortException e) {
            System.out.println("failed: " + e.getMessage());
            return;
        } catch (final UnknownException e) {
            System.out.println("failed: " + e.getMessage());
            return;
        }
    }

}
