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
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the <code>write</code> method of the
 * {@link Transaction} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class TransactionWriteExample {
    /**
     * Writes all key/value pairs given on the command line (given as "key1
     * value1 key2 value2 ...") with the
     * {@link Transaction#write(String, Object)} and
     * {@link Transaction#write(OtpErlangString, OtpErlangObject)} methods
     * in a single transaction.<br />
     * If no key/value pair is given, the default pairs <code>(key1, value1)</code>,
     * <code>(key2, value2)</code> and <code>(key3, value3)</code> are used.
     *
     * @param args
     *            command line arguments (optional key/value pairs to store)
     */
    public static void main(final String[] args) {
        String[] keys;
        String[] values;

        if ((args.length < 2) || ((args.length % 2) != 0)) {
            keys = new String[] { "key1", "key2", "key3" };
            values = new String[] { "value1", "value2", "value3" };
        } else {
            keys = new String[args.length / 2];
            values = new String[args.length / 2];
            for (int i = 0; i < args.length;) {
                keys[i] = args[i++];
                values[i] = args[i++];
            }
        }

        final OtpErlangString[] otpKeys_temp = new OtpErlangString[keys.length];
        for (int i = 0; i < keys.length; ++i) {
            otpKeys_temp[i] = new OtpErlangString(keys[i]);
        }
        final OtpErlangList otpKeys = (new OtpErlangList(otpKeys_temp));

        final OtpErlangString[] otpValues_temp = new OtpErlangString[values.length];
        for (int i = 0; i < values.length; ++i) {
            otpValues_temp[i] = new OtpErlangString(values[i]);
        }
        final OtpErlangList otpValues = (new OtpErlangList(otpValues_temp));

        System.out.println("Writing values with the class `Transaction`:");

        System.out.print("    Initialising Transaction object... ");
        try {
            final Transaction transaction = new Transaction();
            System.out.println("done");

            System.out.print("    Starting transaction... ");
            System.out.println("done");

            System.out
                    .println("    `writeObject(OtpErlangString, OtpErlangObject)`...");
            for (int i = 0; i < otpKeys.arity(); ++i) {
                final OtpErlangString otpKey = (OtpErlangString) otpKeys.elementAt(i);
                final OtpErlangString otpValue = (OtpErlangString) otpValues
                        .elementAt(i);
                try {
                    transaction.write(otpKey, otpValue);
                    System.out.println("      write(" + otpKey.stringValue()
                            + ", " + otpValue.stringValue() + ") succeeded");
                } catch (final ConnectionException e) {
                    System.out.println("      write(" + otpKey.stringValue()
                            + ", " + otpValue.stringValue() + ") failed: "
                            + e.getMessage());
                } catch (final UnknownException e) {
                    System.out.println("      write(" + otpKey.stringValue()
                            + ", " + otpValue.stringValue()
                            + ") failed with unknown: " + e.getMessage());
                }
            }

            System.out.println("    `write(String, String)`...");
            for (int i = 0; i < keys.length; ++i) {
                final String key = keys[i];
                final String value = values[i];
                try {
                    transaction.write(key, value);
                    System.out.println("      write(" + key + ", " + value
                            + ") succeeded");
                } catch (final ConnectionException e) {
                    System.out.println("      write(" + key + ", " + value
                            + ") failed: " + e.getMessage());
                } catch (final UnknownException e) {
                    System.out.println("      write(" + key + ", " + value
                            + ") failed with unknown: " + e.getMessage());
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
