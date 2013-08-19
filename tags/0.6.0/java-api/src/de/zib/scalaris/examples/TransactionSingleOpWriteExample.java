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

import de.zib.scalaris.AbortException;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the <tt>write</tt> methods of the
 * {@link TransactionSingleOp} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class TransactionSingleOpWriteExample {
    /**
     * Writes a key/value pair given on the command line with the <tt>write</tt>
     * methods of {@link TransactionSingleOp}.<br />
     * If no value or key is given, the default key <tt>"key"</tt> and the
     * default value <tt>"value"</tt> is used.
     *
     * @param args
     *            command line arguments (first argument can be an optional key
     *            and the second an optional value)
     */
    public static void main(final String[] args) {
        String key;
        String value;

        if (args.length == 0) {
            key = "key";
            value = "value";
        } else if (args.length == 1) {
            key = args[0];
            value = "value";
        } else {
            key = args[0];
            value = args[1];
        }

        final OtpErlangString otpKey = new OtpErlangString(key);
        final OtpErlangString otpValue = new OtpErlangString(value);

        System.out.println("Writing values with the class `TransactionSingleOp`:");

        try {
            System.out.println("  creating object...");
            final TransactionSingleOp sc = new TransactionSingleOp();
            System.out
                    .println("    `void writeObject(OtpErlangString, OtpErlangObject)`...");
            sc.write(otpKey, otpValue);
            System.out.println("      write(" + otpKey.stringValue() + ", "
                    + otpValue.stringValue() + ") succeeded");
        } catch (final ConnectionException e) {
            System.out.println("      write(" + otpKey.stringValue() + ", "
                    + otpValue.stringValue() + ") failed: " + e.getMessage());
        } catch (final AbortException e) {
            System.out.println("      write(" + otpKey.stringValue() + ", "
                    + otpValue.stringValue() + ") failed with abort: "
                    + e.getMessage());
        } catch (final UnknownException e) {
            System.out.println("      write(" + otpKey.stringValue() + ", "
                    + otpValue.stringValue() + ") failed with unknown: "
                    + e.getMessage());
        }

        try {
            System.out.println("  creating object...");
            final TransactionSingleOp sc = new TransactionSingleOp();
            System.out.println("    `void write(String, String)`...");
            sc.write(key, value);
            System.out.println("      write(" + key + ", " + value
                    + ") succeeded");
        } catch (final ConnectionException e) {
            System.out.println("      write(" + key + ", " + value
                    + ") failed: " + e.getMessage());
        } catch (final AbortException e) {
            System.out.println("      write(" + otpKey.stringValue() + ", "
                    + otpValue.stringValue() + ") failed with abort: "
                    + e.getMessage());
        } catch (final UnknownException e) {
            System.out.println("      write(" + key + ", " + value
                    + ") failed with unknown: " + e.getMessage());
        }
    }
}
