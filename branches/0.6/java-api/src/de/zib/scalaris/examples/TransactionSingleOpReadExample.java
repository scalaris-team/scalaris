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

import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;

/**
 * Provides an example for using the <tt>read</tt> methods of the
 * {@link TransactionSingleOp} class.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 2.0
 * @since 2.0
 */
public class TransactionSingleOpReadExample {
    /**
     * Reads a key given on the command line with the <tt>read</tt> methods of
     * {@link TransactionSingleOp}.<br />
     * If no key is given, the default key <tt>"key"</tt> is used.
     *
     * @param args
     *            command line arguments (first argument can be an optional key
     *            to look up)
     */
    public static void main(final String[] args) {
        String key;
        String value;

        if (args.length != 1) {
            key = "key";
        } else {
            key = args[0];
        }

        final OtpErlangString otpKey = new OtpErlangString(key);
        OtpErlangString otpValue;

        System.out.println("Reading values with the class `TransactionSingleOp`:");

        try {
            System.out.println("  creating object...");
            final TransactionSingleOp sc = new TransactionSingleOp();
            System.out
                    .println("    `OtpErlangObject readObject(OtpErlangString)`...");
            otpValue = (OtpErlangString) sc.read(otpKey).value();
            System.out.println("      read(" + otpKey.stringValue() + ") == "
                    + otpValue.stringValue());
        } catch (final ConnectionException e) {
            System.out.println("      read(" + otpKey.stringValue()
                    + ") failed: " + e.getMessage());
        } catch (final NotFoundException e) {
            System.out.println("      read(" + otpKey.stringValue()
                    + ") failed with not found: " + e.getMessage());
        } catch (final ClassCastException e) {
            System.out.println("      read(" + otpKey.stringValue()
                    + ") failed with unexpected return type: " + e.getMessage());
        } catch (final UnknownException e) {
            System.out.println("      read(" + otpKey.stringValue()
                    + ") failed with unknown: " + e.getMessage());
        }

        try {
            System.out.println("  creating object...");
            final TransactionSingleOp sc = new TransactionSingleOp();
            System.out.println("    `String read(String)`...");
            value = sc.read(key).stringValue();
            System.out.println("      read(" + key + ") == " + value);
        } catch (final ConnectionException e) {
            System.out.println("      read(" + key + ") failed: "
                    + e.getMessage());
        } catch (final ClassCastException e) {
            System.out.println("      read(" + key + ") failed with unexpected return type: "
                    + e.getMessage());
        } catch (final NotFoundException e) {
            System.out.println("      read(" + key + ") failed with not found: "
                    + e.getMessage());
        } catch (final UnknownException e) {
            System.out.println("      read(" + key + ") failed with unknown: "
                    + e.getMessage());
        }
    }
}
