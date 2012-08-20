/**
 *  Copyright 2012 Zuse Institute Berlin
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
package de.zib.scalaris.operations;

import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;

/**
 * Operation incrementing a numeric value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class AddOnNrOp implements TransactionOperation, TransactionSingleOpOperation {
    final protected OtpErlangString key;
    final protected OtpErlangObject toAdd;
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     */
    public AddOnNrOp(final OtpErlangString key, final OtpErlangLong toAdd) {
        this.key = key;
        this.toAdd = toAdd;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     */
    public AddOnNrOp(final OtpErlangString key, final OtpErlangDouble toAdd) {
        this.key = key;
        this.toAdd = toAdd;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     */
    public <T extends Number> AddOnNrOp(final String key, final T toAdd) {
        this.key = new OtpErlangString(key);
        this.toAdd = ErlangValue.convertToErlang(toAdd);
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            the number to add to the number stored at key
     */
    public AddOnNrOp(final String key, final Double toAdd) {
        this.key = new OtpErlangString(key);
        this.toAdd = ErlangValue.convertToErlang(toAdd);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.addOnNrAtom, key,
                compressed ? CommonErlangObjects.encode(toAdd) : toAdd });
    }
    public OtpErlangString getKey() {
        return key;
    }

    @Override
    public String toString() {
        return "add_on_nr(" + key + ", " + toAdd + ")";
    }
}
