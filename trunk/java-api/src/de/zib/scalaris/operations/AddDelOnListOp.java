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

import java.util.List;

import com.ericsson.otp.erlang.OtpErlangList;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;

/**
 * Operation appending to / removing from a list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class AddDelOnListOp implements TransactionOperation, TransactionSingleOpOperation {
    final OtpErlangString key;
    final OtpErlangObject toAdd;
    final OtpErlangObject toRemove;
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     */
    public AddDelOnListOp(final OtpErlangString key, final OtpErlangList toAdd, final OtpErlangList toRemove) {
        this.key = key;
        this.toAdd = toAdd;
        this.toRemove = toRemove;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to write the value to
     * @param toAdd
     *            a list of values to add to a list
     * @param toRemove
     *            a list of values to remove from a list
     */
    public <T> AddDelOnListOp(final String key, final List<T> toAdd, final List<T> toRemove) {
        this.key = new OtpErlangString(key);
        this.toAdd = (toAdd == null) ? new OtpErlangList() : (OtpErlangList) ErlangValue.convertToErlang(toAdd);
        this.toRemove = (toRemove == null) ? new OtpErlangList() : (OtpErlangList) ErlangValue.convertToErlang(toRemove);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.addDelOnListAtom, key,
                compressed ? CommonErlangObjects.encode(toAdd) : toAdd,
                compressed ? CommonErlangObjects.encode(toRemove) : toRemove });
    }
    public OtpErlangString getKey() {
        return key;
    }
}
