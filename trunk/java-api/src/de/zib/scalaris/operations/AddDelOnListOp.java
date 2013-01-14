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
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.UnknownException;

/**
 * Operation appending to / removing from a list.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.14
 * @since 3.14
 */
public class AddDelOnListOp implements TransactionOperation, TransactionSingleOpOperation {
    final protected OtpErlangString key;
    final protected OtpErlangObject toAdd;
    final protected OtpErlangObject toRemove;
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

    @Override
    public String toString() {
        return "add_del_on_list(" + key + ", " + toAdd + ", " + toRemove + ")";
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as a
     * result from a add_del_on_list operation.
     *
     * NOTE: this method should not be called manually by an application and may
     * change without notice!
     *
     * @param received_raw
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws NotAListException
     *             if the previously stored value was no list
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.8
     */
    public static final void processResult_addDelOnList(
            final OtpErlangObject received_raw, final boolean compressed)
            throws TimeoutException, NotAListException, UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, timeout} | {fail, not_a_list}.
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.notAListAtom)) {
                    throw new NotAListException(received_raw);
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }
}
