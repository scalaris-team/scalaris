/**
 *  Copyright 2011 Zuse Institute Berlin
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
package de.zib.scalaris;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;

/**
 * Contains some often used objects as static objects as static members in
 * order to avoid re-creating them each time they are needed.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 3.4
 * @since 2.5
 */
final class CommonErlangObjects {
    static final OtpErlangAtom readAtom = new OtpErlangAtom("read");
    static final OtpErlangAtom writeAtom = new OtpErlangAtom("write");
    static final OtpErlangAtom okAtom = new OtpErlangAtom("ok");
    static final OtpErlangAtom failAtom = new OtpErlangAtom("fail");
    static final OtpErlangAtom abortAtom = new OtpErlangAtom("abort");
    static final OtpErlangAtom timeoutAtom = new OtpErlangAtom("timeout");
    static final OtpErlangAtom notFoundAtom = new OtpErlangAtom("not_found");
    static final OtpErlangAtom keyChangedAtom = new OtpErlangAtom("key_changed");
    static final OtpErlangTuple okTupleAtom = new OtpErlangTuple(okAtom);
    static final OtpErlangTuple commitTupleAtom = new OtpErlangTuple(new OtpErlangAtom("commit"));

    // JSON
    static final OtpErlangAtom structAtom = new OtpErlangAtom("struct");
    static final OtpErlangAtom arrayAtom = new OtpErlangAtom("array");
    static final OtpErlangAtom trueAtom = new OtpErlangAtom("true");
    static final OtpErlangAtom falseAtom = new OtpErlangAtom("false");
    static final OtpErlangAtom nullAtom = new OtpErlangAtom("null");

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as
     * a result from a read operation.
     *
     * @param received_raw
     *             the object to process
     *
     * @return the contained value
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws UnknownException
     *             if any other error occurs
     */
    static final OtpErlangObject processResult_read(final OtpErlangObject received_raw) throws TimeoutException, NotFoundException, UnknownException {
        /*
         * possible return values:
         *  {ok, Value} | {fail, timeout | not_found}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            final OtpErlangObject state = received.elementAt(0);
            if (received.arity() != 2) {
                throw new UnknownException(received_raw);
            }
            if (state.equals(CommonErlangObjects.okAtom)) {
                return received.elementAt(1);
            } else if (state.equals(CommonErlangObjects.failAtom)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(received_raw);
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as
     * a result from a write operation.
     *
     * @param received_raw
     *             the object to process
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws UnknownException
     *             if any other error occurs
     */
    static final void processResult_write(final OtpErlangObject received_raw) throws TimeoutException, UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, timeout}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as
     * a result from a commit operation.
     *
     * @param received_raw
     *             the object to process
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws AbortException
     *             if the commit of the commit failed
     * @throws UnknownException
     *             if any other error occurs
     */
    static final void processResult_commit(final OtpErlangObject received_raw) throws TimeoutException, AbortException, UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, timeout | abort}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.abortAtom)) {
                    throw new AbortException(received_raw);
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }
}
