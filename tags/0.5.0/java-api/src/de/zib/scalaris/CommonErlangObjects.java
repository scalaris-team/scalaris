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

import java.util.List;

import com.ericsson.otp.erlang.OtpErlangAtom;
import com.ericsson.otp.erlang.OtpErlangBinary;
import com.ericsson.otp.erlang.OtpErlangBoolean;
import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangDouble;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangTuple;
import com.ericsson.otp.erlang.OtpExternal;
import com.ericsson.otp.erlang.OtpInputStream;
import com.ericsson.otp.erlang.OtpOutputStream;

/**
 * Contains some often used objects as static objects as static members in
 * order to avoid re-creating them each time they are needed.
 *
 * @author Nico Kruber, kruber@zib.de
 *
 * @version 3.14
 * @since 2.5
 */
@SuppressWarnings("javadoc")
public final class CommonErlangObjects {
    public static final OtpErlangAtom readAtom = new OtpErlangAtom("read");
    public static final OtpErlangAtom writeAtom = new OtpErlangAtom("write");
    public static final OtpErlangAtom addDelOnListAtom = new OtpErlangAtom("add_del_on_list");
    public static final OtpErlangAtom testAndSetAtom = new OtpErlangAtom("test_and_set");
    public static final OtpErlangAtom addOnNrAtom = new OtpErlangAtom("add_on_nr");
    public static final OtpErlangAtom okAtom = new OtpErlangAtom("ok");
    public static final OtpErlangAtom failAtom = new OtpErlangAtom("fail");
    public static final OtpErlangAtom abortAtom = new OtpErlangAtom("abort");
    public static final OtpErlangAtom timeoutAtom = new OtpErlangAtom("timeout");
    public static final OtpErlangAtom notFoundAtom = new OtpErlangAtom("not_found");
    public static final OtpErlangAtom keyChangedAtom = new OtpErlangAtom("key_changed");
    public static final OtpErlangAtom notAListAtom = new OtpErlangAtom("not_a_list");
    public static final OtpErlangAtom notANumberAtom = new OtpErlangAtom("not_a_number");
    public static final OtpErlangTuple okTupleAtom = new OtpErlangTuple(okAtom);
    public static final OtpErlangTuple commitTupleAtom = new OtpErlangTuple(new OtpErlangAtom("commit"));

    // JSON
    public static final OtpErlangAtom structAtom = new OtpErlangAtom("struct");
    public static final OtpErlangAtom arrayAtom = new OtpErlangAtom("array");
    public static final OtpErlangAtom trueAtom = new OtpErlangAtom("true");
    public static final OtpErlangAtom falseAtom = new OtpErlangAtom("false");
    public static final OtpErlangAtom nullAtom = new OtpErlangAtom("null");

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as a
     * result from a read operation.
     *
     * @param received_raw
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
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
    static final OtpErlangObject processResult_read(
            final OtpErlangObject received_raw, final boolean compressed)
            throws TimeoutException, NotFoundException, UnknownException {
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
                OtpErlangObject result = received.elementAt(1);
                if (compressed) {
                    result = decode(result);
                }
                return result;
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
        } catch (final OtpErlangDecodeException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Encoded the given erlang object to a binary the same way as
     * <tt>rdht_tx:encode_value/1</tt>.
     *
     * @param value
     *            the decoded value
     *
     * @return the encoded value
     */
    public static OtpErlangObject encode(final OtpErlangObject value) {
        if (value instanceof OtpErlangAtom) {
            return value;
        } else if (value instanceof OtpErlangBoolean) {
            return value;
        } else if (value instanceof OtpErlangLong) {
            return value;
        } else if (value instanceof OtpErlangDouble) {
            return value;
        } else if (value instanceof OtpErlangBinary) {
            final OtpOutputStream oos = new OtpOutputStream();
            oos.write1(OtpExternal.versionTag);
            oos.write_any(value);
            final byte[] encoded = oos.toByteArray();
            return new OtpErlangBinary(encoded);
        } else {
            final OtpOutputStream oos = new OtpOutputStream();
            oos.write1(OtpExternal.versionTag);
            oos.write_compressed(value);
            final byte[] encoded = oos.toByteArray();
            return new OtpErlangBinary(encoded);
        }
    }

    /**
     * Decodes the given Erlang object from a binary to the according
     * {@link OtpErlangObject} the same way as <tt>rdht_tx:decode_value/1</tt>.
     *
     * @param value
     *            the encoded value
     *
     * @return the decoded value
     *
     * @throws OtpErlangDecodeException
     *             if decoding fails
     */
    public static OtpErlangObject decode(final OtpErlangObject value)
            throws OtpErlangDecodeException {
        if (value instanceof OtpErlangBinary) {
            final OtpErlangBinary valueBin = (OtpErlangBinary) value;
            final OtpInputStream ois = new OtpInputStream(valueBin.binaryValue());
            return ois.read_any();
        } else {
            return value;
        }
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as
     * a result from a write operation.
     *
     * @param received_raw
     *             the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws UnknownException
     *             if any other error occurs
     */
    static final void processResult_write(final OtpErlangObject received_raw,
            final boolean compressed) throws TimeoutException, UnknownException {
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
     * a result from a add_del_on_list operation.
     *
     * @param received_raw
     *             the object to process
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
    static final void processResult_addDelOnList(
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

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as
     * a result from an add_on_nr operation.
     *
     * @param received_raw
     *             the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws NotANumberException
     *             if the previously stored value was not a number
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.8
     */
    static final void processResult_addOnNr(final OtpErlangObject received_raw,
            final boolean compressed) throws TimeoutException,
            NotANumberException, UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, timeout} | {fail, not_a_number}.
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.notANumberAtom)) {
                    throw new NotANumberException(received_raw);
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
     * a result from a test_and_set operation.
     *
     * @param received_raw
     *             the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch/write the value
     * @throws NotFoundException
     *             if the requested key does not exist
     * @throws KeyChangedException
     *             if the key did not match <tt>old_value</tt>
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.8
     */
    static final void processResult_testAndSet(
            final OtpErlangObject received_raw, final boolean compressed)
            throws TimeoutException, NotFoundException, KeyChangedException,
            UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, timeout | not_found | {key_changed, RealOldValue}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else if (received.elementAt(0).equals(CommonErlangObjects.failAtom) && (received.arity() == 2)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                } else if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(received_raw);
                } else {
                    final OtpErlangTuple reason_tpl = (OtpErlangTuple) reason;
                    if (reason_tpl.elementAt(0).equals(
                            CommonErlangObjects.keyChangedAtom)
                            && (reason_tpl.arity() == 2)) {
                        OtpErlangObject result = reason_tpl.elementAt(1);
                        if (compressed) {
                            result = decode(result);
                        }
                        throw new KeyChangedException(new ErlangValue(result));
                    }
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        } catch (final OtpErlangDecodeException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang and if it is a
     * <tt>{fail, abort, KeyList}</tt>, issues an {@link AbortException}.
     *
     * @param received_raw
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.8
     */
    static final void checkResult_failAbort(final OtpErlangObject received_raw,
            final boolean compressed) throws AbortException, UnknownException {
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            checkResult_failAbort(received, compressed);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang and if it is a
     * <tt>{fail, abort, KeyList}</tt>, issues an {@link AbortException}.
     *
     * @param received
     *            the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws AbortException
     *             if the commit of the write failed
     * @throws UnknownException
     *             if any other error occurs
     *
     * @since 3.12
     */
    static final void checkResult_failAbort(final OtpErlangTuple received,
            final boolean compressed) throws AbortException, UnknownException {
        try {
            if (received.elementAt(0).equals(CommonErlangObjects.failAtom)
                    && (received.arity() == 3)
                    && received.elementAt(1).equals(CommonErlangObjects.abortAtom)) {
                final List<String> responsibleKeys = new ErlangValue(
                        received.elementAt(2)).stringListValue();
                throw new AbortException(responsibleKeys);
            }
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received);
        }
    }

    /**
     * Processes the <tt>received_raw</tt> term from erlang interpreting it as
     * a result from a commit operation.
     *
     * @param received_raw
     *             the object to process
     * @param compressed
     *            whether the transfer of values is compressed or not
     *
     * @throws TimeoutException
     *             if a timeout occurred while trying to fetch the value
     * @throws AbortException
     *             if the commit of the commit failed
     * @throws UnknownException
     *             if any other error occurs
     */
    static final void processResult_commit(final OtpErlangObject received_raw,
            final boolean compressed) throws TimeoutException, AbortException,
            UnknownException {
        /*
         * possible return values:
         *  {ok} | {fail, abort, KeyList} | {fail, timeout}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) received_raw;
            if (received.equals(CommonErlangObjects.okTupleAtom)) {
                return;
            } else {
                checkResult_failAbort(received, compressed);
                if (received.elementAt(0).equals(CommonErlangObjects.failAtom)
                        && (received.arity() == 2)
                        && received.elementAt(1).equals(CommonErlangObjects.timeoutAtom)) {
                    throw new TimeoutException(received_raw);
                }
            }
            throw new UnknownException(received_raw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, received_raw);
        }
    }
}
