/**
 *  Copyright 2013 Zuse Institute Berlin
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

import java.math.BigInteger;

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.UnknownException;

/**
 * Operation reading a sublist from a value.
 *
 * Extracts a sublist of length <tt>Length</tt> starting at <tt>Start</tt>.
 * <ul>
 * <li>If Start is negative, we count from the end, e.g. <tt>-1</tt> is the last
 * element, <tt>-2</tt> the second last.</li>
 * <li>If Length is negative, the sublist is created in reversed direction, e.g.
 * <tt>sublist([a,b,c], -1, -2)</tt> gets <tt>[c, b]</tt>.</li>
 * <li>If Start is less than -ListLength and Length is non-negative, it will be
 * set to <tt>1</tt>. If Length is negative in this case, an empty sublist will
 * be returned.</li>
 * <li>If Start is greater than ListLength and Length is non-negative, an empty
 * sublist will be returned. If Length is negative in this case, it will be set
 * to <tt>ListLength</tt>.</li>
 * <li>Note: sublists never wrap between start and end, i.e.
 * <tt>sublist([a,b,c], 1,
 * -2)</tt> gets <tt>[]</tt>!</li>
 * <li>Examples:
 * <ul>
 * <li>first 10: <tt>sublist(L, 1, 10)</tt> | <tt>sublist(L, 10, -10)</tt>
 * (reverse order)</li>
 * <li>last 10 : <tt>sublist(L, -10, 10)</tt> | <tt>sublist(L, -1, -10)</tt>
 * (reverse order)</li>
 * </ul>
 * </li>
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class ReadSublistOp extends PartialReadOp {
    /**
     * Result type of sublist operations.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.18
     * @since 3.18
     */
    public static class Result {
        /**
         * The retrieved sublist.
         */
        public final ErlangValue subList;
        /**
         * The length of the whole list stored in Scalaris.
         */
        public final int listLength;

        protected Result(final OtpErlangObject result0, final boolean compressed)
                throws OtpErlangDecodeException, UnknownException {
            // {SubList, ListLength}
            OtpErlangTuple result;
            if (compressed) {
                result = (OtpErlangTuple) CommonErlangObjects.decode(result0);
            } else {
                result = (OtpErlangTuple) result0;
            }

            if (result.arity() != 2) {
                throw new UnknownException(result);
            }
            subList = new ErlangValue(ErlangValue.otpObjectToOtpList(result.elementAt(0)));
            final OtpErlangLong listLengthOtp = (OtpErlangLong) result.elementAt(1);
            try {
                listLength = listLengthOtp.intValue();
            } catch (final OtpErlangRangeException e) {
                throw new UnknownException("Unsupported list length ("
                        + listLengthOtp + ")");
            }
        }

        /* (non-Javadoc)
         * @see java.lang.Object#toString()
         */
        @Override
        public String toString() {
            return "{subList: " + subList.toString() + ", listLength: "
                    + listLength + "}";
        }
    }

    final protected OtpErlangInt start;
    final protected OtpErlangInt length;

    /**
     * Constructor
     *
     * @param key
     *            the key to read
     * @param start
     *            the start of the sublist (may be negative - see
     *            {@link ReadSublistOp})
     * @param length
     *            the length of the sublist (may be negative - see
     *            {@link ReadSublistOp})
     */
    public ReadSublistOp(final OtpErlangString key, final OtpErlangInt start, final OtpErlangInt length) {
        super(key);
        assert(!start.bigIntegerValue().equals(BigInteger.ZERO));
        this.start = start;
        this.length = length;
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to read
     * @param start
     *            the start of the sublist (may be negative - see
     *            {@link ReadSublistOp})
     * @param length
     *            the length of the sublist (may be negative - see
     *            {@link ReadSublistOp})
     */
    public ReadSublistOp(final String key, final int start, final int length) {
        super(key);
        assert(start != 0);
        this.start = new OtpErlangInt(start);
        this.length = new OtpErlangInt(length);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.readAtom, key,
                new OtpErlangTuple(new OtpErlangObject[] {
                        CommonErlangObjects.sublistAtom, start, length }) });
    }

    public Result processResult() throws NotFoundException, NotAListException,
            UnknownException {
        /*
         * possible return values:
         *  {ok, {SubList, ListLength}} | {fail, not_found | not_a_list}
         */
        try {
            final OtpErlangTuple received = (OtpErlangTuple) resultRaw;
            final OtpErlangObject state = received.elementAt(0);
            if (received.arity() != 2) {
                throw new UnknownException(resultRaw);
            }
            if (state.equals(CommonErlangObjects.okAtom)) {
                return new Result(received.elementAt(1), resultCompressed);
            } else if (state.equals(CommonErlangObjects.failAtom)) {
                final OtpErlangObject reason = received.elementAt(1);
                if (reason.equals(CommonErlangObjects.notFoundAtom)) {
                    throw new NotFoundException(resultRaw);
                } else if (reason.equals(CommonErlangObjects.notAListAtom)) {
                    throw new NotAListException(resultRaw);
                }
            }
            throw new UnknownException(resultRaw);
        } catch (final ClassCastException e) {
            // e.printStackTrace();
            throw new UnknownException(e, resultRaw);
        } catch (final OtpErlangDecodeException e) {
            // e.printStackTrace();
            throw new UnknownException(e, resultRaw);
        }
    }

    public Result processResultSingle() throws NotFoundException,
            NotAListException, UnknownException {
        return processResult();
    }

    @Override
    public String toString() {
        return "readSublist(" + key + "," + start + "," + length + ")";
    }
}
