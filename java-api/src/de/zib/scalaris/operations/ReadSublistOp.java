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

import java.math.BigInteger;

import com.ericsson.otp.erlang.OtpErlangInt;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;

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
public class ReadSublistOp extends ReadOp {
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

    @Override
    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.readAtom, key,
                new OtpErlangTuple(new OtpErlangObject[] {
                        CommonErlangObjects.sublistAtom, start, length }) });
    }

    @Override
    public String toString() {
        return "readSublist(" + key + "," + start + "," + length + ")";
    }
}
