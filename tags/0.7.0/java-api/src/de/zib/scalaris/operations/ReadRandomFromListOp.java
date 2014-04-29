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

import com.ericsson.otp.erlang.OtpErlangDecodeException;
import com.ericsson.otp.erlang.OtpErlangLong;
import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangRangeException;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;
import de.zib.scalaris.EmptyListException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotAListException;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.UnknownException;

/**
 * Operation reading a random entry from a (non-empty) list value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class ReadRandomFromListOp extends PartialReadOp {
    /**
     * Result type of random_from_list operations.
     *
     * @author Nico Kruber, kruber@zib.de
     * @version 3.18
     * @since 3.18
     */
    public static class Result {
        /**
         * A random value from the stored list.
         */
        public final ErlangValue randomElement;
        /**
         * The length of the whole list stored in Scalaris.
         */
        public final int listLength;

        protected Result(final OtpErlangObject result0, final boolean compressed)
                throws OtpErlangDecodeException, UnknownException, ClassCastException {
            // {RandomValue, ListLength}
            OtpErlangTuple result;
            if (compressed) {
                result = (OtpErlangTuple) CommonErlangObjects.decode(result0);
            } else {
                result = (OtpErlangTuple) result0;
            }

            if (result.arity() != 2) {
                throw new UnknownException(result);
            }
            randomElement = new ErlangValue(result.elementAt(0));
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
            return "{randomElement: " + randomElement.toString()
                    + ", listLength: " + listLength + "}";
        }
    }

    /**
     * Constructor
     *
     * @param key
     *            the key to read
     */
    public ReadRandomFromListOp(final OtpErlangString key) {
        super(key);
    }
    /**
     * Constructor
     *
     * @param key
     *            the key to read
     */
    public ReadRandomFromListOp(final String key) {
        super(key);
    }

    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.readAtom, key,
                CommonErlangObjects.randomFromListAtom });
    }

    public Result processResult() throws NotFoundException, EmptyListException,
            NotAListException, UnknownException {
        /*
         * possible return values:
         *  {ok, {RandomValue, ListLength}} | {fail, not_found | empty_list | not_a_list}
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
                } else if (reason.equals(CommonErlangObjects.emptyListAtom)) {
                    throw new EmptyListException(resultRaw);
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
            EmptyListException, NotAListException, UnknownException {
        return processResult();
    }

    @Override
    public String toString() {
        return "readRandomFromList(" + key + ")";
    }
}
