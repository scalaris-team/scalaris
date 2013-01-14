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

import com.ericsson.otp.erlang.OtpErlangObject;
import com.ericsson.otp.erlang.OtpErlangString;
import com.ericsson.otp.erlang.OtpErlangTuple;

import de.zib.scalaris.CommonErlangObjects;

/**
 * Operation reading a random entry from a (non-empty) list value.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public class ReadRandomFromListOp extends ReadOp {

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

    @Override
    public OtpErlangObject getErlang(final boolean compressed) {
        return new OtpErlangTuple(new OtpErlangObject[] {
                CommonErlangObjects.readAtom, key,
                CommonErlangObjects.randomFromListAtom });
    }

    @Override
    public String toString() {
        return "readRandomFromList(" + key + ")";
    }
}
