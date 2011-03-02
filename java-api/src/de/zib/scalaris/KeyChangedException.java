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
package de.zib.scalaris;

import com.ericsson.otp.erlang.OtpErlangException;
import com.ericsson.otp.erlang.OtpErlangObject;

/**
 * Exception that is thrown when a test_and_set operation on a scalaris ring
 * fails because the old value did not match the expected value.
 * 
 * Contains the old value stored in scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 2.7
 * @since 2.7
 */
public class KeyChangedException extends OtpErlangException {
    /**
     * class version for serialisation
     */
    private static final long serialVersionUID = 1L;

    /**
     * The value stored in scalaris.
     */
    private OtpErlangObject oldValue;

    /**
     * Creates the exception with the given old value.
     * 
     * @param old_value
     *            the old value stored in scalaris
     */
    public KeyChangedException(OtpErlangObject old_value) {
        super();
        this.oldValue = old_value;
    }

    /**
     * Creates the exception with the given old value taking the message of the
     * given throwable.
     * 
     * @param e
     *            the exception to "re-throw"
     * @param old_value
     *            the old value stored in scalaris
     */
    public KeyChangedException(Throwable e, OtpErlangObject old_value) {
        super(e.getMessage());
        this.oldValue = old_value;
        setStackTrace(e.getStackTrace());
    }

    /**
     * Returns the (old) value stored in scalaris.
     * 
     * @return the value
     */
    public OtpErlangObject getOldValue() {
        return oldValue;
    }
}
