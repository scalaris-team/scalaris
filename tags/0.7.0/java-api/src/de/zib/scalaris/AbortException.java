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

import java.util.ArrayList;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangException;

/**
 * Exception that is thrown if a the commit of a transaction on a
 * scalaris ring fails.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.12
 * @since 2.5
 */
public class AbortException extends OtpErlangException {
    /**
     * class version for serialisation
     */
    private static final long serialVersionUID = 1L;

    /**
     * List of keys responsible for the abort.
     */
    private final List<String> failedKeys;

    /**
     * Creates the exception with no message.
     *
     * @deprecated abort failures always come with a list of responsible keys,
     *             use {@link #AbortException(List)} instead
     */
    @Deprecated
    public AbortException() {
        this.failedKeys = new ArrayList<String>(0);
    }

    /**
     * Creates the exception with the given message.
     *
     * @param msg
     *            message of the exception
     *
     * @deprecated abort failures always come with a list of responsible keys,
     *             use {@link #AbortException(String, List)} instead
     */
    @Deprecated
    public AbortException(final String msg) {
        super(msg);
        this.failedKeys = new ArrayList<String>(0);
    }

    /**
     * Creates an exception taking the message of the given throwable.
     *
     * @param e the exception to "re-throw"
     *
     * @deprecated abort failures always come with a list of responsible keys,
     *             use {@link #AbortException(Throwable, List)} instead
     */
    @Deprecated
    public AbortException(final Throwable e) {
        super(e.getMessage());
        setStackTrace(e.getStackTrace());
        this.failedKeys = new ArrayList<String>(0);
    }

    /**
     * Creates an exception including the message of the given erlang object.
     *
     * @param responsibleKeys
     *            list of keys responsible for the abort
     *
     * @since 3.12
     */
    public AbortException(final List<String> responsibleKeys) {
        super("Erlang message: {fail, abort, " + responsibleKeys.toString() + "}");
        this.failedKeys = responsibleKeys;
    }

    /**
     * Creates the exception with the given message.
     *
     * @param msg
     *            message of the exception
     * @param responsibleKeys
     *            list of keys responsible for the abort
     *
     * @since 3.12
     */
    public AbortException(final String msg, final List<String> responsibleKeys) {
        super(msg);
        this.failedKeys = new ArrayList<String>(0);
    }

    /**
     * Creates an exception taking the message of the given throwable.
     *
     * @param e
     *            the exception to "re-throw"
     * @param responsibleKeys
     *            list of keys responsible for the abort
     *
     * @since 3.12
     */
    public AbortException(final Throwable e, final List<String> responsibleKeys) {
        super(e.getMessage() + ",\n  Erlang message: {fail, abort, " + responsibleKeys.toString() + "}");
        setStackTrace(e.getStackTrace());
        this.failedKeys = responsibleKeys;
    }

    /**
     * @return the responsibleKeys
     */
    public List<String> getFailedKeys() {
        return failedKeys;
    }
}
