package de.zib.scalaris.executor;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;

/**
 * Interface for arbitrary Scalaris operations.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.13
 * @since 3.13
 */
public interface ScalarisOp {

    /**
     * Gets the number of work phases needed by this operation (not including
     * the final result verification phase).
     *
     * @return number of required phases
     */
    public abstract int workPhases();

    /**
     * Executes the given phase.
     *
     * @param phase
     *            the number of the current phase
     * @param firstOp
     *            the current operation's index in the result list
     * @param results
     *            the results from the previous operations
     *            (may be <tt>null</tt> if there was none)
     * @param requests
     *            the requests for the next operations
     *            (may be <tt>null</tt> if there are none, i.e. in the
     *            verification phase)
     *
     * @return the number of processed operations from the results list
     *
     * @throws OtpErlangException
     *             if an error occured verifying a result from previous
     *             operations
     * @throws UnknownException
     *             if an error occured verifying a result from previous
     *             operations
     * @throws IllegalArgumentException
     *             if the given work phase is not supported
     *
     * @see #workPhases()
     */
    public abstract int doPhase(int phase, int firstOp, ResultList results,
            RequestList requests) throws OtpErlangException, UnknownException,
            IllegalArgumentException;

    /* (non-Javadoc)
     * @see java.lang.Object#toString()
     */
    public abstract String toString();
}
