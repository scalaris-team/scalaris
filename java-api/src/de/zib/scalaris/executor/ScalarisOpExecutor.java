package de.zib.scalaris.executor;

import java.util.ArrayList;
import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;

/**
 * Executes multiple {@link ScalarisOp} operations in multiple phases only
 * sending requests to Scalaris once per work phase.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.13
 * @since 3.13
 */
public abstract class ScalarisOpExecutor {

    /**
     * All operations to be executed.
     */
    protected final ArrayList<ScalarisOp> ops = new ArrayList<ScalarisOp>();
    /**
     * The highest work phase id.
     */
    protected int workPhases = 0;


    /**
     * Adds the given operation to be executed.
     *
     * @param op
     *            the operation to add
     */
    public void addOp(final ScalarisOp op) {
        if (op.workPhases() > workPhases) {
            workPhases = op.workPhases();
        }
        ops.add(op);
    }

    /**
     * Executes all operations previously added with {@link #addOp(ScalarisOp)}.
     *
     * @param commitLast
     *            whether to commit the requests in the last work phase (only
     *            applied if a {@link de.zib.scalaris.Transaction.RequestList}
     *            is used
     *
     * @throws OtpErlangException
     *             if an error occurred verifying a result from previous
     *             operations
     * @throws UnknownException
     *             if an error occurred verifying a result from previous
     *             operations
     */
    public void run(final boolean commitLast) throws OtpErlangException, UnknownException {
        ResultList results = null;
        for (int phase = 0; phase <= workPhases; ++phase) {
            final RequestList requests = newRequestList();
            int curOp = 0;
            for (final ScalarisOp op : ops) {
                // translate the global phase into an operation-specific phase,
                // execute operations as late as possible
                final int opPhase = phase - (workPhases - op.workPhases());
                if ((opPhase >= 0) && (opPhase <= op.workPhases())) {
                    curOp += op.doPhase(opPhase, curOp, results, requests);
                }
            }

            if ((phase == (workPhases - 1)) && commitLast && (requests instanceof de.zib.scalaris.Transaction.RequestList)) {
                requests.addCommit();
            }
            if (phase != workPhases) {
                results = executeRequests(requests);
            }
        }
    }

    /**
     * Creates a new request list.
     *
     * @return an empty request list
     */
    protected abstract RequestList newRequestList();

    /**
     * Executes the given requests.
     *
     * @param requests
     *            a request list to execute
     *
     * @return the results from executing the requests
     *
     * @throws OtpErlangException
     *             if an error occurred verifying a result from previous
     *             operations
     * @throws UnknownException
     *             if an error occurred verifying a result from previous
     *             operations
     */
    protected abstract ResultList executeRequests(RequestList requests)
            throws OtpErlangException, UnknownException;

    /**
     * @return the workPhases
     */
    public int getWorkPhases() {
        return workPhases;
    }
}
