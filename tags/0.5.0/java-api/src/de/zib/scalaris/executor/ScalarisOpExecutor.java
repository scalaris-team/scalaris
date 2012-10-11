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
     * Re-sets the executor as if created from scratch.
     */
    public void reset() {
        ops.clear();
        workPhases = 0;
    }

    /**
     * Executes all operations previously added with {@link #addOp(ScalarisOp)}.
     *
     * @throws OtpErlangException
     *             if an error occurred verifying a result from previous
     *             operations
     * @throws UnknownException
     *             if an error occurred verifying a result from previous
     *             operations
     */
    public void run() throws OtpErlangException, UnknownException {
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
            endWorkPhase(phase, requests);
            if (phase != workPhases) {
                results = executeRequests(requests);
            }
        }
    }

    /**
     * This method is called at the end of each work phase and allows
     * implementing sub-classes to add additional operations.
     *
     * @param phase
     *            the current work phase
     * @param requests
     *            the requests
     */
    protected void endWorkPhase(final int phase, final RequestList requests) {
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

    /**
     * Gets the current list of operations. This is backed by the operations in
     * this executor - if it is reset, the list will be empty.
     *
     * Create a copy of this list if it should be retained.
     *
     * @return the ops the current list of operations
     */
    public ArrayList<ScalarisOp> getOps() {
        return ops;
    }
}
