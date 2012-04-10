package de.zib.scalaris.examples.wikipedia;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.executor.ScalarisOp;

/**
 * Implements a list change operation using the append operation of
 * Scalaris.
 *
 * @param <T> the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class ScalarisListOp2<T> implements ScalarisOp {
    final String key;
    final String countKey;

    /**
     * Creates a new list change operation.
     *
     * @param key       the key to change the list at
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListOp2(final String key, final String countKey) {
        this.key = key;
        this.countKey = countKey;
    }

    public int workPhases() {
        return 1;
    }

    public final int doPhase(final int phase, final int firstOp,
            final ResultList results, final RequestList requests)
            throws OtpErlangException, UnknownException,
            IllegalArgumentException {
        switch (phase) {
            case 0: return changeList(requests);
            case 1: return checkChange(firstOp, results);
            default:
                throw new IllegalArgumentException("No phase " + phase);
        }
    }

    /**
     * Changes the given page list and its counter (if present).
     *
     * @param requests
     *            the request list
     *
     * @return number of processed operations (should be <tt>0</tt>)
     */
    protected abstract int changeList(final RequestList requests);

    /**
     * Verifies the list change operation.
     *
     * @param firstOp   the first operation to process inside the result list
     * @param results   the result list
     *
     * @return number of processed operations (<tt>1</tt> or <tt>2</tt>)
     */
    protected int checkChange(final int firstOp, final ResultList results)
            throws OtpErlangException, UnknownException {
        int checkedOps = 0;
        results.processAddDelOnListAt(firstOp + checkedOps);
        ++checkedOps;
        if (countKey != null) {
            results.processAddDelOnListAt(firstOp + checkedOps);
            ++checkedOps;
        }
        return checkedOps;
    }
}
