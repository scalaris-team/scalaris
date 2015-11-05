package de.zib.scalaris.executor;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.operations.AddOnNrOp;

/**
 * Implements a list change operation using the append operation of Scalaris.
 * Supports an (optional) list counter key which is updated accordingly.
 *
 * Sub-classes need to override {@link #changeList(RequestList)} to perform the
 * changes and issue a <em>single</em> {@link AddOnNrOp} operation and
 * (optionally) a second {@link AddOnNrOp} for a list counter key!
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public abstract class ScalarisChangeListOp2 implements ScalarisOp {
    /**
     * Key used to store the list.
     */
    protected final String key;
    /**
     * Key used to store the list counter.
     */
    protected final String countKey;

    /**
     * Creates a new list change operation.
     *
     * @param key       the key to change the list at
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisChangeListOp2(final String key, final String countKey) {
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
     * Sub-classes overriding this method need to perform the changes and issue
     * a <em>single</em> {@link AddOnNrOp} operation and (optionally) a second
     * {@link AddOnNrOp} for a list counter key!
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
        assert results != null;
        int checkedOps = 0;
        results.processAddDelOnListAt(firstOp + checkedOps);
        ++checkedOps;
        if (countKey != null) {
            results.processAddOnNrAt(firstOp + checkedOps);
            ++checkedOps;
        }
        return checkedOps;
    }
}
