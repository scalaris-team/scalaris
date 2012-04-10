package de.zib.scalaris.examples.wikipedia;

import java.util.LinkedList;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.executor.ScalarisOp;

/**
 * Implements a list change operation using the read and write operations of
 * Scalaris.
 *
 * @param <T> the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class ScalarisListOp1<T> implements ScalarisOp {
    final String key;
    final String countKey;

    /**
     * Creates a new list change operation.
     *
     * @param key       the key to change the list at
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisListOp1(final String key, final String countKey) {
        this.key = key;
        this.countKey = countKey;
    }

    public int workPhases() {
        return 2;
    }

    public final int doPhase(final int phase, final int firstOp,
            final ResultList results, final RequestList requests)
            throws OtpErlangException, UnknownException,
            IllegalArgumentException {
        switch (phase) {
            case 0: return prepareRead(requests);
            case 1: return prepareWrite(firstOp, results, requests);
            case 2: return checkWrite(firstOp, results);
            default:
                throw new IllegalArgumentException("No phase " + phase);
        }
    }

    /**
     * Adds a read operation for the list to the request list.
     *
     * @param requests the request list
     *
     * @return <tt>0</tt> (no operation processed since no results are used)
     */
    protected int prepareRead(final RequestList requests) {
        requests.addRead(key);
        return 0;
    }

    /**
     * Verifies the read operation, changes the list and adds a write operation
     * to the request list.
     *
     * @param firstOp   the first operation to process inside the result list
     * @param results   the result list
     * @param requests  the request list
     *
     * @return <tt>1</tt> operation processed (the read)
     */
    protected int prepareWrite(final int firstOp, final ResultList results,
            final RequestList requests) throws OtpErlangException,
            UnknownException {
        List<ErlangValue> pageList;
        try {
            pageList = results.processReadAt(firstOp).listValue();
        } catch (final NotFoundException e) {
            // this is ok
            pageList = new LinkedList<ErlangValue>();
        }
        changeList(pageList);
        requests.addWrite(key, pageList);
        if (countKey != null) {
            requests.addWrite(countKey, pageList.size());
        }
        return 1;
    }

    /**
     * Changes the given page list.
     *
     * @param pageList
     *            the original page list
     */
    protected abstract void changeList(List<ErlangValue> pageList);

    /**
     * Verifies the write operations.
     *
     * @param firstOp   the first operation to process inside the result list
     * @param results   the result list
     *
     * @return number of processed operations (<tt>1</tt> or <tt>2</tt>)
     */
    protected int checkWrite(final int firstOp, final ResultList results)
            throws OtpErlangException, UnknownException {
        int checkedOps = 0;
        results.processWriteAt(firstOp + checkedOps);
        ++checkedOps;
        if (countKey != null) {
            results.processWriteAt(firstOp + checkedOps);
            ++checkedOps;
        }
        return checkedOps;
    }
}
