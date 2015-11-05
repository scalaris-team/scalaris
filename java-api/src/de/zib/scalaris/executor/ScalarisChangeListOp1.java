package de.zib.scalaris.executor;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.operations.ReadOp;
import de.zib.scalaris.operations.WriteOp;

/**
 * Implements a list change operation using the read and write operations of
 * Scalaris. Supports an (optional) list counter key which is updated
 * accordingly.
 *
 * @author Nico Kruber, kruber@zib.de
 * @version 3.18
 * @since 3.18
 */
public abstract class ScalarisChangeListOp1 implements ScalarisOp {
    /**
     * Key used to store the list.
     */
    protected final String key;
    /**
     * Key used to store the list counter.
     */
    protected final String countKey;
    /**
     * Sub-classes need to set this variable in {@link #changeList(List)},
     * otherwise the list is not written back.
     */
    protected boolean listChanged = false;
    /**
     * Sub-classes need to set this variable in {@link #changeList(List)},
     * otherwise the list is not written back.
     */
    protected boolean listCountChanged = false;

    /**
     * Creates a new list change operation.
     *
     * @param key       the key to change the list at
     * @param countKey  the key for the counter of the entries in the list
     *                  (may be <tt>null</tt>)
     */
    public ScalarisChangeListOp1(final String key, final String countKey) {
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
        requests.addOp(new ReadOp(key));
        return 0;
    }

    /**
     * Verifies the read operation, changes the list and adds write operations
     * to the request list: one for the list, one for the counter (if present).
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
        assert results != null;
        List<ErlangValue> pageList;
        try {
            pageList = results.processReadAt(firstOp).listValue();
        } catch (final NotFoundException e) {
            // this is ok
            pageList = new LinkedList<ErlangValue>();
        }
        pageList = changeList(pageList);
        if (listChanged) {
            requests.addOp(new WriteOp(key, pageList));
            if ((countKey != null) && listCountChanged) {
                requests.addOp(new WriteOp(countKey, pageList.size()));
            }
        }
        return 1;
    }

    /**
     * Changes the given page list.
     *
     * @param pageList
     *            the page list to change
     *
     * @return the new list (may be the same object as <tt>pageList</tt>)
     */
    protected abstract List<ErlangValue> changeList(List<ErlangValue> pageList);

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

        if (listChanged) {
            assert results != null;
            results.processWriteAt(firstOp + checkedOps);
            ++checkedOps;
            if ((countKey != null) && listCountChanged) {
                results.processWriteAt(firstOp + checkedOps);
                ++checkedOps;
            }
        }
        return checkedOps;
    }

    /**
     * Converts a list of <tt>T</tt> to a list of {@link ErlangValue} objects.
     *
     * @param list
     *            the list to convert
     *
     * @throws ClassCastException
     *             if the conversion fails
     */
    protected static <T> List<ErlangValue> toErlangValueList(final List<T> list)
            throws ClassCastException {
        final List<ErlangValue> result = new ArrayList<ErlangValue>(list.size());
        for (final T t : list) {
            result.add(new ErlangValue(t));
        }
        return result;
    }
}
