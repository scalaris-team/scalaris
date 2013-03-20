package de.zib.scalaris.examples.wikipedia;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE;
import de.zib.scalaris.examples.wikipedia.Options.IBuckets;
import de.zib.scalaris.examples.wikipedia.Options.IReadBuckets;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE.WriteCacheDiff;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE.WriteCacheDiffConv;
import de.zib.scalaris.executor.ScalarisOp;
import de.zib.scalaris.operations.ReadOp;

/**
 * Implements a list read operation.
 *
 * @param <T> the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisReadListOp1<T> implements ScalarisOp {
    final Collection<String> keys;
    final int buckets;
    final ErlangConverter<List<T>> listConv;
    final ErlangConverter<T> elemConv;
    final boolean failNotFound;
    final ArrayList<T> value = new ArrayList<T>();
    final private Optimisation optimisation;

    /**
     * Creates a new list read operation.
     * 
     * @param keys
     *            the keys under which the list is stored in Scalaris
     * @param optimisation
     *            the list optimisation to use
     * @param listConv
     *            converter to make an {@link ErlangValue} to a {@link List} of
     *            <tt>T</tt>
     * @param elemConv
     *            converter to make an {@link ErlangValue} to a <tt>T</tt>
     * @param failNotFound
     *            whether to re-throw the {@link NotFoundException} if no list
     *            key was found
     */
    public ScalarisReadListOp1(final Collection<String> keys,
            final Optimisation optimisation, ErlangConverter<List<T>> listConv,
            ErlangConverter<T> elemConv, boolean failNotFound) {
        this.keys = keys;
        if (optimisation instanceof IBuckets) {
            this.buckets = ((IBuckets) optimisation).getBuckets();
        } else {
            this.buckets = 1;
        }
        this.listConv = listConv;
        this.elemConv = elemConv;
        this.failNotFound = failNotFound;
        this.optimisation = optimisation;
    }

    public int workPhases() {
        return 1;
    }

    public final int doPhase(final int phase, final int firstOp,
            final ResultList results, final RequestList requests)
            throws OtpErlangException, UnknownException,
            IllegalArgumentException {
        switch (phase) {
            case 0: return prepareRead(requests);
            case 1: return checkRead(firstOp, results);
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
        for (String key : keys) {
            if (!(optimisation instanceof IBuckets)) {
                requests.addOp(new ReadOp(key));
            } else {
                for (int i = 0; i < buckets; ++i) {
                    requests.addOp(new ReadOp(key + ":" + i));
                }
            }
        }
        return 0;
    }

    /**
     * Verifies the read operation(s) and creates the full list.
     *
     * @param firstOp   the first operation to process inside the result list
     * @param results   the result list
     * @param requests  the request list
     *
     * @return number of processed operations
     */
    protected int checkRead(int firstOp, final ResultList results) throws OtpErlangException,
            UnknownException {
        int notFound = 0;
        NotFoundException lastNotFound = null;
        ErlangConverter<WriteCacheDiff<T>> writeCacheDiffConv = null;
        HashSet<T> toDelete = null;
        if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE) {
            writeCacheDiffConv = new WriteCacheDiffConv<T>(elemConv);
            toDelete = new HashSet<T>();
        }
        for (int x = 0; x < keys.size(); ++x) {
            for (int i = 0; i < buckets; ++i) {
                try {
                    ErlangValue result = results.processReadAt(firstOp++);
                    if (writeCacheDiffConv != null && i >= ((IReadBuckets) optimisation).getReadBuckets()) {
                        WriteCacheDiff<T> diff = writeCacheDiffConv.convert(result);
                        if (value.isEmpty() && !diff.toAdd.isEmpty()) {
                            // assume each bucket has the same size
                            // different keys may have different list sizes though
                            // -> use automatic capacity increase for them
                            value.ensureCapacity(diff.toAdd.size() * (buckets - i));
                        }
                        value.addAll(diff.toAdd);
                        toDelete.addAll(diff.toDelete);
                    } else {
                        final List<T> list = listConv.convert(result);
                        if (value.isEmpty() && !list.isEmpty()) {
                            // assume each bucket has the same size
                            // different keys may have different list sizes though
                            // -> use automatic capacity increase for them
                            value.ensureCapacity(list.size() * (buckets - i));
                        }
                        value.addAll(list);
                    }
                } catch (NotFoundException e) {
                    ++notFound;
                    lastNotFound = e;
                }
            }
        }
        if (toDelete != null && !toDelete.isEmpty()) {
            value.removeAll(toDelete);
        }
        if (failNotFound && notFound == (keys.size() * buckets)) {
            throw lastNotFound;
        }
        return keys.size() * buckets;
    }

    /**
     * The full list that has been read (if {@link #failNotFound} is not set and
     * none of the given keys was found, an empty list is returned).
     * 
     * @return the value from Scalaris or an empty list
     */
    public List<T> getValue() {
        return value;
    }
}
