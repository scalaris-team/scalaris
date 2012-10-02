package de.zib.scalaris.examples.wikipedia;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.executor.ScalarisOp;
import de.zib.scalaris.operations.ReadOp;

/**
 * Implements a random list entry read operation.
 *
 * @param <T> the type of objects in the list
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisReadRandomListEntryOp1<T> implements ScalarisOp {
    final Collection<String> keys;
    final int buckets;
    final ErlangConverter<List<T>> conv;
    final boolean failNotFound;
    T value = null;
    final Random random;

    /**
     * Creates a new (random) list entry read operation.
     * 
     * @param keys
     *            the keys under which the list is stored in Scalaris
     * @param optimisation
     *            the list optimisation to use
     * @param conv
     *            converter to make an {@link ErlangValue} to a {@link List} of
     *            <tt>T</tt>
     * @param failNotFound
     *            whether to re-throw the {@link NotFoundException} if a list
     *            key was not found
     * @param random
     *            the random number generator to use
     */
    public ScalarisReadRandomListEntryOp1(final Collection<String> keys,
            final Optimisation optimisation, ErlangConverter<List<T>> conv,
            boolean failNotFound, Random random) {
        this.keys = keys;
        if (optimisation instanceof APPEND_INCREMENT_BUCKETS) {
            APPEND_INCREMENT_BUCKETS optimisation2 = (APPEND_INCREMENT_BUCKETS) optimisation;
            this.buckets = optimisation2.getBuckets();
        } else {
            this.buckets = 1;
        }
        this.conv = conv;
        this.failNotFound = failNotFound;
        this.random = random;
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
            if (buckets > 1) {
                requests.addOp(new ReadOp(key + ":" + random.nextInt(buckets)));
            } else {
                requests.addOp(new ReadOp(key));
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
        List<T> valueList = new ArrayList<T>();
        for (@SuppressWarnings("unused") String key : keys) {
            try {
                valueList.addAll(conv.convert(results.processReadAt(firstOp++)));
            } catch (NotFoundException e) {
                if (failNotFound) {
                    throw e;
                }
            }
        }
        if (!valueList.isEmpty()) {
            value = valueList.get(random.nextInt(valueList.size()));
        }
        return keys.size();
    }

    /**
     * A random element of the list that has been read (if {@link #failNotFound}
     * is not set and none of the given keys was found / the chosen buckets are
     * empty, <tt>null</tt> is returned).
     * 
     * @return the value from Scalaris or <tt>null</tt>
     */
    public T getValue() {
        return value;
    }
}
