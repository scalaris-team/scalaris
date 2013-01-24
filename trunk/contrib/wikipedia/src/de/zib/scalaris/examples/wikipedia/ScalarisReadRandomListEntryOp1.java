package de.zib.scalaris.examples.wikipedia;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.EmptyListException;
import de.zib.scalaris.ErlangValue;
import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.Options.IBuckets;
import de.zib.scalaris.examples.wikipedia.Options.IPartialRead;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.executor.ScalarisOp;
import de.zib.scalaris.operations.ReadOp;
import de.zib.scalaris.operations.ReadRandomFromListOp;

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
    final ErlangConverter<List<T>> listConv;
    final ErlangConverter<T> elemConv;
    final boolean failNotFound;
    T value = null;
    final Random random;
    final boolean usePartialReads;

    /**
     * Creates a new (random) list entry read operation.
     * 
     * @param keys
     *            the keys under which the list is stored in Scalaris
     * @param optimisation
     *            the list optimisation to use
     * @param elemConv
     *            converter to make an {@link ErlangValue} to a <tt>T</tt>
     * @param listConv
     *            converter to make an {@link ErlangValue} to a {@link List} of
     *            <tt>T</tt>
     * @param failNotFound
     *            whether to re-throw the {@link NotFoundException} if a list
     *            key was not found
     * @param random
     *            the random number generator to use
     */
    public ScalarisReadRandomListEntryOp1(final Collection<String> keys,
            final Optimisation optimisation, ErlangConverter<T> elemConv, ErlangConverter<List<T>> listConv,
            boolean failNotFound, Random random) {
        this.keys = keys;
        if (optimisation instanceof IBuckets) {
            this.buckets = ((IBuckets) optimisation).getBuckets();
        } else {
            this.buckets = 1;
        }
        this.usePartialReads = optimisation instanceof IPartialRead;
        this.listConv = listConv;
        this.elemConv = elemConv;
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
            if (usePartialReads) {
                requests.addOp(new ReadRandomFromListOp(key + ":" + random.nextInt(buckets)));
            } else {
                requests.addOp(new ReadOp(key + ":" + random.nextInt(buckets)));
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
        if (usePartialReads) {
            /*
             * each partition may contain a different number of keys
             * -> collect all partition's random values and calculate the final
             * random value based on the number of entries in the partition's
             * lists
             * -> in the created valueMap, each random value stands for a range
             * of keys equal to the number of list elements of its partition; we
             * finally draw a random number in the overall range and use the
             * read random value responsible for that
             */
            TreeMap<Integer, T> valueMap = new TreeMap<Integer, T>();
            Integer listLen = 0;
            for (@SuppressWarnings("unused") String key : keys) {
                try {
                    ReadRandomFromListOp.Result res = ((ReadRandomFromListOp) results.get(firstOp++)).processResult();
                    listLen += res.listLength;
                    valueMap.put(listLen, elemConv.convert(res.randomElement));
                } catch (NotFoundException e) {
                    if (failNotFound) {
                        throw e;
                    }
                } catch (EmptyListException e) {
                    // this is ok - we simply ignore this partition
                }
            }
            if (listLen != 0) {
                /*
                 * note: tailMap returns the tail including the given key.
                 * Since a value stored at 1 has a range of 1, we must increase
                 * the randomly drawn integer
                 */
                value = valueMap.tailMap(random.nextInt(listLen) + 1).values().iterator().next();
            }
        } else {
            List<T> valueList = new ArrayList<T>();
            for (@SuppressWarnings("unused") String key : keys) {
                try {
                    valueList.addAll(listConv.convert(results.processReadAt(firstOp++)));
                } catch (NotFoundException e) {
                    if (failNotFound) {
                        throw e;
                    }
                }
            }
            if (!valueList.isEmpty()) {
                value = valueList.get(random.nextInt(valueList.size()));
            }
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
