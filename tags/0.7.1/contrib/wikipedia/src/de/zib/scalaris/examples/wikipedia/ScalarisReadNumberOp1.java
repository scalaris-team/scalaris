package de.zib.scalaris.examples.wikipedia;

import java.math.BigInteger;
import java.util.Collection;

import com.ericsson.otp.erlang.OtpErlangException;

import de.zib.scalaris.NotFoundException;
import de.zib.scalaris.RequestList;
import de.zib.scalaris.ResultList;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.Options.APPEND_INCREMENT_BUCKETS_WITH_WCACHE;
import de.zib.scalaris.examples.wikipedia.Options.IBuckets;
import de.zib.scalaris.examples.wikipedia.Options.Optimisation;
import de.zib.scalaris.executor.ScalarisOp;
import de.zib.scalaris.operations.ReadOp;

/**
 * Implements a number read operation.
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class ScalarisReadNumberOp1 implements ScalarisOp {
    final Collection<String> keys;
    final int buckets;
    final boolean failNotFound;
    BigInteger value = BigInteger.ZERO;
    final private Optimisation optimisation;

    /**
     * Creates a new number read operation.
     * 
     * @param keys
     *            the keys under which the number is stored in Scalaris
     * @param optimisation
     *            the list optimisation to use
     * @param failNotFound
     *            whether to re-throw the {@link NotFoundException} if no list
     *            key was found
     */
    public ScalarisReadNumberOp1(final Collection<String> keys,
            final Optimisation optimisation, boolean failNotFound) {
        this.keys = keys;
        if (optimisation instanceof APPEND_INCREMENT_BUCKETS_WITH_WCACHE) {
            APPEND_INCREMENT_BUCKETS_WITH_WCACHE opt2 = (APPEND_INCREMENT_BUCKETS_WITH_WCACHE) optimisation;
            // counters only exist in the read and write-add buckets
            this.buckets = opt2.getReadBuckets() + opt2.getWriteBuckets();
        } else if (optimisation instanceof IBuckets) {
            this.buckets = ((IBuckets) optimisation).getBuckets();
        } else {
            this.buckets = 1;
        }
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
     * Adds a read operation for the number to the request list.
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
     * Verifies the read operation(s) and creates the full number.
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
        for (int x = 0; x < keys.size(); ++x) {
            for (int i = 0; i < buckets; ++i) {
                try {
                    value = value.add(results.processReadAt(firstOp++).bigIntValue());
                } catch (NotFoundException e) {
                    ++notFound;
                    lastNotFound = e;
                }
            }
        }
        if (failNotFound && notFound == (keys.size() * buckets)) {
            throw lastNotFound;
        }
        return keys.size() * buckets;
    }

    /**
     * The (assembled) number that has been read (if {@link #failNotFound} is
     * not set and none of the given keys was found, {@link BigInteger#ZERO} is
     * returned).
     * 
     * @return the value from Scalaris or {@link BigInteger#ZERO}
     */
    public BigInteger getValue() {
        return value;
    }
}
