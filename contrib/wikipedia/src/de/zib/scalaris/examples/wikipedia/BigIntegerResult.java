package de.zib.scalaris.examples.wikipedia;

import java.math.BigInteger;

/**
 * Result of an operation getting an integral number.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class BigIntegerResult extends Result {
    /**
     * The number (<tt>0</tt> if not successful).
     */
    public BigInteger number = BigInteger.valueOf(0);

    /**
     * Creates a new successful result with the given page list.
     * 
     * @param number
     *            the retrieved number
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public BigIntegerResult(BigInteger number, String name, long time) {
        super(name, time);
        this.number = number;
    }

    /**
     * Creates a new custom result.
     * 
     * @param success
     *            the success status
     * @param message
     *            the message to use
     * @param connectFailed
     *            whether the connection to the DB failed or not
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public BigIntegerResult(boolean success, String message,
            boolean connectFailed, String name, long time) {
        super(success, message, connectFailed, name, time);
    }
}
