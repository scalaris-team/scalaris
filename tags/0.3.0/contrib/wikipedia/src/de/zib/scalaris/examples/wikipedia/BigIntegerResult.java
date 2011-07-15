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
     * @param number the retrieved number
     */
    public BigIntegerResult(BigInteger number) {
        super();
        this.number = number;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success the success status
     * @param message the message to use
     */
    public BigIntegerResult(boolean success, String message) {
        super(success, message);
    }
}