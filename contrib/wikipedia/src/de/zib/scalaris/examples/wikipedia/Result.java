package de.zib.scalaris.examples.wikipedia;

/**
 * Common result class with a public member containing the result and a
 * message.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Result {
    /**
     * Whether an operation was a success or not.
     */
    public boolean success;
    /**
     * An additional message (mostly used with unsuccessful operations).
     */
    public String message;
    /**
     * Indicates whether the connection to the DB failed or not.
     */
    public boolean connect_failed;
    /**
     * Time in milliseconds for this operation.
     */
    public long time;
    /**
     * Creates a successful result with an empty message.
     * 
     * @param time  time in milliseconds for this operation
     */
    public Result(long time) {
        this.success = true;
        this.message = "";
        this.connect_failed = false;
        this.time = time;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success        the success status
     * @param message        the message to use
     * @param connectFailed  whether the connection to the DB failed or not
     * @param time           time in milliseconds for this operation
     */
    public Result(boolean success, String message, boolean connectFailed, long time) {
        this.success = success;
        this.message = message;
        this.connect_failed = connectFailed;
        this.time = time;
    }
}