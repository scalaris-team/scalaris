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
     * Creates a successful result with an empty message.
     */
    public Result() {
        this.success = true;
        this.message = "";
    }
    /**
     * Creates a new custom result.
     * 
     * @param success the success status
     * @param message the message to use
     */
    public Result(boolean success, String message) {
        this.success = success;
        this.message = message;
    }
}