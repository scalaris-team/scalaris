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
     * Time in milliseconds for this operation (one entry for each call to the
     * DB).
     */
    public LinkedMultiHashMap<String, Long> stats = new LinkedMultiHashMap<String, Long>();
    
    /**
     * Creates a successful result with an empty message.
     * 
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public Result(String name, long time) {
        this.success = true;
        this.message = "";
        this.connect_failed = false;
        addStat(name, time);
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
    public Result(boolean success, String message, boolean connectFailed, String name, long time) {
        this.success = success;
        this.message = message;
        this.connect_failed = connectFailed;
        addStat(name, time);
    }

    /**
     * Adds the time needed to retrieve the given page to the collected
     * statistics.
     * 
     * @param title
     *            the title of the page
     * @param value
     *            the number of milliseconds it took to retrieve the page
     */
    public void addStat(String title, long value) {
        stats.put(title, value);
    }
}
