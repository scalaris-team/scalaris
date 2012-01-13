package de.zib.scalaris.examples.wikipedia;


/**
 * Result of an operation getting a random page title.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class RandomTitleResult extends Result {
    /**
     * The title of a random page on success.
     */
    public String title;

    /**
     * Creates a new successful result with the given page title.
     * 
     * @param title
     *            the retrieved (random) page title
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public RandomTitleResult(String title, String name, long time) {
        super(name, time);
        this.title = title;
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
    public RandomTitleResult(boolean success, String message,
            boolean connectFailed, String name, long time) {
        super(success, message, connectFailed, name, time);
        title = "";
    }
}
