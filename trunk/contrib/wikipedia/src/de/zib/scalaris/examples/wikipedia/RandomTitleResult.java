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
     * @param title  the retrieved (random) page title
     * @param time   time in milliseconds for this operation
     */
    public RandomTitleResult(String title, long time) {
        super(time);
        this.title = title;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success        the success status
     * @param message        the message to use
     * @param connectFailed  whether the connection to the DB failed or not
     * @param time           time in milliseconds for this operation
     */
    public RandomTitleResult(boolean success, String message, boolean connectFailed, long time) {
        super(success, message, connectFailed, time);
        title = "";
    }
}