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
     * @param title the retrieved (random) page title
     */
    public RandomTitleResult(String title) {
        super();
        this.title = title;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success the success status
     * @param message the message to use
     */
    public RandomTitleResult(boolean success, String message) {
        super(success, message);
        title = "";
    }
}