package de.zib.scalaris.examples.wikipedia;

import java.util.LinkedList;
import java.util.List;

/**
 * Result of an operation getting a page list.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class PageListResult extends Result {
    /**
     * The list of retrieved pages (empty if not successful).
     */
    public List<String> pages;

    /**
     * Creates a new successful result with the given page list.
     * 
     * @param pages
     *            the retrieved revision
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public PageListResult(List<String> pages, String name, long time) {
        super(name, time);
        this.pages = pages;
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
    public PageListResult(boolean success, String message,
            boolean connectFailed, String name, long time) {
        super(success, message, connectFailed, name, time);
        pages = new LinkedList<String>();
    }
}
