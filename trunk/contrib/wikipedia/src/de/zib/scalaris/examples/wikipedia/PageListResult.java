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
     * @param pages  the retrieved revision
     * @param time   time in milliseconds for this operation
     */
    public PageListResult(List<String> pages, long time) {
        super(time);
        this.pages = pages;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success        the success status
     * @param message        the message to use
     * @param connectFailed  whether the connection to the DB failed or not
     * @param time           time in milliseconds for this operation
     */
    public PageListResult(boolean success, String message, boolean connectFailed, long time) {
        super(success, message, connectFailed, time);
        pages = new LinkedList<String>();
    }
}