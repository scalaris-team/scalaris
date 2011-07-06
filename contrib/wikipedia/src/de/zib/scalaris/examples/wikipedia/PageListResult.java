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
     * @param pages the retrieved revision
     */
    public PageListResult(List<String> pages) {
        super();
        this.pages = pages;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success the success status
     * @param message the message to use
     */
    public PageListResult(boolean success, String message) {
        super(success, message);
        pages = new LinkedList<String>();
    }
}