package de.zib.scalaris.examples.wikipedia;

import java.math.BigInteger;
import java.util.List;

import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;

/**
 * Result of an operation saving a page, i.e. adding a new revision.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class SavePageResult extends Result {
    /**
     * Old version of the page (may be null).
     */
    public Page oldPage = null;
    /**
     * New version of the page (may be null).
     */
    public Page newPage = null;
    /**
     * New list of (short) revisions (may be null).
     */
    public List<ShortRevision> newShortRevs = null;
    /**
     * New number of page edists (may be null).
     */
    public BigInteger pageEdits = null;
    
    /**
     * Creates a new successful result.
     * 
     * @param oldPage       old version of the page (may be null)
     * @param newPage       new version of the page (may be null)
     * @param newShortRevs  new list of (short) revisions (may be null)
     * @param pageEdits     new number of page edists (may be null)
     * @param time          time in milliseconds for this operation
     */
    public SavePageResult(Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits, long time) {
        super(time);
        this.oldPage = oldPage;
        this.newPage = newPage;
        this.newShortRevs = newShortRevs;
        this.pageEdits = pageEdits;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success        the success status
     * @param message        the message to use
     * @param connectFailed  whether the connection to the DB failed or not
     * @param oldPage        old version of the page (may be null)
     * @param newPage        new version of the page (may be null)
     * @param newShortRevs   new list of (short) revisions (may be null)
     * @param pageEdits      new number of page edists (may be null)
     * @param time           time in milliseconds for this operation
     */
    public SavePageResult(boolean success, String message,
            boolean connectFailed, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits, long time) {
        super(success, message, connectFailed, time);
        this.oldPage = oldPage;
        this.newPage = newPage;
        this.newShortRevs = newShortRevs;
        this.pageEdits = pageEdits;
    }
}