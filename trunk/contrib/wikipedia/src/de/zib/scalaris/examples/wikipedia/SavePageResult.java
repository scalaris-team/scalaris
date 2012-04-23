package de.zib.scalaris.examples.wikipedia;

import java.math.BigInteger;
import java.util.ArrayList;
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
     * In cases of failed page-save commits, contains a list of failed keys.
     */
    public List<String> failedKeys = new ArrayList<String>();
    
    /**
     * Creates a new successful result.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param oldPage
     *            old version of the page (may be null)
     * @param newPage
     *            new version of the page (may be null)
     * @param newShortRevs
     *            new list of (short) revisions (may be null)
     * @param pageEdits
     *            new number of page edits (may be null)
     */
    public SavePageResult(List<InvolvedKey> involvedKeys, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits) {
        super(involvedKeys);
        this.oldPage = oldPage;
        this.newPage = newPage;
        this.newShortRevs = newShortRevs;
        this.pageEdits = pageEdits;
    }
    
    /**
     * Creates a new successful result.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param oldPage
     *            old version of the page (may be null)
     * @param newPage
     *            new version of the page (may be null)
     * @param newShortRevs
     *            new list of (short) revisions (may be null)
     * @param pageEdits
     *            new number of page edits (may be null)
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public SavePageResult(List<InvolvedKey> involvedKeys, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits,
            String name, long time) {
        super(involvedKeys);
        this.oldPage = oldPage;
        this.newPage = newPage;
        this.newShortRevs = newShortRevs;
        this.pageEdits = pageEdits;
        addStat(name, time);
    }
    
    /**
     * Creates a new custom result.
     * 
     * @param success
     *            the success status
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param message
     *            the message to use
     * @param connectFailed
     *            whether the connection to the DB failed or not
     * @param oldPage
     *            old version of the page (may be null)
     * @param newPage
     *            new version of the page (may be null)
     * @param newShortRevs
     *            new list of (short) revisions (may be null)
     * @param pageEdits
     *            new number of page edits (may be null)
     */
    public SavePageResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits) {
        super(success, involvedKeys, message, connectFailed);
        this.oldPage = oldPage;
        this.newPage = newPage;
        this.newShortRevs = newShortRevs;
        this.pageEdits = pageEdits;
    }
    
    /**
     * Creates a new custom result.
     * 
     * @param success
     *            the success status
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param message
     *            the message to use
     * @param connectFailed
     *            whether the connection to the DB failed or not
     * @param oldPage
     *            old version of the page (may be null)
     * @param newPage
     *            new version of the page (may be null)
     * @param newShortRevs
     *            new list of (short) revisions (may be null)
     * @param pageEdits
     *            new number of page edits (may be null)
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public SavePageResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed, Page oldPage, Page newPage,
            List<ShortRevision> newShortRevs, BigInteger pageEdits,
            String name, long time) {
        super(success, involvedKeys, message, connectFailed);
        this.oldPage = oldPage;
        this.newPage = newPage;
        this.newShortRevs = newShortRevs;
        this.pageEdits = pageEdits;
        addStat(name, time);
    }
}
