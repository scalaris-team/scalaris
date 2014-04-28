package de.zib.scalaris.examples.wikipedia;

import java.util.List;

import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;

/**
 * Result of an operation getting a revision.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class RevisionResult extends Result {
    /**
     * Normalised page title.
     */
    final public NormalisedTitle normalisedTitle;
    /**
     * Revision on success.
     */
    public Revision revision = null;
    /**
     * Page on success (if retrieved).
     */
    public Page page = null;
    /**
     * whether the page exists or not (if <tt>true</tt>, {@link Result#success}
     * must also be <tt>true</tt>).
     */
    public boolean page_not_existing = false;
    /**
     * Whether the requested revision exists or not (if <tt>true</tt>,
     * {@link Result#success} must also be <tt>true</tt>).
     */
    public boolean rev_not_existing = false;
    
    /**
     * Creates a new successful result with the given revision.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param normalisedTitle
     *            normalised page title
     * @param page
     *            the retrieved page
     * @param revision
     *            the retrieved revision
     */
    public RevisionResult(List<InvolvedKey> involvedKeys, NormalisedTitle normalisedTitle, Page page,
            Revision revision) {
        super(involvedKeys);
        this.normalisedTitle = normalisedTitle;
        this.page = page;
        this.revision = revision;
    }
    
    /**
     * Creates a new successful result with the given revision.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param normalisedTitle
     *            normalised page title
     * @param page
     *            the retrieved page
     * @param revision
     *            the retrieved revision
     * @param statName
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public RevisionResult(List<InvolvedKey> involvedKeys, NormalisedTitle normalisedTitle, Page page,
            Revision revision, String statName, long time) {
        super(involvedKeys);
        this.normalisedTitle = normalisedTitle;
        this.page = page;
        this.revision = revision;
        addStat(statName, time);
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
     * @param normalisedTitle
     *            normalised page title
     * @param page
     *            page on success (if retrieved)
     * @param revision
     *            revision on success
     * @param page_not_existing
     *            whether the page exists or not (if <tt>true</tt>,
     *            {@link Result#success} must also be <tt>true</tt>)
     * @param rev_not_existing
     *            whether the requested revision exists or not (if <tt>true</tt>,
     *            {@link Result#success} must also be <tt>true</tt>)
     */
    public RevisionResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed, NormalisedTitle normalisedTitle, Page page,
            Revision revision, boolean page_not_existing,
            boolean rev_not_existing) {
        super(success, involvedKeys, message, connectFailed);
        this.normalisedTitle = normalisedTitle;
        this.page = page;
        this.revision = revision;
        this.page_not_existing = page_not_existing;
        this.rev_not_existing = rev_not_existing;
        assert(!rev_not_existing || !success);
        assert(!page_not_existing || !success);
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
     * @param normalisedTitle
     *            normalised page title
     * @param page
     *            page on success (if retrieved)
     * @param revision
     *            revision on success
     * @param page_not_existing
     *            whether the page exists or not (if <tt>true</tt>,
     *            {@link Result#success} must also be <tt>true</tt>)
     * @param rev_not_existing
     *            whether the requested revision exists or not (if <tt>true</tt>,
     *            {@link Result#success} must also be <tt>true</tt>)
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public RevisionResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed, NormalisedTitle normalisedTitle, Page page,
            Revision revision, boolean page_not_existing,
            boolean rev_not_existing, String name, long time) {
        super(success, involvedKeys, message, connectFailed);
        this.normalisedTitle = normalisedTitle;
        this.page = page;
        this.revision = revision;
        this.page_not_existing = page_not_existing;
        this.rev_not_existing = rev_not_existing;
        assert(!rev_not_existing || !success);
        assert(!page_not_existing || !success);
        addStat(name, time);
    }
}
