package de.zib.scalaris.examples.wikipedia;

import java.util.List;

import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;

/**
 * Result of an operation getting the page history.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class PageHistoryResult extends Result {
    /**
     * The retrieved page on success (or <tt>null</tt>).
     */
    public Page page = null;
    /**
     * The retrieved (short) revisions on success (or <tt>null</tt>).
     */
    public List<ShortRevision> revisions = null;
    /**
     * Whether the page exists or not.
     */
    public boolean not_existing = false;

    /**
     * Creates a successful result with an empty message and the given
     * revisions.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param page
     *            the retrieved page
     * @param revisions
     *            the retrieved (short) revisions
     */
    public PageHistoryResult(List<InvolvedKey> involvedKeys, Page page, List<ShortRevision> revisions) {
        super(involvedKeys);
        this.page = page;
        this.revisions = revisions;
    }

    /**
     * Creates a successful result with an empty message and the given
     * revisions.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param page
     *            the retrieved page
     * @param revisions
     *            the retrieved (short) revisions
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public PageHistoryResult(List<InvolvedKey> involvedKeys, Page page, List<ShortRevision> revisions,
            String name, long time) {
        super(involvedKeys);
        this.page = page;
        this.revisions = revisions;
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
     */
    public PageHistoryResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed) {
        super(success, involvedKeys, message, connectFailed);
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
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public PageHistoryResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed, String name, long time) {
        super(success, involvedKeys, message, connectFailed);
        addStat(name, time);
    }
}
