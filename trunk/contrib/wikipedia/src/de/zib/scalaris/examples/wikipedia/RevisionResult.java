package de.zib.scalaris.examples.wikipedia;

import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;

/**
 * Result of an operation getting a revision.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class RevisionResult extends Result {
    /**
     * Revision on success.
     */
    public Revision revision = null;
    /**
     * Page on success (if retrieved).
     */
    public Page page = null;
    /**
     * Whether the pages does not exist.
     */
    public boolean page_not_existing = false;
    /**
     * Whether the requested revision does not exist.
     */
    public boolean rev_not_existing = false;
    
    /**
     * Creates a new successful result with no {@link #revision} or
     * {@link #page}.
     * 
     * Either {@link #success} should be set to false or {@link #revision}
     * and {@link #page} should be set for a valid result object.
     */
    public RevisionResult() {
        super();
    }
    /**
     * Creates a new successful result with the given revision.
     * 
     * @param revision the retrieved revision
     */
    public RevisionResult(Revision revision) {
        super();
        this.revision = revision;
    }
    /**
     * Creates a new custom result.
     * 
     * @param success       the success status
     * @param message       the message to use
     * @param connectFailed whether the connection to the DB failed or not
     */
    public RevisionResult(boolean success, String message, boolean connectFailed) {
        super(success, message, connectFailed);
    }
}