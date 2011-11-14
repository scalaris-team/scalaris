package de.zib.scalaris.examples.wikipedia;

/**
 * Result of an operation saving a page, i.e. adding a new revision.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class SaveResult extends Result {
    /**
     * Custom object carrying any information that may be needed for
     * further processing (may be null).
     */
    public Object info;
    /**
     * Creates a new successful result.
     */
    public SaveResult() {
        super();
    }
    /**
     * Creates a new custom result.
     * 
     * @param success       the success status
     * @param message       the message to use
     * @param connectFailed whether the connection to the DB failed or not
     */
    public SaveResult(boolean success, String message, boolean connectFailed) {
        super(success, message, connectFailed);
    }
}