package de.zib.scalaris.examples.wikipedia.data.xml;

import java.io.PrintStream;

/**
 * Interface for common WikiDump methods
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public interface WikiDump {

    /**
     * Gets the time the import started.
     * 
     * @return the time the import started (in milliseconds)
     */
    public abstract long getTimeAtStart();

    /**
     * Gets the time the import finished.
     * 
     * @return the time the import finished (in milliseconds)
     */
    public abstract long getTimeAtEnd();

    /**
     * Gets the number of imported pages.
     * 
     * @return the number of pages imported into Scalaris
     */
    public abstract int getImportCount();

    /**
     * Sets the output writer to write status messages to (defaults to
     * System.out).
     * 
     * @param msgOut
     *            the msgOut to set
     */
    public abstract void setMsgOut(PrintStream msgOut);

    /**
     * Tells the import to stop (may not be supported by an implementation!).
     */
    public abstract void stopParsing();

    /**
     * Method to be called before using the handler.
     */
    public abstract void setUp();

    /**
     * Method to be called after using the handler (to clean up).
     */
    public abstract void tearDown();

}