package de.zib.scalaris.examples.wikipedia;

import java.util.List;

/**
 * Result of an operation getting a single value.
 * 
 * @author Nico Kruber, kruber@zib.de
 * 
 * @param <T>
 */
public class ValueResult<T> extends Result {
    /**
     * The retrieved value (may be null, e.g. if unsuccessful).
     */
    public T value = null;

    /**
     * Creates a new successful result with the given page list.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param number
     *            the retrieved number
     */
    public ValueResult(List<InvolvedKey> involvedKeys, T number) {
        super(involvedKeys);
        this.value = number;
    }

    /**
     * Creates a new successful result with the given value.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     * @param value
     *            the retrieved value
     * @param name
     *            the name of the operation (for the stats - see {@link #stats})
     * @param time
     *            time in milliseconds for this operation
     */
    public ValueResult(List<InvolvedKey> involvedKeys, T value, String name,
            long time) {
        super(involvedKeys);
        this.value = value;
        addStat(name, time);
    }

    /**
     * Creates a new custom result (value = <tt>null</tt>).
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
    public ValueResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed) {
        super(success, involvedKeys, message, connectFailed);
    }

    /**
     * Creates a new custom result (value = <tt>null</tt>).
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
    public ValueResult(boolean success, List<InvolvedKey> involvedKeys,
            String message, boolean connectFailed, String name, long time) {
        super(success, involvedKeys, message, connectFailed);
        addStat(name, time);
    }
}
