package de.zib.scalaris.examples.wikipedia;

import java.util.ArrayList;
import java.util.List;

import de.zib.tools.LinkedMultiHashMap;


/**
 * Common result class with a public member containing the result and a
 * message.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Result {
    /**
     * Whether an operation was a success or not.
     */
    public boolean success;
    /**
     * An additional message (mostly used with unsuccessful operations).
     */
    public String message;
    /**
     * Indicates whether the connection to the DB failed or not.
     */
    public boolean connect_failed;
    /**
     * Time in milliseconds for this operation (one entry for each call to the
     * DB).
     */
    public LinkedMultiHashMap<String, Long> stats = new LinkedMultiHashMap<String, Long>();
    /**
     * All keys that have been read or written during the operation.
     */
    public List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
    
    /**
     * Creates a successful result with an empty message.
     * 
     * @param involvedKeys
     *            all keys that have been read or written during the operation
     */
    public Result(List<InvolvedKey> involvedKeys) {
        this.success = true;
        this.involvedKeys = involvedKeys;
        this.message = "";
        this.connect_failed = false;
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
    public Result(boolean success, List<InvolvedKey> involvedKeys, String message, boolean connectFailed) {
        this.success = success;
        this.involvedKeys = involvedKeys;
        this.message = message;
        this.connect_failed = connectFailed;
        assert(!connect_failed || !success);
    }

    /**
     * Adds the time needed to retrieve the given page to the collected
     * statistics.
     * 
     * @param name
     *            the name of the operation
     * @param time
     *            time in milliseconds for this operation
     */
    public void addStat(String name, long time) {
        stats.put1(name, time);
    }
}
