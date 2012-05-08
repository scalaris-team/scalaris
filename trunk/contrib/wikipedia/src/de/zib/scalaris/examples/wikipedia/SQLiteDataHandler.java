/**
 * 
 */
package de.zib.scalaris.examples.wikipedia;

import java.io.File;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

/**
 * Retrieves and writes values from/to a SQLite DB.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class SQLiteDataHandler {

    /**
     * Opens a connection to a database and sets some default PRAGMAs for better
     * performance in our case.
     * 
     * @param fileName
     *            the name of the DB file
     * @param readOnly
     *            whether to open the DB read-only or not
     * @param cacheSize
     *            cache size to set for the DB connection (default if
     *            <tt>null</tt>)
     * 
     * @return the DB connection
     * 
     * @throws SQLiteException
     *             if the connection fails or a pragma could not be set
     */
    public static SQLiteConnection openDB(String fileName, boolean readOnly, Long cacheSize) throws SQLiteException {
        SQLiteConnection db = new SQLiteConnection(new File(fileName));
        if (readOnly) {
            db.openReadonly();
        } else {
            db.open(true);
        }
        // set cache_size:
        if (cacheSize != null) {
            final SQLiteStatement stmt = db.prepare("PRAGMA page_size;");
            if (stmt.step()) {
                long pageSize = stmt.columnLong(0);
                db.exec("PRAGMA cache_size = " + (cacheSize / pageSize) + ";");
            }
            stmt.dispose();
        }
        db.exec("PRAGMA synchronous = OFF;");
        db.exec("PRAGMA journal_mode = OFF;");
        //        db.exec("PRAGMA locking_mode = EXCLUSIVE;");
        db.exec("PRAGMA case_sensitive_like = true;"); 
        db.exec("PRAGMA encoding = 'UTF-8';"); 
        db.exec("PRAGMA temp_store = MEMORY;"); 
        return db;
    }

    /**
     * Opens a connection to a database and sets some default PRAGMAs for better
     * performance in our case.
     * 
     * @param fileName
     *            the name of the DB file
     * @param readOnly
     *            whether to open the DB read-only or not
     * 
     * @return the DB connection
     * 
     * @throws SQLiteException
     *             if the connection fails or a pragma could not be set
     */
    public static SQLiteConnection openDB(String fileName, boolean readOnly) throws SQLiteException {
        return openDB(fileName, readOnly, null);
    }

}
