/**
 * 
 */
package de.zib.scalaris.examples.wikipedia;

import java.io.File;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Contributor;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;

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

    /**
     * Wrapper for several prepared statements of a single SQLite connection.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class Connection {
        /**
         * The DB connection.
         */
        public final SQLiteConnection db;
        
        /**
         * Allows retrieval of the latest revision of a page.
         */
        public SQLiteStatement stmtGetLatestRev;
        
        /**
         * Allows retrieval of the total number of pages.
         */
        public SQLiteStatement stmtGetPageCount;
        
        /**
         * Allows retrieval of the total number of articles.
         */
        public SQLiteStatement stmtGetArticleCount;
        
        /**
         * Allows retrieval of the total number of pages inside a category.
         */
        public SQLiteStatement stmtGetPagesInCatCount;
        
        /**
         * Allows retrieval of the list of pages inside a category.
         */
        public SQLiteStatement stmtGetPagesInCat;
        
        /**
         * Allows retrieval of the list of pages using a template.
         */
        public SQLiteStatement stmtGetPagesInTpl;
        
        /**
         * Allows retrieval of the list of pages linking to another page.
         */
        public SQLiteStatement stmtGetPagesLinkingTo;
        
        /**
         * Creates all prepared statements and stores them in the object's
         * members.
         * 
         * @param connection
         *            the SQLite connection to use
         * 
         * @throws SQLiteException
         *             if a prepared statement fails
         */
        public Connection(SQLiteConnection connection) throws SQLiteException {
            this.db = connection;
            initStmts();
        }
        
        /**
         * Creates all prepared statements and stores them in the object's
         * members.
         * 
         * @param fileName
         *            the name of the DB file
         * 
         * @throws SQLiteException
         *             if a prepared statement fails
         */
        public Connection(String fileName) throws SQLiteException {
            this.db = openDB(fileName, true);
            initStmts();
        }

        protected void initStmts() throws SQLiteException {
            stmtGetLatestRev = this.db.prepare("SELECT * FROM page "
                    + "INNER JOIN revision ON page_latest == rev_id "
                    + "INNER JOIN text ON rev_text_id == old_id "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stmtGetPageCount = this.db
                    .prepare("SELECT ss_total_pages FROM site_stats WHERE ss_row_id == 1;");
            stmtGetArticleCount = this.db
                    .prepare("SELECT ss_good_articles FROM site_stats WHERE ss_row_id == 1;");
            stmtGetPagesInCatCount = this.db
                    .prepare("SELECT COUNT(*) FROM categorylinks "
                            + "WHERE cl_to = (SELECT page_id from page where page_namespace = "
                            + MyNamespace.CATEGORY_NAMESPACE_KEY
                            + " AND page_title = ?);");
            stmtGetPagesInCat = this.db
                    .prepare("SELECT page_namespace, page_title FROM categorylinks "
                            + "INNER JOIN page on cl_from = page_id "
                            + "WHERE cl_to = (SELECT page_id from page where page_namespace = ? AND page_title = ?);");
            stmtGetPagesInTpl = this.db
                    .prepare("SELECT page_namespace, page_title FROM templatelinks "
                            + "INNER JOIN page on tl_from = page_id "
                            + "WHERE tl_to = (SELECT page_id from page where page_namespace = ? AND page_title = ?);");
            stmtGetPagesLinkingTo = this.db
                    .prepare("SELECT page_namespace, page_title FROM pagelinks "
                            + "INNER JOIN page on pl_from = page_id "
                            + "WHERE pl_to = (SELECT page_id from page where page_namespace = ? AND page_title = ?);");
        }
        
        /**
         * Disposes all statements as well as the DB connection to free
         * resources.
         */
        public void dispose() {
            stmtGetLatestRev.dispose();
            stmtGetPageCount.dispose();
            stmtGetArticleCount.dispose();
            stmtGetPagesInCatCount.dispose();
            stmtGetPagesInCat.dispose();
            db.dispose();
        }
    }
    
    /**
     * Retrieves the current, i.e. most up-to-date, version of a page from
     * the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * @param title
     *            the title of the page
     * @param nsObject
     *            the namespace for page title de-normalisation
     * 
     * @return a result object with the page and revision on success
     */
    public static RevisionResult getRevision(Connection connection,
            NormalisedTitle title, MyNamespace nsObject) {
        final long timeAtStart = System.currentTimeMillis();
        Page page = null;
        Revision revision = null;
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        final String statName = "PAGE:" + title.toString();
        if (connection == null) {
            return new RevisionResult(false, involvedKeys,
                    "no connection to SQLite DB", true, title, page, revision, false,
                    false, statName, System.currentTimeMillis() - timeAtStart);
        }
        
        final SQLiteStatement stmt = connection.stmtGetLatestRev;
        try {
            stmt.bind(1, title.namespace).bind(2, title.title);
            if (stmt.step()) {
                page = new Page();
                page.setTitle(title.denormalise(nsObject));
                revision = new Revision();
                for (int i = 0; i < stmt.columnCount(); i++) {
                    String columnName = stmt.getColumnName(i);
                    if (columnName.equals("page_id")) {
                        page.setId(stmt.columnInt(i));
                    } else if (columnName.equals("page_restrictions")) {
                        page.setRestrictions(Page.restrictionsFromString(stmt.columnString(i), ","));
                    } else if (columnName.equals("page_is_redirect")) {
                        page.setRedirect(stmt.columnInt(i) != 0);
                    } else if (columnName.equals("rev_id")) {
                        revision.setId(stmt.columnInt(i));
                    } else if (columnName.equals("rev_comment")) {
                        revision.setComment(stmt.columnString(i));
                    } else if (columnName.equals("rev_user_text")) {
                        Contributor contributor = new Contributor();
                        contributor.setIp(stmt.columnString(i));
                        revision.setContributor(contributor);
                    } else if (columnName.equals("rev_timestamp")) {
                        revision.setTimestamp(stmt.columnString(i));
                    } else if (columnName.equals("rev_minor_edit")) {
                        revision.setMinor(stmt.columnInt(i) != 0);
                    } else if (columnName.equals("old_text")) {
                        revision.setPackedText(stmt.columnBlob(i));
                    }
                }
                page.setCurRev(revision);
            } else {
                return new RevisionResult(false, involvedKeys,
                        "page \"" + statName + "\" not found", false,
                        title, page, revision, true, false, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            // there should only be one data item
            if (stmt.step()) {
                return new RevisionResult(false, involvedKeys,
                        "more than one result", false, title, page, revision, false,
                        false, statName, System.currentTimeMillis() - timeAtStart);
            }

            return new RevisionResult(involvedKeys, title, page, revision, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (SQLiteException e) {
            return new RevisionResult(false, involvedKeys, "SQLite exception: "
                    + e.getMessage(), false, title, page, revision, false,
                    false, statName, System.currentTimeMillis()
                            - timeAtStart);
        } finally {
            try {
                stmt.reset();
            } catch (SQLiteException e) {
            }
        }
    }

    /**
     * Retrieves the number of all available pages from the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPageCount(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "PAGE_COUNT";
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "no connection to SQLite DB", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return getInteger(connection.stmtGetPageCount, null, timeAtStart,
                statName, involvedKeys);
    }

    /**
     * Retrieves the number of available articles, i.e. pages in the main
     * namespace, from the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * 
     * @return a result object with the number of articles on success
     */
    public static ValueResult<BigInteger> getArticleCount(Connection connection) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "ARTICLE_COUNT";
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "no connection to SQLite DB", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return getInteger(connection.stmtGetArticleCount, null, timeAtStart,
                statName, involvedKeys);
    }

    /**
     * Retrieves the number of all pages in the given category from the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * @param title
     *            the title of the category
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getPagesInCategoryCount(
            Connection connection, NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "CAT_CNT:" + title;
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    "no connection to SQLite DB", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return getInteger(connection.stmtGetPagesInCatCount, title.title,
                timeAtStart, statName, involvedKeys);
    }

    /**
     * Retrieves a number from the SQLite DB using a prepared statement with the
     * given bindings and a single result column.
     * 
     * @param stmt
     *            the statement to evaluate
     * @param bind1
     *            string to bind to parameter 1 (if not <tt>null</tt>)
     * @param timeAtStart
     *            the start time of the method using this method
     * @param statName
     *            name for the time measurement statistics
     * @param involvedKeys
     *            list of all involved keys
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<BigInteger> getInteger(
            final SQLiteStatement stmt, String bind1, final long timeAtStart,
            final String statName, List<InvolvedKey> involvedKeys) {
        try {
            if (bind1 != null) {
                stmt.bind(1, bind1);
            }
            if (stmt.step()) {
                BigInteger number = BigInteger.valueOf(stmt.columnLong(0));
                return new ValueResult<BigInteger>(involvedKeys, number, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            return new ValueResult<BigInteger>(
                    false,
                    involvedKeys,
                    "no result reading " + statName,
                    false, statName, System.currentTimeMillis() - timeAtStart);
        } catch (SQLiteException e) {
            return new ValueResult<BigInteger>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading " + statName + ": "
                            + e.getMessage(), false, statName,
                    System.currentTimeMillis() - timeAtStart);
        } finally {
            try {
                stmt.reset();
            } catch (SQLiteException e) {
            }
        }
    }

    /**
     * Retrieves a list of pages in the given category from the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * @param title
     *            the title of the category
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesInCategory(
            Connection connection, NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "CAT_LIST:" + title;
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<List<NormalisedTitle>>(false, involvedKeys,
                    "no connection to SQLite DB", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return getPageList(connection.stmtGetPagesInCat, null, title.title,
                timeAtStart, statName, involvedKeys);
    }

    /**
     * Retrieves a list of pages using the given template from the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * @param title
     *            the title of the template
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesInTemplate(
            Connection connection, NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "TPL_LIST:" + title;
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (connection == null) {
            return new ValueResult<List<NormalisedTitle>>(false, involvedKeys,
                    "no connection to SQLite DB", true, statName,
                    System.currentTimeMillis() - timeAtStart);
        }
        return getPageList(connection.stmtGetPagesInTpl, title.namespace,
                title.title, timeAtStart, statName, involvedKeys);
    }

    /**
     * Retrieves a list of pages linking to the given page from the SQLite DB.
     * 
     * @param connection
     *            the connection to the SQLite DB
     * @param title
     *            the title of the page
     * 
     * @return a result object with the page list on success
     */
    public static ValueResult<List<NormalisedTitle>> getPagesLinkingTo(
            Connection connection, NormalisedTitle title) {
        final long timeAtStart = System.currentTimeMillis();
        final String statName = "LINKS:" + title;
        final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
        if (Options.getInstance().WIKI_USE_BACKLINKS) {
            if (connection == null) {
                return new ValueResult<List<NormalisedTitle>>(false, involvedKeys,
                        "no connection to SQLite DB", true, statName,
                        System.currentTimeMillis() - timeAtStart);
            }
            return getPageList(connection.stmtGetPagesLinkingTo,
                    title.namespace, title.title, timeAtStart, statName,
                    involvedKeys);
        } else {
            return new ValueResult<List<NormalisedTitle>>(involvedKeys,
                    new ArrayList<NormalisedTitle>(0));
        }
    }

    /**
     * Retrieves a list of pages from the SQLite DB using a prepared statement
     * with the given bindings and two result columns, the namespace and the
     * title.
     * 
     * @param stmt
     *            the statement to evaluate
     * @param namespace
     *            bound to parameter 1 if not <tt>null</tt>
     * @param title
     *            bound to parameter 2 if <tt>namespace</tt> is not <tt>null</tt>,
     *            otherwise to parameter 1
     * @param timeAtStart
     *            the start time of the method using this method
     * @param statName
     *            name for the time measurement statistics
     * @param involvedKeys
     *            list of all involved keys
     * 
     * @return a result object with the number of pages on success
     */
    public static ValueResult<List<NormalisedTitle>> getPageList(
            final SQLiteStatement stmt, Integer namespace, String title, final long timeAtStart,
            final String statName, List<InvolvedKey> involvedKeys) {
        // namespace != null -> title != null:
        assert (namespace == null || title != null);
        try {
            ArrayList<NormalisedTitle> result = new ArrayList<NormalisedTitle>();
            if (namespace != null) {
                stmt.bind(1, namespace);
                stmt.bind(2, title);
            } else {
                stmt.bind(1, title);
            }
            while (stmt.step()) {
                result.add(new NormalisedTitle(stmt.columnInt(0), stmt.columnString(1)));
            }
            return new ValueResult<List<NormalisedTitle>>(involvedKeys, result, statName,
                    System.currentTimeMillis() - timeAtStart);
        } catch (SQLiteException e) {
            return new ValueResult<List<NormalisedTitle>>(false, involvedKeys,
                    e.getClass().getCanonicalName() + " reading " + statName + ": "
                            + e.getMessage(), false, statName,
                    System.currentTimeMillis() - timeAtStart);
        } finally {
            try {
                stmt.reset();
            } catch (SQLiteException e) {
            }
        }
    }

}
