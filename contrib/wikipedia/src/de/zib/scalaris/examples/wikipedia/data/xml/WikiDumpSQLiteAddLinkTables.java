/**
 *  Copyright 2011 Zuse Institute Berlin
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package de.zib.scalaris.examples.wikipedia.data.xml;

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Locale;

import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler.Connection;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MySQLiteWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Provides abilities to read a XML2SQLite wiki dump file and renders each page
 * to create the link tables, i.e. templatelinks, pagelinks, categorylinks,
 * redirect.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpSQLiteAddLinkTables implements WikiDump {
    private static final int PRINT_PAGES_EVERY = 500;
    /**
     * The time at the start of an import operation.
     */
    private long timeAtStart = 0;
    /**
     * The time at the end of an import operation.
     */
    private long timeAtEnd = 0;
    /**
     * The number of (successfully) processed pages.
     */
    protected int importedPages = 0;
    
    protected PrintStream msgOut = System.out;
    
    protected boolean stop = false;

    Connection connection = null;
    protected SQLiteStatement stGetPages = null;
    
    final String dbFileName;
    private SQLiteStatement stWriteCat = null;
    private SQLiteStatement stWriteTpl = null;
    private SQLiteStatement stWriteLnk = null;
    private SQLiteStatement stWriteRedirect = null;
    private SQLiteStatement stWriteStats = null;
    
    /**
     * Constructor.
     * 
     * @param dbFileName
     *            the name of the database file
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpSQLiteAddLinkTables(String dbFileName)
            throws RuntimeException {
        this.dbFileName = dbFileName;
    }

    /**
     * Sets the time the import started.
     */
    final protected void importStart() {
        timeAtStart = System.currentTimeMillis();
    }

    /**
     * Sets the time the import finished.
     */
    final protected void importEnd() {
        timeAtEnd = System.currentTimeMillis();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getTimeAtStart()
     */
    @Override
    public long getTimeAtStart() {
        return timeAtStart;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getTimeAtEnd()
     */
    @Override
    public long getTimeAtEnd() {
        return timeAtEnd;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getPageCount()
     */
    @Override
    public int getImportCount() {
        return importedPages;
    }

    /**
     * Reports the speed of the import (pages/s) and may be used as a shutdown
     * handler.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public class ReportAtShutDown extends Thread {
        public void run() {
            // import may have been interrupted - get an end time in this case
            if (timeAtEnd == 0) {
                importEnd();
            }
            final long timeTaken = timeAtEnd - timeAtStart;
            final double speed = (((double) importedPages) * 1000) / timeTaken;
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            println("Finished conversion (" + nf.format(speed) + " pages/s)");
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#setMsgOut(java.io.PrintStream)
     */
    @Override
    public void setMsgOut(PrintStream msgOut) {
        this.msgOut = msgOut;
    }

    /**
     * Whether {@link #stopParsing()} is supported or not.
     * 
     * @return no stop parsing support
     */
    @Override
    public boolean hasStopSupport() {
        return false;
    }

    /**
     * Tells the import to stop (not supported since this may not result in a
     * consistent view).
     */
    @Override
    public void stopParsing() {
//        this.stop = true;
    }
    
    protected void updateLinks3(Integer key, NormalisedTitle value, SQLiteStatement stWrite, String tableName) {
        try {
            try {
                stWrite.bind(1, key).bind(2, value.namespace)
                        .bind(3, value.title).stepThrough();
            } finally {
                stWrite.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("write of " + tableName + "." + key + " failed (sqlite error: " + e.toString() + ")");
            e.printStackTrace();
        }
    }
    
    protected void updateLinks2(Integer key, NormalisedTitle value, SQLiteStatement stWrite, String tableName) {
        try {
            try {
                stWrite.bind(1, key).bind(2, value.title).stepThrough();
            } finally {
                stWrite.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("write of " + tableName + "." + key + " failed (sqlite error: " + e.toString() + ")");
            e.printStackTrace();
        }
    }

    protected void updateSiteStats(long editCount, int articleCount) {
        try {
            try {
                stWriteStats.bind(1, editCount).bind(2, articleCount).stepThrough();
            } finally {
                stWriteStats.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("write of sitestats failed (sqlite error: " + e.toString() + ")");
            e.printStackTrace();
        }
    }
    
    /**
     * Parses all wiki pages in the DB and (re-)creates the link tables.
     * 
     * @throws RuntimeException
     */
    public void processLinks() throws RuntimeException {
        importStart();
        try {
            try {
                SiteInfo siteInfo = WikiDumpXml2SQLite.readSiteInfo(connection.db);
                MyNamespace namespace = new MyNamespace(siteInfo);
                MySQLiteWikiModel wikiModel = new MySQLiteWikiModel("", "", connection, namespace);
                long editCount = 0l;
                int articleCount = 0;

                while (stGetPages.step()) {
                    NormalisedTitle normTitle = new NormalisedTitle(
                            stGetPages.columnInt(0), stGetPages.columnString(1));
                    
                    RevisionResult getRevResult = SQLiteDataHandler
                            .getRevision(connection, normTitle, namespace);
                    
                    if (!getRevResult.success) {
                        System.err.println("read of current revision failed (error: " + getRevResult.message + ")");
                        throw new RuntimeException();
                    }
                    
                    final Page page = getRevResult.page;
                    final Revision revision = getRevResult.revision;

                    wikiModel.setUp();
                    wikiModel.setPageName(page.getTitle());
                    wikiModel.render(null, revision.unpackedText(), true);

                    String redirLink_raw = wikiModel.getRedirectLink();
                    if (redirLink_raw != null) {
                        NormalisedTitle redirLink = wikiModel.normalisePageTitle(redirLink_raw);
                        updateLinks3(page.getId(), redirLink, stWriteRedirect, "redirect");
                    }
                    
                    // TODO: what if the page was a redirect?
                    for (String cat_raw: wikiModel.getCategories().keySet()) {
                        NormalisedTitle category = new NormalisedTitle(
                                MyNamespace.CATEGORY_NAMESPACE_KEY,
                                MyWikiModel.normaliseName(cat_raw));
                        updateLinks2(page.getId(), category, stWriteCat, "categorylinks");
                    }
                    for (String tpl_raw: wikiModel.getTemplates()) {
                        NormalisedTitle template = new NormalisedTitle(
                                MyNamespace.TEMPLATE_NAMESPACE_KEY,
                                MyWikiModel.normaliseName(tpl_raw));
                        updateLinks3(page.getId(), template, stWriteTpl, "templatelinks");
                    }
                    for (String tpl_raw: wikiModel.getIncludes()) {
                        NormalisedTitle template = new NormalisedTitle(
                                MyNamespace.TEMPLATE_NAMESPACE_KEY,
                                MyWikiModel.normaliseName(tpl_raw));
                        updateLinks3(page.getId(), template, stWriteTpl, "templatelinks");
                    }
                    for (String link_raw: wikiModel.getLinks()) {
                        NormalisedTitle link = wikiModel.normalisePageTitle(link_raw);
                        updateLinks3(page.getId(), link, stWriteLnk, "pagelinks");
                    }
                    if (MyWikiModel.isArticle(normTitle.namespace, wikiModel
                            .getLinks(), wikiModel.getCategories().keySet())) {
                        ++articleCount;
                    }
                    
                    wikiModel.tearDown();
                    ++importedPages;
                    // only export page list every UPDATE_PAGELIST_EVERY pages:
                    if ((importedPages % PRINT_PAGES_EVERY) == 0) {
                        println("processed pages: " + importedPages);
                    }
                }
                updateSiteStats(editCount, articleCount);
            } finally {
                stGetPages.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("read failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        }
    }

    /**
     * Sets up the directory to write files to as well as the Scalaris
     * connection.
     * 
     * @throws RuntimeException
     *             if the directory could not be created
     */
    @Override
    public void setUp() {
        try {
            connection = new Connection(SQLiteDataHandler.openDB(dbFileName, false, null));
            stGetPages = connection.db.prepare("SELECT page_namespace, page_title FROM page");
        } catch (SQLiteException e) {
            System.err.println("Cannot read database: " + dbFileName);
            throw new RuntimeException(e);
        }
        println("Creating link tables...");
        prepareDB();
    }

    /* (non-Javadoc)
     * @see java.lang.Object#finalize()
     */
    @Override
    protected void finalize() throws Throwable {
        super.finalize();
        tearDown();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#tearDown()
     */
    @Override
    public void tearDown() {
        if (stGetPages != null) {
            stGetPages.dispose();
        }
        if (connection != null) {
            connection.dispose();
        }
        if (stWriteCat != null) {
            stWriteCat.dispose();
        }
        if (stWriteTpl != null) {
            stWriteTpl.dispose();
        }
        if (stWriteLnk != null) {
            stWriteLnk.dispose();
        }
        if (stWriteRedirect != null) {
            stWriteRedirect.dispose();
        }
        if (stWriteStats != null) {
            stWriteStats.dispose();
        }
        importEnd();
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * 
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    public void print(String message) {
        WikiDumpHandler.print(msgOut, message);
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * Includes a newline character at the end.
     * 
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    public void println(String message) {
        WikiDumpHandler.println(msgOut, message);
    }

    /**
     * Note: this needs to be executed from the thread that works on the DB!
     * 
     * @throws RuntimeException
     */
    public void prepareDB() throws RuntimeException {
        try {
            /**
             * Track page-to-page hyperlinks within the wiki.
             */
            final String createPageLinksTable = "CREATE TABLE IF NOT EXISTS pagelinks ("
                    + "pl_from int unsigned NOT NULL default 0,"
                    + "pl_to int unsigned NOT NULL default 0"
                    + ");";
            connection.db.exec(createPageLinksTable);
            // create index here to check at insertion:
            connection.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS pl_from ON pagelinks (pl_from,pl_to);");
            
            /**
             * Track template inclusions.
             */
            final String createTemplateLinksTable = "CREATE TABLE IF NOT EXISTS templatelinks ("
                    + "tl_from int unsigned NOT NULL default 0,"
                    + "tl_to int unsigned NOT NULL default 0"
                    + ");";
            connection.db.exec(createTemplateLinksTable);
            // create index here to check at insertion:
            connection.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS tl_from ON templatelinks (tl_from,tl_to);");

            /**
             * Track category inclusions *used inline*.
             * This tracks a single level of category membership.
             */
            final String createCategoryLinksTable = "CREATE TABLE IF NOT EXISTS categorylinks ("
                    + "cl_from int unsigned NOT NULL default 0,"
                    + "cl_to int unsigned NOT NULL default 0"
                    + ");";
            connection.db.exec(createCategoryLinksTable);
            // create index here to check at insertion:
            connection.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS cl_from ON categorylinks (cl_from,cl_to);");
            
            /**
             * For each redirect, this table contains exactly one row defining its target.
             */
            final String createRedirectsTable = "CREATE TABLE IF NOT EXISTS redirect ("
                    + "rd_from int unsigned NOT NULL default 0 PRIMARY KEY,"
                    + "rd_to int unsigned NOT NULL default 0"
                    + ");";
            connection.db.exec(createRedirectsTable);

            /**
             * Contains a single row with some aggregate info on the state of the site.
             */
            final String createSiteStatsTable = "CREATE TABLE IF NOT EXISTS site_stats ("
                    + "ss_row_id int unsigned NOT NULL,"
                    + "ss_total_views bigint unsigned default 0,"
                    + "ss_total_edits bigint unsigned default 0,"
                    + "ss_good_articles bigint unsigned default 0,"
                    + "ss_total_pages bigint default '-1'"
                    + ");";
            connection.db.exec(createSiteStatsTable);
            // create index here to check at insertion:
            connection.db.exec("CREATE UNIQUE INDEX IF NOT EXISTS ss_row_id ON site_stats (ss_row_id);");
            
            stWriteCat = connection.db.prepare("REPLACE INTO categorylinks "
                    + "(cl_from, cl_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == "
                    + MyNamespace.CATEGORY_NAMESPACE_KEY
                    + " AND page_title == ?;");
            stWriteTpl = connection.db.prepare("REPLACE INTO templatelinks "
                    + "(tl_from, tl_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stWriteLnk = connection.db.prepare("REPLACE INTO pagelinks "
                    + "(pl_from, pl_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stWriteRedirect = connection.db.prepare("REPLACE INTO redirect "
                    + "(rd_from, rd_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stWriteStats = connection.db.prepare("REPLACE INTO site_stats "
                    + "(ss_row_id, ss_total_views, ss_total_edits, ss_good_articles, ss_total_pages) "
                    + "SELECT 1, 0, ?, ?, COUNT(*) FROM page;");
            
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
        }
    }
}
