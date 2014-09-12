/**
 *  Copyright 2011-2013 Zuse Institute Berlin
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler.Connection;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerNormalised;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MySQLiteWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
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
public class WikiDumpSQLiteLinkTables implements WikiDump {
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
    protected boolean errorDuringImport = false;
    
    /**
     * Constructor.
     * 
     * @param dbFileName
     *            the name of the database file
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpSQLiteLinkTables(String dbFileName)
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

    @Override
    public boolean isErrorDuringImport() {
        return errorDuringImport ;
    }

    @Override
    public void error(String message) {
        System.err.println(message);
        errorDuringImport = true;
    }

    /**
     * Reports the speed of the import (pages/s) and may be used as a shutdown
     * handler.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public class ReportAtShutDown extends Thread {
        public void run() {
            reportAtEnd();
        }

        /**
         * Sets the import end time and reports the overall speed.
         */
        public void reportAtEnd() {
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
            error("write of " + tableName + "." + key + " failed (sqlite error: " + e.toString() + ")");
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
            error("write of " + tableName + "." + key + " failed (sqlite error: " + e.toString() + ")");
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
            error("write of sitestats failed (sqlite error: " + e.toString() + ")");
            e.printStackTrace();
        }
    }
    
    /**
     * Parses all wiki pages in the DB and (re-)creates the link tables.
     * 
     * @throws RuntimeException
     */
    public void processLinks() throws RuntimeException {
        println("Populating link tables...");
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
                        error("read of current revision failed (error: " + getRevResult.message + ")");
                        throw new RuntimeException();
                    }
                    
                    final Page page = getRevResult.page;
                    final Revision revision = getRevResult.revision;

                    wikiModel.setUp();
                    wikiModel.setNamespaceName(wikiModel.getNamespace().getNamespaceByNumber(normTitle.namespace));
                    wikiModel.setPageName(normTitle.title);
                    wikiModel.renderPageWithCache(null, revision.unpackedText());

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
                    for (String tpl_raw: wikiModel.getTemplatesNoMagicWords()) {
                        NormalisedTitle template = new NormalisedTitle(
                                MyNamespace.TEMPLATE_NAMESPACE_KEY,
                                MyWikiModel.normaliseName(tpl_raw));
                        updateLinks3(page.getId(), template, stWriteTpl, "templatelinks");
                    }
                    for (String incl_raw: wikiModel.getIncludes()) {
                        NormalisedTitle include = wikiModel.normalisePageTitle(incl_raw);
                        updateLinks3(page.getId(), include, stWriteTpl, "templatelinks");
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
            error("read failed (sqlite error: " + e.toString() + ")");
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
        // note: this needs to be executed from the thread that works on the DB!
        try {
            println("Creating link tables...");
            final SQLiteConnection db = SQLiteDataHandler.openDB(dbFileName, false, null);
            
            /**
             * Track page-to-page hyperlinks within the wiki.
             */
            final String createPageLinksTable = "CREATE TABLE IF NOT EXISTS pagelinks ("
                    + "pl_from int unsigned NOT NULL default 0,"
                    + "pl_to int unsigned NOT NULL default 0"
                    + ");";
            db.exec(createPageLinksTable);
            // create index here to check at insertion:
            db.exec("CREATE UNIQUE INDEX IF NOT EXISTS pl_from ON pagelinks (pl_from,pl_to);");
            
            /**
             * Track template inclusions.
             */
            final String createTemplateLinksTable = "CREATE TABLE IF NOT EXISTS templatelinks ("
                    + "tl_from int unsigned NOT NULL default 0,"
                    + "tl_to int unsigned NOT NULL default 0"
                    + ");";
            db.exec(createTemplateLinksTable);
            // create index here to check at insertion:
            db.exec("CREATE UNIQUE INDEX IF NOT EXISTS tl_from ON templatelinks (tl_from,tl_to);");
        
            /**
             * Track category inclusions *used inline*.
             * This tracks a single level of category membership.
             */
            final String createCategoryLinksTable = "CREATE TABLE IF NOT EXISTS categorylinks ("
                    + "cl_from int unsigned NOT NULL default 0,"
                    + "cl_to int unsigned NOT NULL default 0"
                    + ");";
            db.exec(createCategoryLinksTable);
            // create index here to check at insertion:
            db.exec("CREATE UNIQUE INDEX IF NOT EXISTS cl_from ON categorylinks (cl_from,cl_to);");
            
            /**
             * For each redirect, this table contains exactly one row defining its target.
             */
            final String createRedirectsTable = "CREATE TABLE IF NOT EXISTS redirect ("
                    + "rd_from int unsigned NOT NULL default 0 PRIMARY KEY,"
                    + "rd_to int unsigned NOT NULL default 0"
                    + ");";
            db.exec(createRedirectsTable);
        
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
            db.exec(createSiteStatsTable);
            // create index here to check at insertion:
            db.exec("CREATE UNIQUE INDEX IF NOT EXISTS ss_row_id ON site_stats (ss_row_id);");
        
            stGetPages = db.prepare("SELECT page_namespace, page_title FROM page");
            stWriteCat = db.prepare("REPLACE INTO categorylinks "
                    + "(cl_from, cl_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == "
                    + MyNamespace.CATEGORY_NAMESPACE_KEY
                    + " AND page_title == ?;");
            stWriteTpl = db.prepare("REPLACE INTO templatelinks "
                    + "(tl_from, tl_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stWriteLnk = db.prepare("REPLACE INTO pagelinks "
                    + "(pl_from, pl_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stWriteRedirect = db.prepare("REPLACE INTO redirect "
                    + "(rd_from, rd_to) SELECT ?, page_id FROM page "
                    + "WHERE page_namespace == ? AND page_title == ?;");
            stWriteStats = db.prepare("REPLACE INTO site_stats "
                    + "(ss_row_id, ss_total_views, ss_total_edits, ss_good_articles, ss_total_pages) "
                    + "SELECT 1, 0, ?, ?, COUNT(*) FROM page;");
        
            connection = new Connection(db);
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
        }
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
     * Extracts all pages in the given categories from the given DB.
     * 
     * @param allowedCats0
     *            include all pages in these categories (un-normalised page
     *            titles)
     * @param allowedPages0
     *            a number of pages to include, also parses these pages for more
     *            links (un-normalised page titles)
     * @param depth
     *            follow links this deep
     * @param normalised
     *            whether the pages should be returned as normalised page titles
     *            or not
     * 
     * @return a (sorted) set of page titles
     * 
     * @throws RuntimeException
     *             if any error occurs
     */
    public SortedSet<String> getPagesInCategories(
            Collection<String> allowedCats0, Collection<String> allowedPages0, int depth,
            boolean normalised) throws RuntimeException {
        SiteInfo siteInfo = WikiDumpXml2SQLite.readSiteInfo(connection.db);
        MyNamespace namespace = new MyNamespace(siteInfo);
        ArrayList<NormalisedTitle> allowedCats = new ArrayList<NormalisedTitle>(allowedCats0.size());
        MyWikiModel.normalisePageTitles(allowedCats0, namespace, allowedCats);
        ArrayList<NormalisedTitle> allowedPages = new ArrayList<NormalisedTitle>(allowedPages0.size());
        MyWikiModel.normalisePageTitles(allowedPages0, namespace, allowedPages);
        return getPagesInCategories2(allowedCats, allowedPages, depth, normalised);
    }
    
    /**
     * Extracts all pages in the given categories from the given DB.
     * 
     * @param allowedCats
     *            include all pages in these categories (normalised page
     *            titles)
     * @param allowedPages
     *            a number of pages to include, also parses these pages for more
     *            links (normalised page titles)
     * @param depth
     *            follow links this deep
     * @param normalised
     *            whether the pages should be returned as normalised page titles
     *            or not
     * 
     * @return a (sorted) set of page titles
     * 
     * @throws RuntimeException
     *             if any error occurs
     */
    public SortedSet<String> getPagesInCategories2(
            Collection<NormalisedTitle> allowedCats, Collection<NormalisedTitle> allowedPages, int depth,
            boolean normalised) throws RuntimeException {
        try {
            connection.db.exec("CREATE TEMPORARY TABLE currentpages(cp_id INTEGER PRIMARY KEY ASC);");
            SiteInfo siteInfo = WikiDumpXml2SQLite.readSiteInfo(connection.db);
            MyNamespace namespace = new MyNamespace(siteInfo);

            Set<NormalisedTitle> allowedCatsFull = getSubCategories(
                    allowedCats, namespace);

            Set<NormalisedTitle> currentPages = new HashSet<NormalisedTitle>();
            currentPages.addAll(allowedPages);
            currentPages.addAll(allowedCatsFull);
            currentPages.addAll(getPagesDirectlyInCategories(allowedCatsFull));

            Set<NormalisedTitle> normalisedPages = getRecursivePages(currentPages, depth);
            
            // no need to drop table - we set temporary tables to be in-memory only
//            connection.db.exec("DROP TABLE currentpages;");

            // note: need to sort case-sensitively (wiki is only case-insensitive at the first char)
            final TreeSet<String> pages = new TreeSet<String>();
            if (normalised) {
                pages.addAll(ScalarisDataHandlerNormalised.normList2normStringList(normalisedPages));
            } else {
                MyWikiModel.denormalisePageTitles(normalisedPages, namespace, pages);
            }
            return pages;
        } catch (SQLiteException e) {
            error("read of pages in categories failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Gets all sub-categories for the given ones from an SQLite database.
     * 
     * Note: needs a (temporary) <tt>currentpages</tt> table to be set up before this
     * call.
     * 
     * @param allowedCats
     *            include all pages in these categories (normalised page titles)
     * 
     * @return the set of the given categories and all their sub-categories
     *         (normalised)
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private Set<NormalisedTitle> getSubCategories(
            Collection<? extends NormalisedTitle> allowedCats,
            MyNamespace nsObject) throws SQLiteException {
        if (!allowedCats.isEmpty()) {
            SQLiteStatement stmt = null;
            SQLiteStatement stmtCount = null;
            try {
                println(" determining sub-categories of " + allowedCats.toString() + "");
                // first insert all categories:
                stmt = connection.db.prepare("REPLACE INTO currentpages (cp_id) SELECT page_id FROM page WHERE page_title == ?;");
                for (NormalisedTitle pageTitle : allowedCats) {
                    if (pageTitle.namespace.equals(MyNamespace.CATEGORY_NAMESPACE_KEY)) {
                        stmt.bind(1, pageTitle.toString()).stepThrough().reset();
                    }
                }
                stmt.dispose();

                // then recursively determine all sub categories:
                stmt = connection.db
                        .prepare("REPLACE INTO currentpages (cp_id) SELECT cl_from FROM categorylinks "
                                + "INNER JOIN currentpages on cl_to == cp_id "
                                + "INNER JOIN page ON cl_from == page_id WHERE page_namespace == "
                                + MyNamespace.CATEGORY_NAMESPACE_KEY + ";");
                stmtCount = connection.db
                        .prepare("SELECT COUNT(*) FROM currentpages;");
                // note: not necessary to use the correct value at first
                int oldCount = 0;
                int newCount = 0;
                do {
                    oldCount = newCount;
                    stmt.stepThrough().reset();
                    if (stmtCount.step()) {
                        newCount = stmtCount.columnInt(0);
                    }
                    stmtCount.reset();
                    println("  added " + newCount + " categories");
                } while (oldCount != newCount);
                stmt.dispose();
                stmtCount.dispose();

                // now read back all categories:
                stmt = connection.db.prepare("SELECT page_title FROM currentpages INNER JOIN page on page_id == cp_id;");
                Set<NormalisedTitle> allowedCatsFull = new HashSet<NormalisedTitle>(newCount);
                while(stmt.step()) {
                    String title = stmt.columnString(0);
                    allowedCatsFull.add(new NormalisedTitle(MyNamespace.CATEGORY_NAMESPACE_KEY, title));
                }
                stmt.reset();
                connection.db.exec("DELETE FROM currentpages;");
                return allowedCatsFull;
            } finally {
                if (stmt != null) {
                    stmt.dispose();
                }
                if (stmtCount != null) {
                    stmtCount.dispose();
                }
            }
        } else {
            return new HashSet<NormalisedTitle>();
        }
    }
    
    /**
     * Gets all pages in the given category set from an SQLite database. Note:
     * sub-categories are not taken into account.
     * 
     * @param allowedCats
     *            include all pages in these categories (normalised page
     *            titles)
     * @param db
     *            connection to the SQLite database
     * 
     * @return a set of pages (directly) in the given categories (normalised
     *         page titles)
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private Set<NormalisedTitle> getPagesDirectlyInCategories(Set<NormalisedTitle> allowedCats) throws SQLiteException {
        Set<NormalisedTitle> currentPages = new HashSet<NormalisedTitle>();

        if (!allowedCats.isEmpty()) {
            SQLiteStatement stmt = null;
            try {
                println(" determining pages in any of the sub-categories");
                // first insert all categories:
                stmt = connection.db
                        .prepare("REPLACE INTO currentpages (cp_id) "
                                + "SELECT page_id FROM page WHERE page_namespace == "
                                + MyNamespace.CATEGORY_NAMESPACE_KEY
                                + " page_title == ?;");
                for (NormalisedTitle pageTitle : allowedCats) {
                    if (pageTitle.namespace.equals(MyNamespace.CATEGORY_NAMESPACE_KEY)) {
                        stmt.bind(1, pageTitle.toString()).stepThrough().reset();
                    }
                }
                stmt.dispose();
                
                // select all pages belonging to any of the allowed categories:
                stmt = connection.db
                        .prepare("SELECT page_namespace, page_title FROM currentpages " +
                                "INNER JOIN categorylinks ON cl_to == cp_id " +
                                "INNER JOIN page ON cl_from == page_id;");
                while (stmt.step()) {
                    int namespace = stmt.columnInt(0);
                    String title = stmt.columnString(1);
                    currentPages.add(new NormalisedTitle(namespace, title));
                }
                stmt.dispose();
                connection.db.exec("DELETE FROM currentpages;");
            } finally {
                if (stmt != null) {
                    stmt.dispose();
                }
            }
        }
        return currentPages;
    }
    
    /**
     * Gets all pages and their dependencies from an SQLite database, follows
     * links recursively.
     * 
     * Note: needs a (temporary) currentPages table to be set up before this
     * call.
     * 
     * @param currentPages
     *            parse these pages recursively (normalised page titles)
     * @param depth
     *            follow links this deep
     * 
     * @return a set of normalised page titles
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private Set<NormalisedTitle> getRecursivePages(
            Set<NormalisedTitle> currentPages, int depth)
            throws SQLiteException {
        Set<NormalisedTitle> allPages = new HashSet<NormalisedTitle>(100000);
        SQLiteStatement stmt = null;
        try {
            println("adding all mediawiki pages");
            // add all auto-included pages
            stmt = connection.db
                    .prepare("REPLACE INTO currentpages (cp_id) SELECT page_id FROM page WHERE page_namespace == "
                            + MyNamespace.MEDIAWIKI_NAMESPACE_KEY + ";");
            stmt.stepThrough().reset();
            stmt.dispose();

            println("adding " + currentPages.size() + " pages");
            stmt = connection.db.prepare("REPLACE INTO currentpages (cp_id) SELECT page_id FROM page WHERE page_namespace == ? AND page_title == ?;");
            for (NormalisedTitle page : currentPages) {
                stmt.bind(1, page.namespace).bind(2, page.title).stepThrough().reset();
            }
            stmt.dispose();
            allPages.addAll(currentPages);
            while(depth >= 0) {
                println("recursion level: " + depth);

                println(" adding categories of " + allPages.size() + " pages");
                // add all categories the page belongs to
                stmt = connection.db
                        .prepare("REPLACE INTO currentpages (cp_id) SELECT cl_to FROM categorylinks "
                                + "INNER JOIN currentpages on cl_from == cp_id;");
                stmt.stepThrough().dispose();

                println(" adding templates of " + allPages.size() + " pages");
                // add all templates (and their requirements) of the pages
                stmt = connection.db
                        .prepare("REPLACE INTO currentpages (cp_id) SELECT tl_to FROM templatelinks "
                                + "INNER JOIN currentpages on tl_from == cp_id;");
                stmt.stepThrough().dispose();

                // now read back all pages (except auto-included ones):
                stmt = connection.db
                        .prepare("SELECT page_namespace,page_title FROM currentpages "
                                + "INNER JOIN page on page_id == cp_id " 
                                + "WHERE page_namespace != " + MyNamespace.MEDIAWIKI_NAMESPACE_KEY + " ;");
                while(stmt.step()) {
                    int namespace = stmt.columnInt(0);
                    String title = stmt.columnString(1);
                    allPages.add(new NormalisedTitle(namespace, title));
                }
                stmt.reset();

                if (depth > 1) {
                    println(" adding links of " + allPages.size() + " pages");
                    // add all links of the pages for further processing
                    stmt = connection.db
                            .prepare("REPLACE INTO currentpages (cp_id) SELECT pl_to FROM pagelinks "
                                    + "INNER JOIN currentpages on pl_from == cp_id;");
                    stmt.stepThrough().dispose();
                }

                --depth;
            }
            connection.db.exec("DELETE FROM currentPages;");
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
        }
        return allPages;
    }
}
