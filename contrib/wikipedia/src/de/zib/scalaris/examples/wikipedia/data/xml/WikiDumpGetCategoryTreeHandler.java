/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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

import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyParsingWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Provides abilities to read an xml wiki dump file and create a category (and
 * template) tree.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpGetCategoryTreeHandler extends WikiDumpHandler {
    private static final int PRINT_PAGES_EVERY = 400;
    protected String dbFileName;
    protected SQLiteConnection db = null;
    protected SQLiteStatement stGetPageId = null;
    protected SQLiteStatement stWritePages = null;
    protected SQLiteStatement stWriteCategories = null;
    protected SQLiteStatement stWriteTemplates = null;
    protected SQLiteStatement stWriteIncludes = null;
    protected SQLiteStatement stWriteRedirects = null;
    protected SQLiteStatement stWriteLinks = null;
    protected long nextPageId = 0l;
    protected ArrayBlockingQueue<SQLiteJob> sqliteJobs = new ArrayBlockingQueue<SQLiteJob>(PRINT_PAGES_EVERY);
    SQLiteWorker sqliteWorker = new SQLiteWorker();
    
    /**
     * Sets up a SAX XmlHandler extracting all categories from all pages except
     * the ones in a blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param minTime
     *            minimum time a revision should have (only one revision older
     *            than this will be imported) - <tt>null/tt> imports all
     *            revisions
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param dbFileName
     *            the name of the directory to write categories, templates,
     *            inclusions etc to
     * 
     * @throws RuntimeException
     *             if the creation of the SQLite DB fails
     */
    public WikiDumpGetCategoryTreeHandler(Set<String> blacklist,
            Calendar minTime, Calendar maxTime, String dbFileName)
            throws RuntimeException {
        super(blacklist, null, 1, minTime, maxTime);
        this.dbFileName = dbFileName;
    }
    
    static Set<String> readValues(SQLiteStatement stmt, String key)
            throws RuntimeException {
        try {
            try {
                HashSet<String> results = new HashSet<String>();
                stmt.bind(1, key);
                while (stmt.step()) {
                    results.add(stmt.columnString(1));
                }
                return results;
            } finally {
                stmt.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("read of " + key + " failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        }
    }
    
    protected void addSQLiteJob(SQLiteJob job) throws RuntimeException {
        try {
            sqliteJobs.put(job);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeValue(SQLiteStatement stmt, String key, String value)
            throws RuntimeException {
        addSQLiteJob(new SQLiteWriteValuesJob(stmt, key, value));
    }

    protected void writeValues(SQLiteStatement stmt, String key, Collection<? extends String> values)
            throws RuntimeException {
        addSQLiteJob(new SQLiteWriteValuesJob(stmt, key, values));
    }

    protected void writeSiteInfo(SiteInfo siteInfo)
            throws RuntimeException {
        addSQLiteJob(new SQLiteWriteSiteInfoJob(siteInfo));
    }

    static SiteInfo readSiteInfo(SQLiteConnection db) throws RuntimeException {
        SQLiteStatement stmt = null;
        try {
            stmt = db.prepare("SELECT value FROM properties WHERE key == ?");
            return WikiDumpPrepareSQLiteForScalarisHandler.readObject(stmt, "siteinfo");
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
        }
    }
    
    protected static void updateMap(Map<String, Set<String>> map, String key, String addToValue) {
        Set<String> oldValue = map.get(key);
        if (oldValue == null) {
            oldValue = new HashSet<String>();
            map.put(key, oldValue);
        }
        oldValue.add(addToValue);
    }
    
    protected static void updateMap(Map<String, Set<String>> map, String key, Collection<? extends String> addToValues) {
        Set<String> oldValue = map.get(key);
        if (oldValue == null) {
            oldValue = new HashSet<String>(addToValues);
            map.put(key, oldValue);
        } else {
            oldValue.addAll(addToValues);
        }
    }

    /**
     * Exports the given siteinfo (nothing to do here).
     * 
     * @param revisions
     *            the siteinfo to export
     */
    @Override
    protected void export(XmlSiteInfo siteinfo_xml) {
        writeSiteInfo(siteinfo_xml.getSiteInfo());
    }

    /**
     * Builds the category tree.
     * 
     * @param page_xml
     *            the page object extracted from XML
     */
    @Override
    protected void export(XmlPage page_xml) {
        Page page = page_xml.getPage();

        if (page.getCurRev() != null && wikiModel != null) {
            wikiModel.setUp();
            final String pageTitle = page.getTitle();
            wikiModel.setPageName(pageTitle);
            wikiModel.render(null, page.getCurRev().unpackedText());
            
            // categories:
            do {
                final Set<String> pageCategories_raw = wikiModel.getCategories().keySet();
                ArrayList<String> pageCategories = new ArrayList<String>(pageCategories_raw.size());
                for (String cat_raw: pageCategories_raw) {
                    String category = (wikiModel.getCategoryNamespace() + ":" + cat_raw);
                    pageCategories.add(category);
                }
                writeValues(stWriteCategories, pageTitle, pageCategories);
            } while(false);
            
            // templates:
            do {
                final Set<String> pageTemplates_raw = wikiModel.getTemplates();
                ArrayList<String> pageTemplates = new ArrayList<String>(pageTemplates_raw.size());
                for (String tpl_raw: pageTemplates_raw) {
                    String template = (wikiModel.getTemplateNamespace() + ":" + tpl_raw);
                    pageTemplates.add(template);
                }
                writeValues(stWriteTemplates, pageTitle, pageTemplates);
            } while (false);
            
            // includes:
            do {
                Set<String> pageIncludes = wikiModel.getIncludes();
                if (!pageIncludes.isEmpty()) {
                    // make sure, the set is not changed anymore (deferred processing in the thread):
                    writeValues(stWriteIncludes, pageTitle, new ArrayList<String>(pageIncludes));
                }
            } while (false);
            
            // redirections:
            do {
                String pageRedirLink = wikiModel.getRedirectLink();
                if (pageRedirLink != null) {
                    writeValue(stWriteRedirects, pageTitle, pageRedirLink);
                }
            } while(false);
            
            // links:
            do {
                Set<String> pageLinks = wikiModel.getLinks();
                if (!pageLinks.isEmpty()) {
                    // make sure, the set is not changed anymore (deferred processing in the thread):
                    writeValues(stWriteLinks, pageTitle, new ArrayList<String>(pageLinks));
                }
            } while(false);
            
            wikiModel.tearDown();
        }
        ++pageCount;
        // only export page list every UPDATE_PAGELIST_EVERY pages:
        if ((pageCount % PRINT_PAGES_EVERY) == 0) {
            println("processed pages: " + pageCount);
        }
    }

    /**
     * Gets all sub categories that belong to a given root category
     * (recursively).
     * 
     * @param tree
     *            the tree of categories or templates as created by
     *            {@link #readTrees(String, Map, Map, Map)}
     * @param root
     *            a root category or template
     * 
     * @return a set of all sub categories/templates; also includes the root
     */
    public static Set<String> getAllChildren(Map<String, Set<String>> tree, String root) {
        return getAllChildren(tree, new LinkedList<String>(Arrays.asList(root)));
    }
    
    /**
     * Gets all sub categories that belong to any of the given root categories
     * (recursively).
     * 
     * @param tree
     *            the tree of categories or templates as created by
     *            {@link #readTrees(String, Map, Map, Map)}
     * @param roots
     *            a list of root categories or templates
     * 
     * @return a set of all sub categories; also includes the rootCats
     */
    public static Set<String> getAllChildren(Map<String, Set<String>> tree, List<String> roots) {
        HashSet<String> allChildren = new HashSet<String>(roots);
        while (!roots.isEmpty()) {
            String curChild = roots.remove(0);
            Set<String> subChilds = tree.get(curChild);
            if (subChilds != null) {
                // only add new children to the root list
                // (remove already processed ones)
                // -> prevents endless loops in circles
                Set<String> newChilds = new HashSet<String>(subChilds);
                newChilds.removeAll(allChildren);
                allChildren.addAll(newChilds);
                roots.addAll(newChilds);
            }
        }
        return allChildren;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#setUp()
     */
    @Override
    public void setUp() {
        super.setUp();
        sqliteWorker.start();
        // wait for worker to initialise the DB and the prepared statements
        while (!sqliteWorker.initialised) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        sqliteWorker.stopWhenQueueEmpty = true;
        addSQLiteJob(new SQLiteNoOpJob());
        // wait for worker to close the DB
        try {
            sqliteWorker.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        importEnd();
    }
    
    /**
     * Reads the given parameter trees from the DB file.
     * 
     * @param dbFileName
     *            name of the DB file
     * @param templateTree
     *            information about the templates and their dependencies
     * @param includeTree
     *            information about page includes
     * @param referenceTree
     *            information about references to a page
     * 
     * @throws RuntimeException if any error occurs
     */
    public static void readTrees(
            String dbFileName,
            Map<String, Set<String>> templateTree,
            Map<String, Set<String>> includeTree,
            Map<String, Set<String>> referenceTree)
            throws RuntimeException {
        SQLiteConnection db = null;
        SQLiteStatement stmt = null;
        try {
            db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName, true);
            SiteInfo siteInfo = readSiteInfo(db);
            MyParsingWikiModel wikiModel = new MyParsingWikiModel("", "", new MyNamespace(siteInfo));
            stmt = db
                    .prepare("SELECT page.title, tpl.title FROM " +
                            "templates INNER JOIN pages AS page ON templates.title == page.id " +
                            "INNER JOIN pages AS tpl ON templates.template == tpl.id " +
                            "WHERE page.title LIKE '" + wikiModel.normalisePageTitle(wikiModel.getTemplateNamespace() + ":") + "%';");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0);
                String template = stmt.columnString(1);
                updateMap(templateTree, pageTitle, template);
            }
            stmt.dispose();
            stmt = db
                    .prepare("SELECT page.title, incl.title FROM " +
                            "includes INNER JOIN pages AS page ON includes.title == page.id " +
                            "INNER JOIN pages AS incl ON includes.include == incl.id;");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0);
                String include = stmt.columnString(1);
                updateMap(includeTree, pageTitle, include);
            }
            stmt.dispose();
            stmt = db
                    .prepare("SELECT page.title, redir.title FROM " +
                            "redirects INNER JOIN pages AS page ON redirects.title == page.id " +
                            "INNER JOIN pages AS redir ON redirects.redirect == redir.id;");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0);
                String redirect = stmt.columnString(1);
                updateMap(referenceTree, redirect, pageTitle);
            }
        } catch (SQLiteException e) {
            System.err.println("read of category tree failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
            if (db != null) {
                db.dispose();
            }
        }
    }
    
    /**
     * Extracts all pages in the given categories from the given DB.
     * 
     * @param dbFileName
     *            name of the DB file
     * @param allowedCats
     *            include all pages in these categories
     * @param allowedPages0
     *            a number of pages to include (also parses these pages for more
     *            links - will be normalised)
     * @param depth
     *            follow links this deep
     * @param templateTree
     *            information about the templates and their dependencies
     * @param includeTree
     *            information about page includes
     * @param referenceTree
     *            information about references to a page
     * @param msgOut
     *            the output stream to write status messages to
     *            
     * @return full list of allowed pages
     * 
     * @throws RuntimeException
     *             if any error occurs
     */
    public static Set<String> getPagesInCategories(String dbFileName,
            Set<String> allowedCats, Set<String> allowedPages0, int depth,
            Map<String, Set<String>> templateTree,
            Map<String, Set<String>> includeTree,
            Map<String, Set<String>> referenceTree,
            PrintStream msgOut) throws RuntimeException {
        SQLiteConnection db = null;
        try {
            db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName, true);
            db.exec("CREATE TEMPORARY TABLE currentPages(id INTEGER PRIMARY KEY ASC);");

            Set<String> allowedCatsFull = getSubCategories(allowedCats, db,
                    templateTree, includeTree, referenceTree, msgOut);

            SiteInfo siteInfo = readSiteInfo(db);
            MyNamespace namespace = new MyNamespace(siteInfo);
            Set<String> allowedPages = new HashSet<String>(allowedPages0.size());
            MyWikiModel.normalisePageTitles(allowedPages0, namespace, allowedPages);

            Set<String> currentPages = new HashSet<String>();
            currentPages.addAll(allowedPages);
            currentPages.addAll(allowedCatsFull);
            currentPages.addAll(getPagesDirectlyInCategories(allowedCatsFull, db));

            Set<String> pages = getRecursivePages(currentPages, depth, db,
                    templateTree, includeTree, referenceTree, msgOut);
            
            // no need to drop table - we set temporary tables to be in-memory only
//            db.exec("DROP TABLE currentPages;");
            
            return pages;
        } catch (SQLiteException e) {
            System.err.println("read of pages in categories failed (sqlite error: " + e.toString() + ")");
            throw new RuntimeException(e);
        } finally {
            if (db != null) {
                db.dispose();
            }
        }
    }
    
    /**
     * Gets all sub-categories for the given ones from an SQLite database.
     * 
     * Note: needs a (temporary) currentPages table to be set up before this
     * call.
     * 
     * @param allowedCats
     *            include all pages in these categories
     * @param db
     *            connection to the SQLite database
     * @param templateTree
     *            information about the templates and their dependencies
     * @param includeTree
     *            information about page includes
     * @param referenceTree
     *            information about references to a page
     * @param msgOut
     *            the output stream to write status messages to
     * 
     * @return the set of the given categories and all their sub-categories
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private static Set<String> getSubCategories(Set<String> allowedCats,
            SQLiteConnection db, Map<String, Set<String>> templateTree,
            Map<String, Set<String>> includeTree,
            Map<String, Set<String>> referenceTree,
            PrintStream msgOut) throws SQLiteException {
        Set<String> allowedCatsFull = new HashSet<String>();
        Set<String> currentPages = new HashSet<String>();
        Set<String> newPages = new HashSet<String>();
        
        if (!allowedCats.isEmpty()) {
            SQLiteStatement stmt = null;
            try {
                // need to extend the category set by all sub-categories:
                currentPages.addAll(allowedCats);
                SiteInfo siteInfo = readSiteInfo(db);
                MyParsingWikiModel wikiModel = new MyParsingWikiModel("", "", new MyNamespace(siteInfo));

                println(msgOut, " determining sub-categories of " + allowedCats.toString() + "");
                do {
                    stmt = db.prepare("INSERT INTO currentPages (id) SELECT pages.id FROM pages WHERE pages.title == ?;");
                    for (String pageTitle : currentPages) {
                        addToPages(allowedCatsFull, newPages, pageTitle, includeTree, referenceTree);
                        // beware: add pageTitle to allowedCatsFull _AFTER_ adding its dependencies
                        // (otherwise the dependencies won't be added)
                        allowedCatsFull.add(pageTitle);
                        stmt.bind(1, pageTitle).stepThrough().reset();
                    }
                    stmt.dispose();

                    println(msgOut, "  adding sub-categories of " + currentPages.size() + " categories or templates");
                    // add all categories the page belongs to
                    stmt = db
                            .prepare("SELECT page.title FROM categories " +
                                    "INNER JOIN currentPages AS cp ON categories.category == cp.id " +
                                    "INNER JOIN pages AS page ON categories.title == page.id " +
                                    // "INNER JOIN pages AS cat ON categories.category == cat.id" +
                                    "WHERE page.title LIKE '" + wikiModel.normalisePageTitle(wikiModel.getCategoryNamespace() + ":") + "%';");
                    while (stmt.step()) {
                        String pageCategory = stmt.columnString(0);
                        addToPages(allowedCatsFull, newPages, pageCategory, includeTree, referenceTree);
                    }
                    stmt.dispose();
                    println(msgOut, "  adding sub-templates or -categories of " + currentPages.size() + " categories or templates");
                    // add all templates (and their requirements) of the pages
                    stmt = db
                            .prepare("SELECT page.title FROM templates " +
                                    "INNER JOIN currentPages AS cp ON templates.template == cp.id " +
                                    "INNER JOIN pages AS page ON templates.title == page.id " +
                                    // "INNER JOIN pages AS tpl ON templates.template == tpl.id" +
                                    "WHERE page.title LIKE '" + wikiModel.normalisePageTitle(wikiModel.getCategoryNamespace() + ":") + "%' OR "
                                    + "page.title LIKE '" + wikiModel.normalisePageTitle(wikiModel.getTemplateNamespace() + ":") + "%';");
                    while (stmt.step()) {
                        String pageTemplate = stmt.columnString(0);
                        Set<String> tplChildren = WikiDumpGetCategoryTreeHandler.getAllChildren(templateTree, pageTemplate);
                        addToPages(allowedCatsFull, newPages, tplChildren, includeTree, referenceTree);
                    }
                    stmt.dispose();
                    db.exec("DELETE FROM currentPages;");
                    if (newPages.isEmpty()) {
                        break;
                    } else {
                        println(msgOut, " adding " + newPages.size() + " dependencies");
                        currentPages = newPages;
                        newPages = new HashSet<String>();
                    }
                } while (true);
            } finally {
                if (stmt != null) {
                    stmt.dispose();
                }
            }
        }
        return allowedCatsFull;
    }
    
    /**
     * Gets all pages in the given category set from an SQLite database.
     * Note: sub-categories are not taken into account.
     * 
     * @param allowedCats
     *            include all pages in these categories
     * @param db
     *            connection to the SQLite database
     * 
     * @return a set of pages (directly) in the given categories
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private static Set<String> getPagesDirectlyInCategories(Set<String> allowedCats,
            SQLiteConnection db) throws SQLiteException {
        Set<String> currentPages = new HashSet<String>();

        // note: allowedCatsFull can contain categories or templates
        if (!allowedCats.isEmpty()) {
            SQLiteStatement stmt = null;
            try {
                // select all pages belonging to any of the allowed categories:
                stmt = db
                        .prepare("SELECT page.title, cat.title FROM " +
                                "categories INNER JOIN pages AS page ON categories.title == page.id " +
                                "INNER JOIN pages AS cat ON categories.category == cat.id;");
                while (stmt.step()) {
                    String pageTitle = stmt.columnString(0);
                    String pageCategory = stmt.columnString(1);
                    if (allowedCats.contains(pageCategory)) {
                        currentPages.add(pageTitle);
                    }
                }
                stmt.dispose();
                // select all pages belonging to any of the allowed templates:
                stmt = db
                        .prepare("SELECT page.title, tpl.title FROM " +
                                "templates INNER JOIN pages AS page ON templates.title == page.id " +
                                "INNER JOIN pages AS tpl ON templates.template == tpl.id;");
                while (stmt.step()) {
                    String pageTitle = stmt.columnString(0);
                    String pageTemplate = stmt.columnString(1);
                    if (allowedCats.contains(pageTemplate)) {
                        currentPages.add(pageTitle);
                    }
                }
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
     *            parse these pages recursively
     * @param depth
     *            follow links this deep
     * @param db
     *            connection to the SQLite database
     * @param templateTree
     *            information about the templates and their dependencies
     * @param includeTree
     *            information about page includes
     * @param referenceTree
     *            information about references to a page
     * @param msgOut
     *            the output stream to write status messages to
     * 
     * @return the whole set of pages
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private static Set<String> getRecursivePages(Set<String> currentPages,
            int depth, SQLiteConnection db, Map<String, Set<String>> templateTree,
            Map<String, Set<String>> includeTree,
            Map<String, Set<String>> referenceTree,
            PrintStream msgOut)
            throws SQLiteException {
        Set<String> allPages = new HashSet<String>();
        Set<String> newPages = new HashSet<String>();
        Set<String> pageLinks = new HashSet<String>();
        SQLiteStatement stmt = null;
        try {
            while(depth >= 0) {
                println(msgOut, "recursion level: " + depth);
                println(msgOut, " adding " + currentPages.size() + " pages");
                do {
                    stmt = db.prepare("INSERT INTO currentPages (id) SELECT pages.id FROM pages WHERE pages.title == ?;");
                    for (String pageTitle : currentPages) {
                        addToPages(allPages, newPages, pageTitle, includeTree, referenceTree);
                        // beware: add pageTitle to pages _AFTER_ adding its dependencies
                        // (otherwise the dependencies won't be added)
                        allPages.add(pageTitle);
                        stmt.bind(1, pageTitle).stepThrough().reset();
                    }
                    stmt.dispose();

                    println(msgOut, "  adding categories of " + currentPages.size() + " pages");
                    // add all categories the page belongs to
                    stmt = db
                            .prepare("SELECT cat.title FROM categories " +
                                    "INNER JOIN currentPages AS cp ON categories.title == cp.id " +
                                    // "INNER JOIN pages AS page ON categories.title == page.id " +
                                    "INNER JOIN pages AS cat ON categories.category == cat.id;");
                    while (stmt.step()) {
                        String pageCategory = stmt.columnString(0);
                        addToPages(allPages, newPages, pageCategory, includeTree, referenceTree);
                    }
                    stmt.dispose();
                    println(msgOut, "  adding templates of " + currentPages.size() + " pages");
                    // add all templates (and their requirements) of the pages
                    stmt = db
                            .prepare("SELECT tpl.title FROM templates " +
                                    "INNER JOIN currentPages AS cp ON templates.title == cp.id " +
                                    // "INNER JOIN pages AS page ON templates.title == page.id " +
                                    "INNER JOIN pages AS tpl ON templates.template == tpl.id;");
                    while (stmt.step()) {
                        String pageTemplate = stmt.columnString(0);
                        Set<String> tplChildren = WikiDumpGetCategoryTreeHandler.getAllChildren(templateTree, pageTemplate);
                        addToPages(allPages, newPages, tplChildren, includeTree, referenceTree);
                    }
                    stmt.dispose();
                    println(msgOut, "  adding links of " + currentPages.size() + " pages");
                    // add all links of the pages for further processing
                    stmt = db
                            .prepare("SELECT lnk.title FROM links " +
                                    "INNER JOIN currentPages AS cp ON links.title == cp.id " +
                                    // "INNER JOIN pages AS page ON links.title == page.id " +
                                    "INNER JOIN pages AS lnk ON links.link == lnk.id;");
                    while (stmt.step()) {
                        String pageLink = stmt.columnString(0);
                        if (!pageLink.isEmpty()) { // there may be empty links
                            pageLinks.add(pageLink);
                        }
                    }
                    stmt.dispose();
                    db.exec("DELETE FROM currentPages;");
                    if (newPages.isEmpty()) {
                        break;
                    } else {
                        println(msgOut, " adding " + newPages.size() + " dependencies");
                        currentPages = newPages;
                        newPages = new HashSet<String>();
                    }
                } while (true);
                // for the next recursion:
                currentPages = pageLinks;
                pageLinks = new HashSet<String>();
                --depth;
            }
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
        }
        return allPages;
    }
    
    static protected void addToPages(Set<String> pages, Set<String> newPages, String title, Map<String, Set<String>> includeTree, Map<String, Set<String>> referenceTree) {
        if (!pages.contains(title) && newPages.add(title)) {
            // title not yet in pages -> add includes, redirects and pages redirecting to this page
            addToPages(pages, newPages, WikiDumpGetCategoryTreeHandler.getAllChildren(includeTree, title), includeTree, referenceTree); // also has redirects
            addToPages(pages, newPages, WikiDumpGetCategoryTreeHandler.getAllChildren(referenceTree, title), includeTree, referenceTree);
        }
    }
    
    static protected void addToPages(Set<String> pages, Set<String> newPages, Collection<? extends String> titles, Map<String, Set<String>> includeTree, Map<String, Set<String>> referenceTree) {
        for (String title : titles) {
            addToPages(pages, newPages, title, includeTree, referenceTree);
        }
    }
    
    protected class SQLiteWorker extends Thread {
        boolean stopWhenQueueEmpty = false;
        boolean initialised = false;
        
        @Override
        public void run() {
            try {
                // set up DB:
                try {
                    db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName, false);
                    db.exec("CREATE TABLE pages(id INTEGER PRIMARY KEY ASC, title STRING);");
                    db.exec("CREATE INDEX page_titles ON pages(title);");
                    db.exec("CREATE TABLE categories(title INTEGER, category INTEGER);");
                    db.exec("CREATE TABLE templates(title INTEGER, template INTEGER);");
                    db.exec("CREATE TABLE includes(title INTEGER, include INTEGER);");
                    db.exec("CREATE TABLE redirects(title INTEGER, redirect INTEGER);");
                    db.exec("CREATE TABLE links(title INTEGER, link INTEGER);");
                    db.exec("CREATE TABLE properties(key STRING PRIMARY KEY ASC, value);");
                    stGetPageId = db.prepare("SELECT id FROM pages WHERE title == ?;");
                    stWritePages = db.prepare("INSERT INTO pages (id, title) VALUES (?, ?);");
                    stWriteCategories = db.prepare("INSERT INTO categories (title, category) VALUES (?, ?);");
                    stWriteTemplates = db.prepare("INSERT INTO templates (title, template) VALUES (?, ?);");
                    stWriteIncludes = db.prepare("INSERT INTO includes (title, include) VALUES (?, ?);");
                    stWriteRedirects = db.prepare("INSERT INTO redirects (title, redirect) VALUES (?, ?);");
                    stWriteLinks = db.prepare("INSERT INTO links (title, link) VALUES (?, ?);");
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                }
                initialised = true;

                // take jobs

                while(!(sqliteJobs.isEmpty() && stopWhenQueueEmpty)) {
                    SQLiteJob job;
                    try {
                        job = sqliteJobs.take();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    job.run();
                }
                try {
                    db.exec("CREATE INDEX cat_titles ON categories(title);");
                    db.exec("CREATE INDEX tpl_titles ON templates(title);");
                    db.exec("CREATE INDEX incl_titles ON includes(title);");
                    db.exec("CREATE INDEX redir_titles ON redirects(title);");
                    db.exec("CREATE INDEX lnk_titles ON links(title);");
                } catch (SQLiteException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                if (stGetPageId != null) {
                    stGetPageId.dispose();
                }
                if (stWritePages != null) {
                    stWritePages.dispose();
                }
                if (stWriteCategories != null) {
                    stWriteCategories.dispose();
                }
                if (stWriteTemplates != null) {
                    stWriteTemplates.dispose();
                }
                if (stWriteIncludes != null) {
                    stWriteIncludes.dispose();
                }
                if (stWriteRedirects != null) {
                    stWriteRedirects.dispose();
                }
                if (stWriteLinks != null) {
                    stWriteLinks.dispose();
                }
                if (db != null) {
                    db.dispose();
                }
                initialised = false;
            }
        }
    }
    
    protected static interface SQLiteJob {
        public abstract void run();
    };
    
    protected static class SQLiteNoOpJob implements SQLiteJob {
        @Override
        public void run() {
        }
    }
    
    protected class SQLiteWriteValuesJob implements SQLiteJob {
        SQLiteStatement stmt;
        String key;
        Collection<? extends String> values;
        
        public SQLiteWriteValuesJob(SQLiteStatement stmt, String key, String value) {
            this.stmt = stmt;
            this.key = key;
            this.values = Arrays.asList(value);
        }
        
        public SQLiteWriteValuesJob(SQLiteStatement stmt, String key, Collection<? extends String> values) {
            this.stmt = stmt;
            this.key = key;
            this.values = values;
        }

        protected long pageToId(String origPageTitle) throws RuntimeException {
            String pageTitle = wikiModel.normalisePageTitle(origPageTitle);
            try {
                long pageId = -1;
                // try to find the page id in the pages table:
                try {
                    stGetPageId.bind(1, pageTitle);
                    if (stGetPageId.step()) {
                        pageId = stGetPageId.columnLong(0);
                    }
                } finally {
                    stGetPageId.reset();
                }
                // page not found yet -> add to pages table:
                if (pageId == -1) {
                    pageId = nextPageId++;
                    try {
                        stWritePages.bind(1, pageId).bind(2, pageTitle).stepThrough();
                    } finally {
                        stWritePages.reset();
                    }
                }
                return pageId;
            } catch (SQLiteException e) {
                System.err.println("write of " + pageTitle + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void run() {
            long key_id = pageToId(key);
            LinkedList<Long> values_id = new LinkedList<Long>();
            for (String value : values) {
                values_id.add(pageToId(value));
            }
            try {
                try {
                    stmt.bind(1, key_id);
                    for (Long value_id : values_id) {
                        stmt.bind(2, value_id).stepThrough().reset(false);
                    }
                } finally {
                    stmt.reset();
                }
            } catch (SQLiteException e) {
                System.err.println("write of " + key + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
    }
    
    protected class SQLiteWriteSiteInfoJob implements SQLiteJob {
        SiteInfo siteInfo;
        
        public SQLiteWriteSiteInfoJob(SiteInfo siteInfo) {
            this.siteInfo = siteInfo;
        }
        
        @Override
        public void run() {
            SQLiteStatement stmt = null;
            try {
                stmt = db.prepare("REPLACE INTO properties (key, value) VALUES (?, ?);");
                WikiDumpPrepareSQLiteForScalarisHandler.writeObject(stmt, "siteinfo", siteInfo);
            } catch (SQLiteException e) {
                throw new RuntimeException(e);
            } finally {
                if (stmt != null) {
                    stmt.dispose();
                }
            }
        }
    }
}
