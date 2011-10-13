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
            Calendar maxTime, String dbFileName) throws RuntimeException {
        super(blacklist, null, 1, maxTime);
        this.dbFileName = dbFileName;
    }

    static SQLiteStatement createReadCategoriesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT category FROM categories WHERE title == ?;");
    }

    static SQLiteStatement createReadTemplatesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT template FROM templates WHERE title == ?;");
    }

    static SQLiteStatement createReadIncludesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT include FROM includes WHERE title == ?;");
    }

    static SQLiteStatement createReadRedirectsStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT redirect FROM redirects WHERE title == ?;");
    }

    static SQLiteStatement createReadLinksStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT link FROM links WHERE title == ?;");
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
            wikiModel.render(null, page.getCurRev().getText());
            
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
            msgOut.println("processed pages: " + pageCount);
        }
    }

    /**
     * Gets all sub categories that belong to a given root category
     * (recursively).
     * 
     * @param tree
     *            the tree of categories or templates as created by
     *            {@link #readTrees(String, Map, Map, Map, Map)}
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
     *            {@link #readTrees(String, Map, Map, Map, Map)}
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
        importEnd();
        // wait for worker to close the DB
        while (sqliteWorker.initialised) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
    /**
     * Reads the given parameter trees from the DB file.
     * 
     * @param dbFileName
     *            name of the DB file
     * @param categoryTree
     *            information about the categories and their dependencies
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
            Map<String, Set<String>> categoryTree,
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
                    .prepare("SELECT page.title, cat.title FROM " +
                            "categories INNER JOIN pages AS page ON categories.title == page.id " +
                            "INNER JOIN pages AS cat ON categories.category == cat.id " +
                            "WHERE page.title LIKE '" + MyWikiModel.normalisePageTitle(wikiModel.getCategoryNamespace() + ":") + "%';");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0);
                String category = stmt.columnString(1);
                updateMap(categoryTree, category, pageTitle);
            }
            stmt.dispose();
            stmt = db
                    .prepare("SELECT page.title, tpl.title FROM " +
                            "templates INNER JOIN pages AS page ON templates.title == page.id " +
                            "INNER JOIN pages AS tpl ON templates.template == tpl.id " +
                            "WHERE page.title LIKE '" + MyWikiModel.normalisePageTitle(wikiModel.getCategoryNamespace() + ":") + "%' OR "
                            + "page.title LIKE '" + MyWikiModel.normalisePageTitle(wikiModel.getTemplateNamespace() + ":") + "%';");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0);
                String template = stmt.columnString(1);
                final String namespace = MyWikiModel.getNamespace(pageTitle);
                updateMap(categoryTree, template, pageTitle);
                if (wikiModel.isTemplateNamespace(namespace)) {
                    updateMap(templateTree, pageTitle, template);
                }
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
     * @param allowedPages
     *            a number of pages to include (also parses these pages for more
     *            links)
     * @param depth
     *            follow links this deep
     * @param templateTree
     *            information about the templates and their dependencies
     * @param includeTree
     *            information about page includes
     * @param referenceTree
     *            information about references to a page
     *            
     * @return full list of allowed pages
     * 
     * @throws RuntimeException
     *             if any error occurs
     */
    public static Set<String> getPagesInCategories(String dbFileName,
            Set<String> allowedCats, Set<String> allowedPages, int depth,
            Map<String, Set<String>> templateTree,
            Map<String, Set<String>> includeTree,
            Map<String, Set<String>> referenceTree) throws RuntimeException {
        Set<String> pages = new HashSet<String>();
        Set<String> pageLinks = new HashSet<String>();
        SQLiteConnection db = null;
        SQLiteStatement stmt = null;
        try {
            db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName, true);
            Set<String> currentPages = new HashSet<String>(allowedPages);
            currentPages.addAll(allowedCats);

            // note: allowedCats can contain categories or templates
            if (!allowedCats.isEmpty()) {
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
                stmt.dispose();
            }

            pageLinks = new HashSet<String>();
            Set<String> newPages = new HashSet<String>();
            db.exec("CREATE TEMPORARY TABLE currentPages(id INTEGER PRIMARY KEY ASC);");
            while(depth >= 0) {
                System.out.println("recursion level: " + depth);
                System.out.println(" adding " + currentPages.size() + " pages");
                do {
                    stmt = db.prepare("INSERT INTO currentPages (id) SELECT pages.id FROM pages WHERE pages.title == ?;");
                    for (String pageTitle : currentPages) {
                        addToPages(pages, newPages, pageTitle, includeTree, referenceTree);
                        // beware: add pageTitle to pages _AFTER_ adding its dependencies
                        // (otherwise the dependencies won't be added)
                        pages.add(pageTitle);
                        stmt.bind(1, pageTitle).stepThrough().reset();
                    }
                    stmt.reset();

                    System.out.println("  adding categories of " + currentPages.size() + " pages");
                    // add all categories the page belongs to
                    stmt = db
                            .prepare("SELECT cat.title FROM categories " +
                                    "INNER JOIN currentPages AS cp ON categories.title == cp.id " +
                                    "INNER JOIN pages AS page ON categories.title == page.id " +
                                    "INNER JOIN pages AS cat ON categories.category == cat.id;");
                    while (stmt.step()) {
                        String pageCategory = stmt.columnString(0);
                        addToPages(pages, newPages, pageCategory, includeTree, referenceTree);
                    }
                    stmt.reset();
                    System.out.println("  adding templates of " + currentPages.size() + " pages");
                    // add all templates (and their requirements) of the pages
                    stmt = db
                            .prepare("SELECT tpl.title FROM templates " +
                                    "INNER JOIN currentPages AS cp ON templates.title == cp.id " +
                                    "INNER JOIN pages AS page ON templates.title == page.id " +
                                    "INNER JOIN pages AS tpl ON templates.template == tpl.id;");
                    while (stmt.step()) {
                        String pageTemplate = stmt.columnString(0);
                        Set<String> tplChildren = WikiDumpGetCategoryTreeHandler.getAllChildren(templateTree, pageTemplate);
                        addToPages(pages, newPages, tplChildren, includeTree, referenceTree);
                    }
                    stmt.reset();
                    System.out.println("  adding links of " + currentPages.size() + " pages");
                    // add all links of the pages for further processing
                    stmt = db
                            .prepare("SELECT lnk.title FROM links " +
                                    "INNER JOIN currentPages AS cp ON links.title == cp.id " +
                                    "INNER JOIN pages AS page ON links.title == page.id " +
                                    "INNER JOIN pages AS lnk ON links.link == lnk.id;");
                    while (stmt.step()) {
                        String pageLink = stmt.columnString(0);
                        if (!pageLink.isEmpty()) { // there may be empty links
                            pageLinks.add(pageLink);
                        }
                    }
                    stmt.reset();
                    db.exec("DELETE FROM currentPages;");
                    if (newPages.isEmpty()) {
                        break;
                    } else {
                        System.out.println(" adding " + newPages.size() + " dependencies");
                        currentPages = newPages;
                        newPages = new HashSet<String>();
                    }
                } while (true);
                // for the next recursion:
                currentPages = pageLinks;
                pageLinks = new HashSet<String>();
                --depth;
            }
            // no need to drop table - we set temporary tables to be in-memory only
//            db.exec("DROP TABLE currentPages;");
            
            return pages;
        } catch (SQLiteException e) {
            System.err.println("read of pages in categories failed (sqlite error: " + e.toString() + ")");
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
            // set up DB:
            try {
                db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName, false);
                db.exec("CREATE TABLE pages(id INTEGER PRIMARY KEY ASC, title STRING);");
                db.exec("CREATE INDEX page_titles ON pages(title);");
                db.exec("CREATE TABLE categories(title INTEGER, category INTEGER);");
                db.exec("CREATE INDEX cat_titles ON categories(title);");
                db.exec("CREATE TABLE templates(title INTEGER, template INTEGER);");
                db.exec("CREATE INDEX tpl_titles ON templates(title);");
                db.exec("CREATE TABLE includes(title INTEGER, include INTEGER);");
                db.exec("CREATE INDEX incl_titles ON includes(title);");
                db.exec("CREATE TABLE redirects(title INTEGER, redirect INTEGER);");
                db.exec("CREATE INDEX redir_titles ON redirects(title);");
                db.exec("CREATE TABLE links(title INTEGER, link INTEGER);");
                db.exec("CREATE INDEX lnk_titles ON links(title);");
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
            if (db != null) {
                db.dispose();
            }
            initialised = false;
        }
    }
    
    protected static interface SQLiteJob {
        public abstract void run();
    };
    
    protected class SQLiteNoOpJob implements SQLiteJob {
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
            String pageTitle = MyWikiModel.normalisePageTitle(origPageTitle);
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
