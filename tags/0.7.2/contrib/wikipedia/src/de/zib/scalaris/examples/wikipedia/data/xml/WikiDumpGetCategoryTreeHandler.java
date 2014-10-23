/**
 *  Copyright 2007-2013 Zuse Institute Berlin
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
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;

import com.almworks.sqlite4java.SQLiteConnection;
import com.almworks.sqlite4java.SQLiteException;
import com.almworks.sqlite4java.SQLiteStatement;

import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerNormalised;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
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
    protected ArrayBlockingQueue<SQLiteJob> sqliteJobs = new ArrayBlockingQueue<SQLiteJob>(100);
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
    
    protected void addSQLiteJob(SQLiteJob job) throws RuntimeException {
        try {
            sqliteJobs.put(job);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected void writeValue(SQLiteStatement stmt, NormalisedTitle key, NormalisedTitle value)
            throws RuntimeException {
        addSQLiteJob(new SQLiteWriteValuesJob(stmt, key, value));
    }

    protected void writeValues(SQLiteStatement stmt, NormalisedTitle key, Collection<? extends NormalisedTitle> values)
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
            return WikiDumpPrepareSQLiteForScalarisHandler.readObject2(stmt, "siteinfo").jsonValue(SiteInfo.class);
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
    
    protected static void updateMap(
            Map<NormalisedTitle, Set<NormalisedTitle>> map,
            NormalisedTitle key, NormalisedTitle addToValue) {
        Set<NormalisedTitle> oldValue = map.get(key);
        if (oldValue == null) {
            oldValue = new HashSet<NormalisedTitle>();
            map.put(key, oldValue);
        }
        oldValue.add(addToValue);
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
            final NormalisedTitle normTitle = wikiModel.normalisePageTitle(page.getTitle());
            wikiModel.setNamespaceName(wikiModel.getNamespace().getNamespaceByNumber(normTitle.namespace));
            wikiModel.setPageName(normTitle.title);
            wikiModel.renderPageWithCache(null, page.getCurRev().unpackedText());
            
            // categories:
            do {
                final Set<String> pageCategories_raw = wikiModel.getCategories().keySet();
                ArrayList<NormalisedTitle> pageCategories = new ArrayList<NormalisedTitle>(pageCategories_raw.size());
                for (String cat_raw: pageCategories_raw) {
                    NormalisedTitle category = new NormalisedTitle(
                            MyNamespace.CATEGORY_NAMESPACE_KEY,
                            MyWikiModel.normaliseName(cat_raw));
                    pageCategories.add(category);
                }
                writeValues(stWriteCategories, normTitle, pageCategories);
            } while(false);
            
            // templates:
            do {
                final Set<String> pageTemplates_raw = wikiModel.getTemplatesNoMagicWords();
                ArrayList<NormalisedTitle> pageTemplates = new ArrayList<NormalisedTitle>(pageTemplates_raw.size());
                for (String tpl_raw: pageTemplates_raw) {
                    NormalisedTitle template = new NormalisedTitle(
                            MyNamespace.TEMPLATE_NAMESPACE_KEY,
                            MyWikiModel.normaliseName(tpl_raw));
                    pageTemplates.add(template);
                }
                writeValues(stWriteTemplates, normTitle, pageTemplates);
            } while (false);
            
            // includes:
            do {
                Set<String> pageIncludes = wikiModel.getIncludes();
                if (!pageIncludes.isEmpty()) {
                    // make sure, the pageIncludes set is not changed anymore (deferred processing in the thread taking place):
                    ArrayList<NormalisedTitle> normPageIncludes = new ArrayList<NormalisedTitle>(pageIncludes.size());
                    wikiModel.normalisePageTitles(pageIncludes, normPageIncludes);
                    writeValues(stWriteIncludes, normTitle, normPageIncludes);
                }
            } while (false);
            
            // redirections:
            do {
                String pageRedirLink = wikiModel.getRedirectLink();
                if (pageRedirLink != null) {
                    writeValue(stWriteRedirects, normTitle, wikiModel.normalisePageTitle(pageRedirLink));
                }
            } while(false);
            
            // links:
            do {
                Set<String> pageLinks = wikiModel.getLinks();
                if (!pageLinks.isEmpty()) {
                    // make sure, the pageIncludes set is not changed anymore (deferred processing in the thread taking place):
                    ArrayList<NormalisedTitle> normPageLinks = new ArrayList<NormalisedTitle>(pageLinks.size());
                    wikiModel.normalisePageTitles(pageLinks, normPageLinks);
                    writeValues(stWriteLinks, normTitle, normPageLinks);
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
    public static Set<NormalisedTitle> getAllChildren(Map<NormalisedTitle, Set<NormalisedTitle>> tree, NormalisedTitle root) {
        return getAllChildren(tree, new LinkedList<NormalisedTitle>(Arrays.asList(root)));
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
    public static Set<NormalisedTitle> getAllChildren(Map<NormalisedTitle, Set<NormalisedTitle>> tree, List<NormalisedTitle> roots) {
        HashSet<NormalisedTitle> allChildren = new HashSet<NormalisedTitle>(roots);
        while (!roots.isEmpty()) {
            NormalisedTitle curChild = roots.remove(0);
            Set<NormalisedTitle> subChilds = tree.get(curChild);
            if (subChilds != null) {
                // only add new children to the root list
                // (remove already processed ones)
                // -> prevents endless loops in circles
                Set<NormalisedTitle> newChilds = new HashSet<NormalisedTitle>(subChilds);
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
            Map<NormalisedTitle, Set<NormalisedTitle>> templateTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> includeTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree)
            throws RuntimeException {
        SQLiteConnection db = null;
        SQLiteStatement stmt = null;
        try {
            db = SQLiteDataHandler.openDB(dbFileName, true);
            stmt = db
                    .prepare("SELECT page.title, tpl.title FROM " +
                            "templates INNER JOIN pages AS page ON templates.title == page.id " +
                            "INNER JOIN pages AS tpl ON templates.template == tpl.id " +
                            "WHERE page.title LIKE '" + (new NormalisedTitle(MyNamespace.TEMPLATE_NAMESPACE_KEY, "")).toString() + "%';");
            while (stmt.step()) {
                NormalisedTitle pageTitle = NormalisedTitle.fromNormalised(stmt.columnString(0));
                NormalisedTitle template = NormalisedTitle.fromNormalised(stmt.columnString(1));
                updateMap(templateTree, pageTitle, template);
            }
            stmt.dispose();
            stmt = db
                    .prepare("SELECT page.title, incl.title FROM " +
                            "includes INNER JOIN pages AS page ON includes.title == page.id " +
                            "INNER JOIN pages AS incl ON includes.include == incl.id;");
            while (stmt.step()) {
                NormalisedTitle pageTitle = NormalisedTitle.fromNormalised(stmt.columnString(0));
                NormalisedTitle include = NormalisedTitle.fromNormalised(stmt.columnString(1));
                updateMap(includeTree, pageTitle, include);
            }
            stmt.dispose();
            stmt = db
                    .prepare("SELECT page.title, redir.title FROM " +
                            "redirects INNER JOIN pages AS page ON redirects.title == page.id " +
                            "INNER JOIN pages AS redir ON redirects.redirect == redir.id;");
            while (stmt.step()) {
                NormalisedTitle pageTitle = NormalisedTitle.fromNormalised(stmt.columnString(0));
                NormalisedTitle redirect = NormalisedTitle.fromNormalised(stmt.columnString(1));
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
     * @param allowedCats0
     *            include all pages in these categories (un-normalised page
     *            titles)
     * @param allowedPages0
     *            a number of pages to include, also parses these pages for more
     *            links (un-normalised page titles)
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
     * @param normalised
     *            whether the pages should be returned as normalised page titles
     *            or not
     * 
     * @return a (sorted) set of page titles
     * 
     * @throws RuntimeException
     *             if any error occurs
     */
    public static SortedSet<String> getPagesInCategories(String dbFileName,
            Set<String> allowedCats0, Set<String> allowedPages0, int depth,
            Map<NormalisedTitle, Set<NormalisedTitle>> templateTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> includeTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree,
            PrintStream msgOut, boolean normalised) throws RuntimeException {
        SQLiteConnection db = null;
        try {
            // set 1GB cache_size:
            db = SQLiteDataHandler.openDB(dbFileName, true, 1024l*1024l*1024l);
            db.exec("CREATE TEMPORARY TABLE currentPages(id INTEGER PRIMARY KEY ASC);");
            SiteInfo siteInfo = readSiteInfo(db);
            MyNamespace namespace = new MyNamespace(siteInfo);
            ArrayList<NormalisedTitle> allowedCats = new ArrayList<NormalisedTitle>(allowedCats0.size());
            MyWikiModel.normalisePageTitles(allowedCats0, namespace, allowedCats);

            Set<NormalisedTitle> allowedCatsFull = getSubCategories(allowedCats0, allowedCats, db,
                    templateTree, includeTree, referenceTree, msgOut, namespace);

            ArrayList<NormalisedTitle> allowedPages = new ArrayList<NormalisedTitle>(allowedPages0.size());
            MyWikiModel.normalisePageTitles(allowedPages0, namespace, allowedPages);

            Set<NormalisedTitle> currentPages = new HashSet<NormalisedTitle>();
            currentPages.addAll(allowedPages);
            currentPages.addAll(allowedCatsFull);
            currentPages.addAll(getPagesDirectlyInCategories(allowedCatsFull, db));

            Set<NormalisedTitle> normalisedPages = getRecursivePages(currentPages, depth, db,
                    templateTree, includeTree, referenceTree, msgOut);
            
            // no need to drop table - we set temporary tables to be in-memory only
//            db.exec("DROP TABLE currentPages;");

            // note: need to sort case-sensitively (wiki is only case-insensitive at the first char)
            final TreeSet<String> pages = new TreeSet<String>();
            if (normalised) {
                pages.addAll(ScalarisDataHandlerNormalised.normList2normStringList(normalisedPages));
            } else {
                MyWikiModel.denormalisePageTitles(normalisedPages, namespace, pages);
            }
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
     * @param allowedCats0
     *            include all pages in these categories (un-normalised page
     *            titles)
     * @param allowedCats
     *            include all pages in these categories (normalised page titles)
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
     *         (normalised)
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private static Set<NormalisedTitle> getSubCategories(Collection<? extends String> allowedCats0,
            Collection<? extends NormalisedTitle> allowedCats, SQLiteConnection db,
            Map<NormalisedTitle, Set<NormalisedTitle>> templateTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> includeTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree, PrintStream msgOut,
            MyNamespace nsObject) throws SQLiteException {
        Set<NormalisedTitle> allowedCatsFull = new HashSet<NormalisedTitle>();
        Set<NormalisedTitle> currentPages = new HashSet<NormalisedTitle>();
        Set<NormalisedTitle> newPages = new HashSet<NormalisedTitle>();
        
        if (!allowedCats.isEmpty()) {
            SQLiteStatement stmt = null;
            try {
                // need to extend the category set by all sub-categories:
                currentPages.addAll(allowedCats);

                println(msgOut, " determining sub-categories of " + allowedCats0.toString() + "");
                do {
                    stmt = db.prepare("INSERT INTO currentPages (id) SELECT pages.id FROM pages WHERE pages.title == ?;");
                    for (NormalisedTitle pageTitle : currentPages) {
                        addToPages(allowedCatsFull, newPages, pageTitle, includeTree, referenceTree);
                        // beware: add pageTitle to allowedCatsFull _AFTER_ adding its dependencies
                        // (otherwise the dependencies won't be added)
                        allowedCatsFull.add(pageTitle);
                        stmt.bind(1, pageTitle.toString()).stepThrough().reset();
                    }
                    stmt.dispose();

                    println(msgOut, "  adding sub-categories of " + currentPages.size() + " categories or templates");
                    // add all categories the page belongs to
                    stmt = db
                            .prepare("SELECT page.title FROM categories " +
                                    "INNER JOIN currentPages AS cp ON categories.category == cp.id " +
                                    "INNER JOIN pages AS page ON categories.title == page.id " +
                                    // "INNER JOIN pages AS cat ON categories.category == cat.id" +
                                    "WHERE page.title LIKE '" + MyNamespace.NamespaceEnum.CATEGORY_NAMESPACE_KEY.getId() + ":%';");
                    while (stmt.step()) {
                        NormalisedTitle pageCategory = NormalisedTitle.fromNormalised(stmt.columnString(0));
                        addToPages(allowedCatsFull, newPages, pageCategory, includeTree, referenceTree);
                    }
                    stmt.dispose();
                    println(msgOut, "  adding sub-templates or sub-categories of " + currentPages.size() + " categories or templates");
                    // add all templates (and their requirements) of the pages
                    stmt = db
                            .prepare("SELECT page.title FROM templates " +
                                    "INNER JOIN currentPages AS cp ON templates.template == cp.id " +
                                    "INNER JOIN pages AS page ON templates.title == page.id " +
                                    // "INNER JOIN pages AS tpl ON templates.template == tpl.id" +
                                    "WHERE page.title LIKE '" + MyNamespace.NamespaceEnum.CATEGORY_NAMESPACE_KEY.getId() + ":%' OR "
                                    + "page.title LIKE '" + MyNamespace.NamespaceEnum.TEMPLATE_NAMESPACE_KEY.getId() + ":%';");
                    while (stmt.step()) {
                        NormalisedTitle pageTemplate = NormalisedTitle.fromNormalised(stmt.columnString(0));
                        Set<NormalisedTitle> tplChildren = WikiDumpGetCategoryTreeHandler.getAllChildren(templateTree, pageTemplate);
                        addToPages(allowedCatsFull, newPages, tplChildren, includeTree, referenceTree);
                    }
                    stmt.dispose();
                    db.exec("DELETE FROM currentPages;");
                    if (newPages.isEmpty()) {
                        break;
                    } else {
                        println(msgOut, " adding " + newPages.size() + " dependencies");
                        currentPages = newPages;
                        newPages = new HashSet<NormalisedTitle>();
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
    private static Set<NormalisedTitle> getPagesDirectlyInCategories(Set<NormalisedTitle> allowedCats,
            SQLiteConnection db) throws SQLiteException {
        Set<NormalisedTitle> currentPages = new HashSet<NormalisedTitle>();

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
                    NormalisedTitle pageTitle = NormalisedTitle.fromNormalised(stmt.columnString(0));
                    NormalisedTitle pageCategory = NormalisedTitle.fromNormalised(stmt.columnString(1));
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
                    NormalisedTitle pageTitle = NormalisedTitle.fromNormalised(stmt.columnString(0));
                    NormalisedTitle pageTemplate = NormalisedTitle.fromNormalised(stmt.columnString(1));
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
     *            parse these pages recursively (normalised page titles)
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
     * @return a set of normalised page titles
     * 
     * @throws SQLiteException
     *             if an error occurs
     */
    private static Set<NormalisedTitle> getRecursivePages(Set<NormalisedTitle> currentPages,
            int depth, SQLiteConnection db, Map<NormalisedTitle, Set<NormalisedTitle>> templateTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> includeTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree,
            PrintStream msgOut)
            throws SQLiteException {
        Set<NormalisedTitle> allPages = new HashSet<NormalisedTitle>();
        Set<NormalisedTitle> newPages = new HashSet<NormalisedTitle>();
        Set<NormalisedTitle> pageLinks = new HashSet<NormalisedTitle>();
        SQLiteStatement stmt = null;
        final ArrayList<NormalisedTitle> autoIncluded = new ArrayList<NormalisedTitle>(10000);
        try {
            println(msgOut, "adding all mediawiki pages");
            // add all auto-included pages
            stmt = db.prepare("SELECT pages.title FROM pages WHERE pages.title LIKE '"
                              + MyNamespace.NamespaceEnum.MEDIAWIKI_NAMESPACE_KEY.getId()
                              + ":%';");
            while (stmt.step()) {
                autoIncluded.add(NormalisedTitle.fromNormalised(stmt.columnString(0)));
            }
            currentPages.addAll(autoIncluded);
            stmt.dispose();
            
            while(depth >= 0) {
                println(msgOut, "recursion level: " + depth);
                println(msgOut, " adding " + currentPages.size() + " pages");
                do {
                    stmt = db.prepare("INSERT INTO currentPages (id) SELECT pages.id FROM pages WHERE pages.title == ?;");
                    for (NormalisedTitle pageTitle : currentPages) {
                        addToPages(allPages, newPages, pageTitle, includeTree, referenceTree);
                        // beware: add pageTitle to pages _AFTER_ adding its dependencies
                        // (otherwise the dependencies won't be added)
                        allPages.add(pageTitle);
                        stmt.bind(1, pageTitle.toString()).stepThrough().reset();
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
                        NormalisedTitle pageCategory = NormalisedTitle.fromNormalised(stmt.columnString(0));
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
                        NormalisedTitle pageTemplate = NormalisedTitle.fromNormalised(stmt.columnString(0));
                        Set<NormalisedTitle> tplChildren = WikiDumpGetCategoryTreeHandler.getAllChildren(templateTree, pageTemplate);
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
                            pageLinks.add(NormalisedTitle.fromNormalised(pageLink));
                        }
                    }
                    stmt.dispose();
                    db.exec("DELETE FROM currentPages;");
                    if (newPages.isEmpty()) {
                        break;
                    } else {
                        println(msgOut, " adding " + newPages.size() + " dependencies");
                        currentPages = newPages;
                        newPages = new HashSet<NormalisedTitle>();
                    }
                } while (true);
                // for the next recursion:
                currentPages = pageLinks;
                pageLinks = new HashSet<NormalisedTitle>();
                --depth;
            }
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
        }
        // these pages will be automatically included even if not in the page
        // list -> do not include them for a smaller more readable page list
        allPages.removeAll(autoIncluded);
        return allPages;
    }
    
    static protected void addToPages(Set<NormalisedTitle> pages, Set<NormalisedTitle> newPages,
            NormalisedTitle title,
            Map<NormalisedTitle, Set<NormalisedTitle>> includeTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree) {
        if (!pages.contains(title) && newPages.add(title)) {
            // title not yet in pages -> add includes, redirects and pages redirecting to this page
            addToPages(pages, newPages,
                    WikiDumpGetCategoryTreeHandler.getAllChildren(includeTree,
                            title), includeTree, referenceTree); // also has redirects
            addToPages(pages, newPages,
                    WikiDumpGetCategoryTreeHandler.getAllChildren(
                            referenceTree, title), includeTree, referenceTree);
        }
    }
    
    static protected void addToPages(Set<NormalisedTitle> pages, Set<NormalisedTitle> newPages,
            Collection<? extends NormalisedTitle> titles,
            Map<NormalisedTitle, Set<NormalisedTitle>> includeTree,
            Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree) {
        for (NormalisedTitle title : titles) {
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
                    // set 1GB cache_size:
                    db = SQLiteDataHandler.openDB(dbFileName, false, 1024l*1024l*1024l);
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
        NormalisedTitle key;
        Collection<? extends NormalisedTitle> values;
        
        public SQLiteWriteValuesJob(SQLiteStatement stmt, NormalisedTitle key, NormalisedTitle value) {
            this.stmt = stmt;
            this.key = key;
            this.values = Arrays.asList(value);
        }
        
        public SQLiteWriteValuesJob(SQLiteStatement stmt, NormalisedTitle key, Collection<? extends NormalisedTitle> values) {
            this.stmt = stmt;
            this.key = key;
            this.values = values;
        }

        protected long pageToId(NormalisedTitle pageTitle) throws RuntimeException {
            String pageTitle2 = pageTitle.toString();
            try {
                long pageId = -1;
                // try to find the page id in the pages table:
                try {
                    stGetPageId.bind(1, pageTitle2);
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
                        stWritePages.bind(1, pageId).bind(2, pageTitle2).stepThrough();
                    } finally {
                        stWritePages.reset();
                    }
                }
                return pageId;
            } catch (SQLiteException e) {
                error("write of " + pageTitle2 + " failed (sqlite error: " + e.toString() + ")");
                throw new RuntimeException(e);
            }
        }
        
        @Override
        public void run() {
            long key_id = pageToId(key);
            ArrayList<Long> values_id = new ArrayList<Long>(values.size());
            for (NormalisedTitle value : values) {
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
                error("write of " + key + " failed (sqlite error: " + e.toString() + ")");
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
