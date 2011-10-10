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
    protected SQLiteStatement stWriteCategories = null;
    protected SQLiteStatement stWriteTemplates = null;
    protected SQLiteStatement stWriteIncludes = null;
    protected SQLiteStatement stWriteRedirects = null;
    protected SQLiteStatement stWriteLinks = null;
    
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
        return db.prepare("SELECT category FROM categories WHERE title == ?");
    }

    static SQLiteStatement createReadTemplatesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT template FROM templates WHERE title == ?");
    }

    static SQLiteStatement createReadIncludesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT include FROM includes WHERE title == ?");
    }

    static SQLiteStatement createReadRedirectsStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT redirect FROM redirects WHERE title == ?");
    }

    static SQLiteStatement createReadLinksStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("SELECT link FROM links WHERE title == ?");
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

    static SQLiteStatement createWriteCategoriesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("REPLACE INTO categories (title, category) VALUES (?, ?);");
    }

    static SQLiteStatement createWriteTemplatesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("REPLACE INTO templates (title, template) VALUES (?, ?);");
    }

    static SQLiteStatement createWriteIncludesStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("REPLACE INTO includes (title, include) VALUES (?, ?);");
    }

    static SQLiteStatement createWriteRedirectsStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("REPLACE INTO redirects (title, redirect) VALUES (?, ?);");
    }

    static SQLiteStatement createWriteLinksStmt(SQLiteConnection db) throws SQLiteException {
        return db.prepare("REPLACE INTO links (title, link) VALUES (?, ?);");
    }

    static void writeValue(SQLiteStatement stmt, String key, String value)
            throws RuntimeException {
        writeValues(stmt, key, Arrays.asList(value));
    }

    static void writeValues(SQLiteStatement stmt, String key, Collection<? extends String> values)
            throws RuntimeException {
        try {
            try {
                stmt.bind(1, key);
                for (String value : values) {
                    stmt.bind(2, value).stepThrough().reset(false);
                }
            } finally {
                stmt.reset();
            }
        } catch (SQLiteException e) {
            System.err.println("write of " + key + " failed (sqlite error: " + e.toString() + ")");
        }
    }

    static void writeSiteInfo(SQLiteConnection db, SiteInfo siteInfo)
            throws RuntimeException {
        SQLiteStatement stmt = null;
        try {
            stmt = db.prepare("REPLACE INTO siteinfo (id, siteinfo) VALUES (?, ?);");
            WikiDumpPrepareSQLiteForScalarisHandler.writeObject(stmt, "1", siteInfo);
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
        } finally {
            if (stmt != null) {
                stmt.dispose();
            }
        }
    }

    static SiteInfo readSiteInfo(SQLiteConnection db) throws RuntimeException {
        SQLiteStatement stmt = null;
        try {
            stmt = db.prepare("SELECT siteinfo FROM siteinfo WHERE id == ?");
            return WikiDumpPrepareSQLiteForScalarisHandler.readObject(stmt, "1");
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
        writeSiteInfo(db, siteinfo_xml.getSiteInfo());
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
                    String category = (wikiModel.getCategoryNamespace() + ":" + cat_raw).intern();
                    pageCategories.add(category);
                }
                writeValues(stWriteCategories, pageTitle, pageCategories);
            } while(false);
            
            // templates:
            do {
                final Set<String> pageTemplates_raw = wikiModel.getTemplates();
                ArrayList<String> pageTemplates = new ArrayList<String>(pageTemplates_raw.size());
                for (String tpl_raw: pageTemplates_raw) {
                    String template = (wikiModel.getTemplateNamespace() + ":" + tpl_raw).intern();
                    pageTemplates.add(template);
                }
                writeValues(stWriteTemplates, pageTitle, pageTemplates);
            } while (false);
            
            // includes:
            do {
                Set<String> pageIncludes = wikiModel.getIncludes();
                if (!pageIncludes.isEmpty()) {
                    writeValues(stWriteIncludes, pageTitle, pageIncludes);
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
                    writeValues(stWriteLinks, pageTitle, pageLinks);
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

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        super.tearDown();
        if (db != null) {
            db.dispose();
        }
        importEnd();
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
                // only add new categories to the root categories
                // (remove already processed ones)
                // -> prevents endless loops in circles
                Set<String> newCats = new HashSet<String>(subChilds);
                newCats.removeAll(allChildren);
                allChildren.addAll(subChilds);
                roots.addAll(newCats);
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

        try {
            db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName);
            db.exec("CREATE TABLE categories(title STRING, category STRING);");
            db.exec("CREATE INDEX cat_titles ON categories(title);");
            db.exec("CREATE TABLE templates(title STRING, template STRING);");
            db.exec("CREATE INDEX tpl_titles ON templates(title);");
            db.exec("CREATE TABLE includes(title STRING, include STRING);");
            db.exec("CREATE INDEX incl_titles ON includes(title);");
            db.exec("CREATE TABLE redirects(title STRING, redirect STRING);");
            db.exec("CREATE INDEX redir_titles ON redirects(title);");
            db.exec("CREATE TABLE links(title STRING, link STRING);");
            db.exec("CREATE INDEX lnk_titles ON links(title);");
            db.exec("CREATE TABLE siteinfo(id STRING PRIMARY KEY ASC, siteinfo);");
            stWriteCategories = createWriteCategoriesStmt(db);
            stWriteTemplates = createWriteTemplatesStmt(db);
            stWriteIncludes = createWriteIncludesStmt(db);
            stWriteRedirects = createWriteRedirectsStmt(db);
            stWriteLinks = createWriteLinksStmt(db);
        } catch (SQLiteException e) {
            throw new RuntimeException(e);
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
            db = WikiDumpPrepareSQLiteForScalarisHandler.openDB(dbFileName);
            SiteInfo siteInfo = readSiteInfo(db);
            MyParsingWikiModel wikiModel = new MyParsingWikiModel("", "", new MyNamespace(siteInfo));
            stmt = db
                    .prepare("SELECT title, category FROM categories WHERE "
                            + "title LIKE '" + wikiModel.getCategoryNamespace() + ":%'");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0).intern();
                String category = stmt.columnString(1).intern();
                updateMap(categoryTree, category, pageTitle);
            }
            stmt.dispose();
            stmt = db
                    .prepare("SELECT title, template FROM templates WHERE "
                            + "title LIKE '" + wikiModel.getCategoryNamespace() + ":%' OR "
                            + "title LIKE '" + wikiModel.getTemplateNamespace() + ":%'");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0).intern();
                String template = stmt.columnString(1).intern();
                final String namespace = MyWikiModel.getNamespace(pageTitle);
                updateMap(categoryTree, template, pageTitle);
                if (wikiModel.isTemplateNamespace(namespace)) {
                    updateMap(templateTree, pageTitle, template);
                }
            }
            stmt.dispose();
            stmt = db.prepare("SELECT title, include FROM includes;");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0).intern();
                String include = stmt.columnString(1).intern();
                updateMap(includeTree, pageTitle, include);
            }
            stmt.dispose();
            stmt = db.prepare("SELECT title, redirect FROM redirects;");
            while (stmt.step()) {
                String pageTitle = stmt.columnString(0).intern();
                String redirect = stmt.columnString(1).intern();
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
}
