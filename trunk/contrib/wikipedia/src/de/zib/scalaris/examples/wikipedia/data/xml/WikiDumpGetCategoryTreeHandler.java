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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Page;

/**
 * Provides abilities to read an xml wiki dump file and create a category (and
 * template) tree.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpGetCategoryTreeHandler extends WikiDumpHandler {
    private static final int PRINT_PAGES_EVERY = 400;
    Map<String, Set<String>> categories = new HashMap<String, Set<String>>();
    Map<String, Set<String>> templates = new HashMap<String, Set<String>>();
    Map<String, Set<String>> includes = new HashMap<String, Set<String>>();
    Map<String, Set<String>> references = new HashMap<String, Set<String>>();

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
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpGetCategoryTreeHandler(Set<String> blacklist,
            Calendar maxTime) throws RuntimeException {
        super(blacklist, null, 1, maxTime);
    }
    
    private static void updateMap(Map<String, Set<String>> map, String key, String addToValue) {
        Set<String> oldValue = map.get(key);
        if (oldValue == null) {
            oldValue = new HashSet<String>();
            map.put(key, oldValue);
        }
        oldValue.add(addToValue);
    }
    
    private static void updateMap(Map<String, Set<String>> map, String key, Collection<? extends String> addToValues) {
        Set<String> oldValue = map.get(key);
        if (oldValue == null) {
            oldValue = new HashSet<String>(addToValues);
            map.put(key, oldValue);
        } else {
            oldValue.addAll(addToValues);
        }
    }
    
    private void updateSubCats(String category, String newSubCat) {
        updateMap(categories, category, newSubCat);
    }
    
    private void updateTplReqs(String template, Collection<? extends String> requiredTpls) {
        updateMap(templates, template, requiredTpls);
    }
    
    private void updateIncludes(String pageTitle, Collection<? extends String> includedPages) {
        updateMap(includes, pageTitle, includedPages);
    }
    
    private void updateReferences(String redirectTo, String referringPage) {
        updateMap(references, redirectTo, referringPage);
    }

    /**
     * Exports the given siteinfo (nothing to do here).
     * 
     * @param revisions
     *            the siteinfo to export
     */
    @Override
    protected void export(XmlSiteInfo siteinfo_xml) {
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
            wikiModel.setPageName(page.getTitle());
            wikiModel.render(null, page.getCurRev().getText());
            final String namespace = MyWikiModel.getNamespace(page.getTitle());
            final boolean isCategory = wikiModel.isCategoryNamespace(namespace);
            final boolean isTemplate = wikiModel.isTemplateNamespace(namespace);
            if (isCategory) {
                for (String cat_raw: wikiModel.getCategories().keySet()) {
                    String category = (wikiModel.getCategoryNamespace() + ":" + cat_raw).intern();
                    updateSubCats(category, page.getTitle());
                }
            }
            if (isCategory || isTemplate) {
                Set<String> pageTemplates_raw = wikiModel.getTemplates();
                ArrayList<String> pageTemplates = new ArrayList<String>(pageTemplates_raw.size());
                for (String tpl_raw: pageTemplates_raw) {
                    String template = (wikiModel.getTemplateNamespace() + ":" + tpl_raw).intern();
                    updateSubCats(template, page.getTitle());
                    pageTemplates.add(template);
                }
                // also need the dependencies of each template:
                if (isTemplate) {
                    updateTplReqs(page.getTitle(), pageTemplates);
                }
            }
            Set<String> pageIncludes = wikiModel.getIncludes();
            if (!pageIncludes.isEmpty()) {
                updateIncludes(page.getTitle(), pageIncludes);
            }
            String pageRedirLink = wikiModel.getRedirectLink();
            if (pageRedirLink != null) {
                updateReferences(pageRedirLink, page.getTitle());
            }
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
        importEnd();
    }

    /**
     * @return the categories
     */
    public Map<String, Set<String>> getCategories() {
        return categories;
    }

    /**
     * @return the categories
     */
    public Map<String, Set<String>> getTemplates() {
        return templates;
    }
    
    /**
     * @return the includes
     */
    public Map<String, Set<String>> getIncludes() {
        return includes;
    }

    /**
     * @return the references
     */
    public Map<String, Set<String>> getReferences() {
        return references;
    }

    /**
     * Gets all sub categories that belong to a given root category
     * (recursively).
     * 
     * @param tree
     *            the tree of categories or templates as returned by
     *            {@link #getCategories()} or {@link #getTemplates()}
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
     *            the tree of categories or templates as returned by
     *            {@link #getCategories()} or {@link #getTemplates()}
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
}
