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
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.xml.XmlPage.CheckSkipRevisions;

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
        // since we do not use a whitelist, we can safely overwrite the skip
        // revision handler:
        // (otherwise we should also filter using the whitelist)
        setPageCheckSkipRevisions(new CheckSkipRevisions() {
            @Override
            public boolean skipRevisions(String pageTitle) {
                return !isTemplateOrCategory(pageTitle);
            }
        });
    }
    
    private boolean isTemplateOrCategory(String pageTitle) {
        return wikiModel.isCategoryNamespace(MyWikiModel.getNamespace(pageTitle)) ||
               wikiModel.isTemplateNamespace(MyWikiModel.getNamespace(pageTitle));
    }
    
    private void updateSubCats(String category, String newSubCat) {
        Set<String> subCats = categories.get(category);
        if (subCats == null) {
            subCats = new HashSet<String>();
            categories.put(category, subCats);
        }
        subCats.add(newSubCat);
    }
    
    private void updateTplReqs(String template, Collection<? extends String> requiredTpls) {
        Set<String> subTpls = templates.get(template);
        if (subTpls == null) {
            subTpls = new HashSet<String>(requiredTpls);
            templates.put(template, subTpls);
        } else {
            subTpls.addAll(requiredTpls);
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

        if (page.getCurRev() != null && wikiModel != null &&
                isTemplateOrCategory(page.getTitle())) {
            wikiModel.render(null, page.getCurRev().getText());
            for (String cat_raw: wikiModel.getCategories().keySet()) {
                String category = wikiModel.getCategoryNamespace() + ":" + cat_raw;
                updateSubCats(category, page.getTitle());
            }
            Set<String> pageTemplates_raw = wikiModel.getTemplates();
            ArrayList<String> pageTemplates = new ArrayList<String>(pageTemplates_raw.size());
            for (String tpl_raw: pageTemplates_raw) {
                String template = wikiModel.getTemplateNamespace() + ":" + tpl_raw;
                updateSubCats(template, page.getTitle());
                pageTemplates.add(template);
            }
            // also need the dependencies of each template:
            if (wikiModel.isTemplateNamespace(MyWikiModel.getNamespace(page.getTitle()))) {
                updateTplReqs(page.getTitle(), pageTemplates);
            }
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
        return getAllChildren(tree, Arrays.asList(root));
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
