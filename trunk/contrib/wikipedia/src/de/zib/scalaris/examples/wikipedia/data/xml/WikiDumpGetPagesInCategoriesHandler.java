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
import java.util.Calendar;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.xml.XmlPage.CheckSkipRevisions;

/**
 * Provides abilities to read an xml wiki dump file and extract page titles
 * in categories and white lists as well as links from those pages.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpGetPagesInCategoriesHandler extends WikiDumpHandler {
    private static final int PRINT_PAGES_EVERY = 400;
    protected Set<String> pages = new HashSet<String>();
    protected Set<String> linksOnPages = new HashSet<String>();
    /**
     * A number of pages to include (parses these pages for more links).
     * Note: this is different from {@link WikiDumpHandler#allowedPages} as it
     * this still allows pages not in this list to be parsed.
     */
    protected final Set<String> allowedPages;
    protected final Set<String> allowedCats;
    protected Map<String, Set<String>> categoryTree;
    protected Map<String, Set<String>> templateTree;
    protected Map<String, Set<String>> includeTree;
    protected Map<String, Set<String>> referenceTree;

    /**
     * Sets up a SAX XmlHandler exporting all page titles in the given
     * categories or the white list.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param categoryTree
     *            information about the categories and their dependencies
     * @param templateTree
     *            information about the templates and their dependencies
     * @param includeTree
     *            information about page includes
     * @param referenceTree
     *            information about references to a page
     * @param allowedCats
     *            include all pages in these categories
     * @param allowedPages
     *            a number of pages to include (also parses these pages for more
     *            links)
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpGetPagesInCategoriesHandler(Set<String> blacklist,
            Calendar maxTime, Map<String, Set<String>> categoryTree,
            Map<String, Set<String>> templateTree,
            Map<String, Set<String>> includeTree,
            Map<String, Set<String>> referenceTree,
            Set<String> allowedCats, Set<String> allowedPages) throws RuntimeException {
        super(blacklist, null, 1, maxTime);
        this.allowedCats = allowedCats;
        this.allowedPages = allowedPages;
        this.categoryTree = categoryTree;
        this.templateTree = templateTree;
        this.includeTree = includeTree;
        this.referenceTree = referenceTree;
        // we do not need to parse any other pages if the allowedCats set is empty!
        if (allowedCats.isEmpty()) {
            setPageCheckSkipRevisions(new CheckSkipRevisions() {
                @Override
                public boolean skipRevisions(String pageTitle) {
                    return !WikiDumpGetPagesInCategoriesHandler.this.allowedPages
                            .contains(pageTitle);
                }
            });
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
     * Retrieves all page titles in allowed categories or in the white list.
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
            
            boolean pageInAllowedCat = false;
            Set<String> pageCategories_raw = wikiModel.getCategories().keySet();
            ArrayList<String> pageCategories = new ArrayList<String>(pageCategories_raw.size());
            for (String cat_raw: pageCategories_raw) {
                String category = (wikiModel.getCategoryNamespace() + ":" + cat_raw).intern();
                pageCategories.add(category);
                if (!pageInAllowedCat && allowedCats.contains(category)) {
//                    System.out.println("page " + page.getTitle() + " in category " + category);
                    pageInAllowedCat = true;
                }
            }
            
            boolean pageInAllowedTpl = false;
            Set<String> pageTemplates_raw = wikiModel.getTemplates();
            ArrayList<String> pageTemplates = new ArrayList<String>(pageTemplates_raw.size());
            for (String tpl_raw: pageTemplates_raw) {
                String template = (wikiModel.getTemplateNamespace() + ":" + tpl_raw).intern();
                pageTemplates.add(template);
                if (!pageInAllowedTpl && allowedCats.contains(template)) {
//                    System.out.println("page " + page.getTitle() + " uses template " + template);
                    pageInAllowedTpl = true;
                }
            }
            
            if (allowedPages.contains(page.getTitle()) || pageInAllowedCat || pageInAllowedTpl) {
                addToPages(page.getTitle());
                System.out.println("added: " + page.getTitle());
                // add only new categories to the pages:
                // note: no need to include sub-categories
                // note: parent categories are not included
                addToPages(pageCategories);
                // add templates and their requirements:
                Set<String> tplChildren = WikiDumpGetCategoryTreeHandler.getAllChildren(templateTree, pageTemplates);
                addToPages(tplChildren);
                // add links for further processing
                Set<String> pageLinks = wikiModel.getLinks();
                pageLinks.remove(""); // there may be empty links
                for (String pageLink : pageLinks) {
                    linksOnPages.add(pageLink.intern());
                }
            }
            wikiModel.tearDown();
        }
        ++pageCount;
        // only export page list every PRINT_PAGES_EVERY pages:
        if ((pageCount % PRINT_PAGES_EVERY) == 0) {
            msgOut.println("processed pages: " + pageCount);
        }
    }
    
    protected void addToPages(String title) {
        if (pages.add(title)) {
            // title not yet in pages -> add includes, redirects and pages redirecting to this page
            addToPages(WikiDumpGetCategoryTreeHandler.getAllChildren(includeTree, title)); // also has redirects
            addToPages(WikiDumpGetCategoryTreeHandler.getAllChildren(referenceTree, title));
        }
    }
    
    protected void addToPages(Collection<? extends String> titles) {
        for (String title : titles) {
            addToPages(title);
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
     * @return the pages
     */
    public Set<String> getPages() {
        return pages;
    }

    /**
     * @return the linksOnPages
     */
    public Set<String> getLinksOnPages() {
        return linksOnPages;
    }
}
