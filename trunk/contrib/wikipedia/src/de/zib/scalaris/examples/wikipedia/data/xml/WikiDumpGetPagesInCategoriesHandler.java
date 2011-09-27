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

import java.util.Calendar;
import java.util.HashSet;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.data.Page;

/**
 * Provides abilities to read an xml wiki dump file and extract page titles
 * in categories and white lists as well as links from those pages.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpGetPagesInCategoriesHandler extends WikiDumpHandler {
    private static final int PRINT_PAGES_EVERY = 400;
    Set<String> pages = new HashSet<String>();
    Set<String> linksOnPages = new HashSet<String>();
    Set<String> whitelist;
    Set<String> categories;

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
     * @param categories
     *            include all pages in there categories
     * @param whitelist
     *            a number of pages to include (parses these pages for more
     *            links)
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpGetPagesInCategoriesHandler(Set<String> blacklist,
            Calendar maxTime, Set<String> categories, Set<String> whitelist) throws RuntimeException {
        super(blacklist, null, 1, maxTime);
        this.categories = categories;
        this.whitelist = whitelist;
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
            wikiModel.render(null, page.getCurRev().getText());
            
            boolean pageInAllowedCat = false;
            Set<String> pageCategories = new HashSet<String>();
            for (String cat_raw: wikiModel.getCategories().keySet()) {
                String category = wikiModel.getCategoryNamespace() + ":" + cat_raw;
                pageCategories.add(category);
                if (categories.contains(category)) {
                    pageInAllowedCat = true;
                    break;
                }
            }

            boolean pageInAllowedTpl = false;
            Set<String> pageTemplates= new HashSet<String>();
            for (String tpl_raw: wikiModel.getTemplates()) {
                String template = wikiModel.getTemplateNamespace() + ":" + tpl_raw;
                pageTemplates.add(template);
                if (categories.contains(template)) {
                    pageInAllowedTpl = true;
                    break;
                }
            }
            
            if (whitelist.contains(page.getTitle()) || pageInAllowedCat || pageInAllowedTpl) {
                pages.add(page.getTitle());
                pages.addAll(pageCategories);
                pages.addAll(pageTemplates);
                String redirLink = wikiModel.getRedirectLink();
                if (redirLink != null) {
                    pages.add(redirLink);
                }
                Set<String> pageLinks = wikiModel.getLinks();
                pageLinks.remove(""); // there may be empty links
                linksOnPages.addAll(pageLinks);
            }
        }
        ++pageCount;
        // only export page list every PRINT_PAGES_EVERY pages:
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
