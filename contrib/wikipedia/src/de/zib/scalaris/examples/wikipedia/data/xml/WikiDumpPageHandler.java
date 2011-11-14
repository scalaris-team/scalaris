package de.zib.scalaris.examples.wikipedia.data.xml;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Intermediate class between XML processing of a Wiki dump and custom
 * exporting of read {@link SiteInfo} and {@link Page} objects, including all
 * revisions and the list of short revisions.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class WikiDumpPageHandler extends WikiDumpHandler {
    protected static final int UPDATE_PAGELIST_EVERY = 400;
    protected static final int NEW_CATS_HASH_DEF_SIZE = 100;
    protected static final int NEW_TPLS_HASH_DEF_SIZE = 100;
    protected static final int NEW_BLNKS_HASH_DEF_SIZE = 100;
    
    protected List<String> newPages = new LinkedList<String>();
    protected List<String> newArticles = new LinkedList<String>();
    protected HashMap<String, List<String>> newCategories = new HashMap<String, List<String>>(NEW_CATS_HASH_DEF_SIZE);
    protected HashMap<String, List<String>> newTemplates = new HashMap<String, List<String>>(NEW_TPLS_HASH_DEF_SIZE);
    protected HashMap<String, List<String>> newBackLinks = new HashMap<String, List<String>>(NEW_BLNKS_HASH_DEF_SIZE);

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param minTime
     *            minimum time a revision should have (only one revision older
     *            than this will be imported) - <tt>null/tt> imports all
     *            revisions
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     */
    public WikiDumpPageHandler(Set<String> blacklist, Set<String> whitelist,
            int maxRevisions, Calendar minTime, Calendar maxTime) {
        super(blacklist, whitelist, maxRevisions, minTime, maxTime);
    }

    /**
     * Exports the given siteinfo to Scalaris
     * 
     * @param revisions
     *            the siteinfo to export
     */
    @Override
    protected void export(XmlSiteInfo siteinfo_xml) {
        doExport(siteinfo_xml.getSiteInfo());
    }

    /**
     * Exports the given page (including all revisions) to Scalaris
     * 
     * @param page_xml
     *            the page object extracted from XML
     */
    @Override
    protected void export(XmlPage page_xml) {
        Page page = page_xml.getPage();
        ++pageCount;
        
        if (page.getCurRev() != null) {
            List<Revision> revisions = page_xml.getRevisions();
            List<ShortRevision> revisions_short = ShortRevision.fromRevisions(revisions);
            Collections.sort(revisions, Collections.reverseOrder(new byRevId()));
            Collections.sort(revisions_short, Collections.reverseOrder(new byShortRevId()));
    
            if (!revisions.isEmpty() && wikiModel != null) {
                wikiModel.setUp();
                wikiModel.setPageName(page.getTitle());
                wikiModel.render(null, revisions.get(0).unpackedText());
                for (String cat_raw: wikiModel.getCategories().keySet()) {
                    String category = wikiModel.getCategoryNamespace() + ":" + cat_raw;
                    List<String> catPages = newCategories.get(category);
                    if (catPages == null) {
                        catPages = new ArrayList<String>(UPDATE_PAGELIST_EVERY / 4);
                    }
                    catPages.add(wikiModel.normalisePageTitle(page.getTitle()));
                    newCategories.put(category, catPages);
                }
                for (String tpl_raw: wikiModel.getTemplates()) {
                    String template = wikiModel.getTemplateNamespace() + ":" + tpl_raw;
                    List<String> templatePages = newTemplates.get(template);
                    if (templatePages == null) {
                        templatePages = new ArrayList<String>(UPDATE_PAGELIST_EVERY / 4);
                    }
                    templatePages.add(wikiModel.normalisePageTitle(page.getTitle()));
                    newTemplates.put(template, templatePages);
                }
                for (String link: wikiModel.getLinks()) {
                    List<String> backLinks = newBackLinks.get(link);
                    if (backLinks == null) {
                        backLinks = new ArrayList<String>(UPDATE_PAGELIST_EVERY / 4);
                    }
                    backLinks.add(wikiModel.normalisePageTitle(page.getTitle()));
                    newBackLinks.put(link, backLinks);
                }
                wikiModel.tearDown();
            }
    
            doExport(page, revisions, revisions_short);
        }
        if ((pageCount % UPDATE_PAGELIST_EVERY) == 0) {
            println("processed pages: " + pageCount);
        }
    }

    abstract protected void doExport(SiteInfo siteInfo);

    abstract protected void doExport(Page page, List<Revision> revisions,
            List<ShortRevision> revisions_short);

    /**
     * Provides a comparator for sorting {@link Revision} objects by their IDs.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    protected static class byRevId implements java.util.Comparator<Revision> {
        /**
         * Compares its two arguments for order. Returns a negative integer,
         * zero, or a positive integer as the first argument is less than, equal
         * to, or greater than the second.
         * 
         * @param rev1
         *            the first revision to be compared.
         * @param rev2
         *            the second revision to be compared.
         * 
         * @return a negative integer, zero, or a positive integer as the first
         *         argument is less than, equal to, or greater than the second.
         */
        @Override
        public int compare(Revision rev1, Revision rev2) {
            return (rev1.getId() - rev2.getId());
        }
    }

    /**
     * Provides a comparator for sorting {@link Revision} objects by their IDs.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    protected static class byShortRevId implements java.util.Comparator<ShortRevision> {
        /**
         * Compares its two arguments for order. Returns a negative integer,
         * zero, or a positive integer as the first argument is less than, equal
         * to, or greater than the second.
         * 
         * @param rev1
         *            the first revision to be compared.
         * @param rev2
         *            the second revision to be compared.
         * 
         * @return a negative integer, zero, or a positive integer as the first
         *         argument is less than, equal to, or greater than the second.
         */
        @Override
        public int compare(ShortRevision rev1, ShortRevision rev2) {
            return (rev1.getId() - rev2.getId());
        }
    }
}