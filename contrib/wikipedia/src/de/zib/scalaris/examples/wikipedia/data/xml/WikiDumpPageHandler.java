package de.zib.scalaris.examples.wikipedia.data.xml;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.tools.MultiHashMap;

/**
 * Intermediate class between XML processing of a Wiki dump and custom
 * exporting of read {@link SiteInfo} and {@link Page} objects, including all
 * revisions and the list of short revisions.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class WikiDumpPageHandler extends WikiDumpHandler {
    protected static final int UPDATE_PAGELIST_EVERY = 500;
    protected static final int NEW_CATS_HASH_DEF_SIZE = 100;
    protected static final int NEW_TPLS_HASH_DEF_SIZE = 100;
    protected static final int NEW_BLNKS_HASH_DEF_SIZE = 100;

    protected EnumMap<NamespaceEnum, ArrayList<NormalisedTitle>> newPages;
    protected int articleCount = 0;
    protected MultiHashMap<NormalisedTitle, NormalisedTitle> newCategories;
    protected MultiHashMap<NormalisedTitle, NormalisedTitle> newTemplates;
    protected MultiHashMap<NormalisedTitle, NormalisedTitle> newBackLinks;

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
        initNewPagesList();
        initLinkLists();
    }

    /**
     * Initialises the {@link #newPages} member.
     */
    protected void initNewPagesList() {
        newPages = createNewPagesList();
    }

    /**
     * Creates a new EnumMap similar to the {@link #newPages} member.
     * 
     * @return a map with wiki namespace keys and lists of strings as values
     */
    public static EnumMap<NamespaceEnum, ArrayList<NormalisedTitle>> createNewPagesList() {
        EnumMap<NamespaceEnum, ArrayList<NormalisedTitle>> result =
                new EnumMap<NamespaceEnum, ArrayList<NormalisedTitle>>(NamespaceEnum.class);
        for(NamespaceEnum ns : NamespaceEnum.values()) {
            result.put(ns, new ArrayList<NormalisedTitle>(UPDATE_PAGELIST_EVERY));
        }
        return result;
    }

    /**
     * Initialises the {@link #newCategories}, {@link #newTemplates} and
     * {@link #newBackLinks} members.
     */
    protected void initLinkLists() {
        newCategories = new MultiHashMap<NormalisedTitle, NormalisedTitle>(NEW_CATS_HASH_DEF_SIZE);
        newTemplates = new MultiHashMap<NormalisedTitle, NormalisedTitle>(NEW_TPLS_HASH_DEF_SIZE);
        newBackLinks = new MultiHashMap<NormalisedTitle, NormalisedTitle>(NEW_BLNKS_HASH_DEF_SIZE);
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

            assert(wikiModel != null);
            final NormalisedTitle normTitle = wikiModel.normalisePageTitle(page.getTitle());
            if (!revisions.isEmpty()) {
                wikiModel.setUp();
                wikiModel.setNamespaceName(wikiModel.getNamespace().getNamespaceByNumber(normTitle.namespace));
                wikiModel.setPageName(normTitle.title);
                wikiModel.renderPageWithCache(null, revisions.get(0).unpackedText());
                for (String cat_raw: wikiModel.getCategories().keySet()) {
                    NormalisedTitle category = new NormalisedTitle(
                            MyNamespace.CATEGORY_NAMESPACE_KEY,
                            MyWikiModel.normaliseName(cat_raw));
                    newCategories.put1(category, normTitle);
                }
                for (String tpl_raw: wikiModel.getTemplatesNoMagicWords()) {
                    NormalisedTitle template = new NormalisedTitle(
                            MyNamespace.TEMPLATE_NAMESPACE_KEY,
                            MyWikiModel.normaliseName(tpl_raw));
                    newTemplates.put1(template, normTitle);
                }
                for (String link: wikiModel.getLinks()) {
                    newBackLinks.put1(wikiModel.normalisePageTitle(link),
                            normTitle);
                }
                if (MyWikiModel.isArticle(normTitle.namespace, wikiModel
                        .getLinks(), wikiModel.getCategories().keySet())) {
                    ++articleCount;
                }
                wikiModel.tearDown();
            }
    
            doExport(page, revisions, revisions_short, normTitle);
        }
        if ((pageCount % UPDATE_PAGELIST_EVERY) == 0) {
            println("processed pages: " + pageCount);
        }
    }

    abstract protected void doExport(SiteInfo siteInfo);

    abstract protected void doExport(Page page, List<Revision> revisions,
            List<ShortRevision> revisions_short, NormalisedTitle title);

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