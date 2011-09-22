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
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.data.Contributor;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * the standard output.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpToStdoutHandler extends WikiDumpHandler {

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     */
    public WikiDumpToStdoutHandler(Set<String> blacklist, Set<String> whitelist, int maxRevisions, Calendar maxTime) {
        super(blacklist, whitelist, maxRevisions, maxTime);
    }

    /**
     * dumps the given siteinfo to the standard output in
     * the following text format:
     * <code>
     * {siteinfo, "http://bar.wikipedia.org/wiki/Hauptseitn",
     *  [ {sitename, "Wikipedia"}, {generator, "MediaWiki 1.11alpha"} ]}
     * </code>
     * 
     * @param revisions
     *            the siteinfo to export
     */
    @Override
    protected void export(XmlSiteInfo siteinfo_xml) {
        SiteInfo siteinfo = siteinfo_xml.getSiteInfo();
        System.out.println("{siteinfo, \"" + siteinfo.getBase() + "\", [ {sitename, \""
                + siteinfo.getSitename() + "\"}, {generator, \"" + siteinfo.getGenerator() + "\"} ]}.");
    }

    /**
     * Dumps the given page (including all revisions) to the standard output in
     * the following text format:
     * <code>
     * {title, id, revisions, props}
     * </code>
     * 
     * @param page_xml
     *            the page object extracted from XML
     */
    @Override
    protected void export(XmlPage page_xml) {
        Page page = page_xml.getPage();
        
        if (page.getCurRev() != null) {
            String title = page.getTitle().replaceFirst("^Category:", "Kategorie:");

            try {
                System.out.print("{page, \"");
                System.out.print(escape(title));
                System.out.print("\", \"");
                System.out.print(page.getId());
                System.out.println("\", [");

                for (Iterator<Revision> iterator = page_xml.getRevisions().iterator(); iterator
                        .hasNext();) {
                    Revision revision = iterator.next();
                    export(revision);
                    if (iterator.hasNext())
                        System.out.println(",");
                }
                ++pageCount;
                msgOut.println(" " + page_xml.getRevisions().size() + " " + title);

                System.out.println("], []}.");
            } catch (Exception e) {
                msgOut.println(title);
                System.exit(0);
            }
        }
    }
    
    /**
     * dumps the current revision to the standard output in the following
     * text format:
     * <code>
     * {id, timestamp, contributor, comment, text, props}
     * </code>
     */
    protected void export(Revision rev) {
        StringBuffer categories_str = new StringBuffer(1000);
        categories_str.append('[');
        if (wikiModel != null) {
            Collection<String> categories = rev.parseCategories(wikiModel);
            Iterator<String> iter = categories.iterator();
            if (iter.hasNext()) {
                categories_str.append("\"Category:");
                categories_str.append(iter.next());
                categories_str.append('"');
            }
            while (iter.hasNext()) {
                categories_str.append(", \"Category:");
                categories_str.append(iter.next());
                categories_str.append('"');
            }
        }
        categories_str.append(']');
        
        /**
         * {revision, "1", "2006-09-30T19:57:40Z", {ip, "127.0.0.1"}, "comment", "content", [{categoryPage, []}]}
         */
        System.out.print("{revision, \"" + rev.getId() + "\", \"" + rev.getTimestamp() + "\", ");
        export(rev.getContributor());
        System.out.print(", \"");
        System.out.print(WikiDumpToStdoutHandler.escape(rev.getComment()));
        System.out.print("\", \"");
        System.out.print(WikiDumpToStdoutHandler.escape(rev.getText()));
        System.out.print("\", [ {categoryPage, ");
        System.out.print(categories_str.toString());
        System.out.println("} ]}");
    }

    /**
     * dumps the given contributor to the standard output in the following text
     * format:
     * <code>
     * {ip, "<IP>"}
     * </code> or
     * <code>
     * {user, {id, user}}
     * </code>
     * depending on which contributor has been provided.
     * 
     * @param contributor
     *            the contributor to export
     */
    protected void export(Contributor contributor) {
        if (contributor.getIp().equals("")) {
            System.out.print("{user, {" + contributor.getId() + ", \""
                    + WikiDumpToStdoutHandler.escape(contributor.getUser()) + "\"}}");
        } else {
            System.out.print("{ip, \"" + contributor.getIp() + "\"}");
        }
    }

    private static String escape(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }
}
