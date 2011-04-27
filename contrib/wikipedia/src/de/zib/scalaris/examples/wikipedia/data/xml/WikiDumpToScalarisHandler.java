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
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;

import de.zib.scalaris.AbortException;
import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionException;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.TimeoutException;
import de.zib.scalaris.Transaction;
import de.zib.scalaris.Transaction.RequestList;
import de.zib.scalaris.Transaction.ResultList;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.UnknownException;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.ShortRevision;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * the standard output.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpToScalarisHandler extends WikiDumpHandler {
    private Connection connection;
    private TransactionSingleOp scalaris_single;
    private Transaction scalaris_tx;
    private List<String> pages = new LinkedList<String>();
    private int maxRevisions = -1; // all revisions by default
    private HashMap<String, List<String>> categories = new HashMap<String, List<String>>(100);
    private HashMap<String, List<String>> templates = new HashMap<String, List<String>>(100);

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpToScalarisHandler(Set<String> blacklist, int maxRevisions) throws RuntimeException {
        super(blacklist);
        this.maxRevisions = maxRevisions;
        try {
            connection = ConnectionFactory.getInstance().createConnection(
                    "wiki_import", true);
            scalaris_single = new TransactionSingleOp(connection);
            scalaris_tx = new Transaction(connection);
        } catch (ConnectionException e) {
            System.err.println("Connection to Scalaris failed");
            throw new RuntimeException(e);
        }
    }

    /**
     * Exports the given siteinfo to Scalaris
     * 
     * @param revisions
     *            the siteinfo to export
     */
    @Override
    protected void export(XmlSiteInfo siteinfo_xml) {
        String key = ScalarisDataHandler.getSiteInfoKey();
        try {
            scalaris_single.write(key, siteinfo_xml.getSiteInfo());
        } catch (ConnectionException e) {
            System.err.println("write of " + key + " failed with connection error");
        } catch (TimeoutException e) {
            System.err.println("write of " + key + " failed with timeout");
        } catch (AbortException e) {
            System.err.println("write of " + key + " failed with abort");
        } catch (UnknownException e) {
            System.err.println("write of " + key + " failed with unknown");
        }
    }

    /**
     * Provides a comparator for sorting {@link Revision} objects by their IDs.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    private static class byRevId implements java.util.Comparator<Revision> {
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
    private static class byShortRevId implements java.util.Comparator<ShortRevision> {
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
    
    /**
     * dumps the given page (including all revisions) to the standard output in
     * the following text format:
     * <code>
     * {title, id, revisions, props}
     * </code>
     * 
     * @param revision
     *            the page to export
     */
    @Override
    protected void export(XmlPage page_xml) {
        Page page = page_xml.getPage();
//      String title = page.getTitle().replaceFirst("^Category:", "Kategorie:");

        List<Revision> revisions = page_xml.getRevisions();
        List<ShortRevision> revisions_short = page_xml.getRevisions_short();
        Collections.sort(revisions, Collections.reverseOrder(new byRevId()));
        Collections.sort(revisions_short, Collections.reverseOrder(new byShortRevId()));
        if (maxRevisions > 0) {
            // cut off some revisions - only keep the newest ones:
            revisions = revisions.subList(0, Math.min(revisions.size(), maxRevisions));
            revisions_short = revisions_short.subList(0, Math.min(revisions_short.size(), maxRevisions));            
        }
        
        try {
            if (!revisions.isEmpty() && wikiModel != null) {
                for (String cat_raw: revisions.get(0).parseCategories(wikiModel)) {
                    String category = wikiModel.getCategoryNamespace() + ":" + cat_raw;
                    List<String> catPages = categories.get(category);
                    if (catPages == null) {
                        catPages = new ArrayList<String>(100);
                    }
                    catPages.add(page.getTitle());
                    categories.put(category, catPages);
                }
                for (String tpl_raw: revisions.get(0).parseTemplates(wikiModel)) {
                    String template = wikiModel.getTemplateNamespace() + ":" + tpl_raw;
                    List<String> templatePages = templates.get(template);
                    if (templatePages == null) {
                        templatePages = new ArrayList<String>(100);
                    }
                    templatePages.add(page.getTitle());
                    templates.put(template, templatePages);
                }
            }
            
            // do not make the translog too full -> write revisions beforehand,
            // ignore the (rest of the) page if a failure occured
            for (Revision rev : revisions) {
                String key = ScalarisDataHandler.getRevKey(page.getTitle(), rev.getId());
                scalaris_single.write(key, rev);
            }
            ResultList result = scalaris_tx.req_list(new RequestList().
                    addWrite(ScalarisDataHandler.getPageKey(page.getTitle()), page).
                    addWrite(ScalarisDataHandler.getRevListKey(page.getTitle()), revisions_short).
                    addCommit());
            result.processWriteAt(0);
            result.processWriteAt(1);
//            result.processCommitAt(2);
            pages.add(page.getTitle());
            // only export page list every 100 pages:
            if ((pages.size() % 100) == 0) {
                System.out.println("imported pages: " + pages.size());
                scalaris_single.write(ScalarisDataHandler.getPageListKey(), pages);
            }
        } catch (ConnectionException e) {
            System.err.println("write of page \"" + page.getTitle() + "\" failed with connection error");
        } catch (TimeoutException e) {
            System.err.println("write of page \"" + page.getTitle() + "\" failed with timeout");
        } catch (AbortException e) {
            System.err.println("write of page \"" + page.getTitle() + "\" failed with abort");
        } catch (UnknownException e) {
            System.err.println("write of page \"" + page.getTitle() + "\" failed with unknown: " + e.getMessage());
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler#tearDown()
     */
    @Override
    public void tearDown() {
        try {
            scalaris_single.write(ScalarisDataHandler.getPageListKey(), pages);
        } catch (ConnectionException e) {
            System.err.println("write of pages list failed with connection error");
        } catch (TimeoutException e) {
            System.err.println("write of pages list failed with timeout");
        } catch (AbortException e) {
            System.err.println("write of pages list failed with abort");
        } catch (UnknownException e) {
            System.err.println("write of pages list failed with unknown: " + e.getMessage());
        }
        String scalaris_key;
        for (Entry<String, List<String>> category: categories.entrySet()) {
            scalaris_key = ScalarisDataHandler.getCatPageListKey(category.getKey());
            try {
                scalaris_single.write(scalaris_key, category.getValue());
            } catch (ConnectionException e) {
                System.err.println("write of category page list \"" + scalaris_key + "\" failed with connection error");
            } catch (TimeoutException e) {
                System.err.println("write of category page list \"" + scalaris_key + "\" failed with timeout");
            } catch (AbortException e) {
                System.err.println("write of category page list \"" + scalaris_key + "\" failed with abort");
            } catch (UnknownException e) {
                System.err.println("write of category page list \"" + scalaris_key + "\" failed with unknown: " + e.getMessage());
            }
        }
        for (Entry<String, List<String>> template: templates.entrySet()) {
            scalaris_key = ScalarisDataHandler.getTplPageListKey(template.getKey());
            try {
                scalaris_single.write(scalaris_key, template.getValue());
            } catch (ConnectionException e) {
                System.err.println("write of template page list \"" + scalaris_key + "\" failed with connection error");
            } catch (TimeoutException e) {
                System.err.println("write of template page list \"" + scalaris_key + "\" failed with timeout");
            } catch (AbortException e) {
                System.err.println("write of template page list \"" + scalaris_key + "\" failed with abort");
            } catch (UnknownException e) {
                System.err.println("write of template page list \"" + scalaris_key + "\" failed with unknown: " + e.getMessage());
            }
        }
    }
}
