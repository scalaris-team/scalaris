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

import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.WikiModel;

import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * the standard output.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class WikiDumpHandler extends DefaultHandler {
    private boolean inSiteInfo = false;
    private boolean inPage = false;

    private XmlSiteInfo currentSiteInfo = null;
    private XmlPage currentPage = null;
    
    private Set<String> blacklist = null;
    
    protected WikiModel wikiModel;
    
    private int maxRevisions = -1; // all revisions by default

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     */
    public WikiDumpHandler(Set<String> blacklist, int maxRevisions) {
        this.blacklist = blacklist;
        this.maxRevisions = maxRevisions;
    }

    /**
     * Called to when a starting element is encountered.
     * 
     * @param uri
     *            The Namespace URI, or the empty string if the element has no
     *            Namespace URI or if Namespace processing is not being
     *            performed.
     * @param localName
     *            The local name (without prefix), or the empty string if
     *            Namespace processing is not being performed.
     * @param qName
     *            The qualified name (with prefix), or the empty string if
     *            qualified names are not available.
     * @param attributes
     *            The attributes attached to the element. If there are no
     *            attributes, it shall be an empty Attributes object.
     */
    @Override
    public void startElement(String uri, String localName, String qName,
            Attributes attributes) {
        // System.out.println(localName);

        /*
         * <siteinfo> <sitename>Wikipedia</sitename>
         * <base>http://bar.wikipedia.org/wiki/Hauptseitn</base>
         * <generator>MediaWiki 1.11alpha</generator> <case>first-letter</case>
         * <namespaces> <namespace key="-2">Media</namespace> ... </namespaces>
         * </siteinfo> <page></page> ...
         */
        if (localName.equals("siteinfo")) {
            inSiteInfo = true;
            currentSiteInfo = new XmlSiteInfo();
            currentSiteInfo.startSiteInfo(uri, localName, qName, attributes);
        } else if (localName.equals("page")) {
            inPage = true;
            currentPage = new XmlPage(maxRevisions);
            currentPage.startPage(uri, localName, qName, attributes);
        } else if (inSiteInfo) {
            currentSiteInfo.startElement(uri, localName, qName, attributes);
        } else if (inPage) {
            currentPage.startElement(uri, localName, qName, attributes);
        }
    }

    /**
     * Called to process character data.
     * 
     * Note: a SAX driver is free to chunk the character data any way it wants,
     * so you cannot count on all of the character data content of an element
     * arriving in a single characters event.
     * 
     * @param ch
     *            The characters.
     * @param start
     *            The start position in the character array.
     * @param length
     *            The number of characters to use from the character array.
     */
    @Override
    public void characters(char[] ch, int start, int length) {
        // System.out.println(new String(ch, start, length));
        if (inSiteInfo) {
            currentSiteInfo.characters(ch, start, length);
        }
        if (inPage) {
            currentPage.characters(ch, start, length);
        }
    }

    /**
     * Called to when an ending element is encountered.
     * 
     * @param uri
     *            The Namespace URI, or the empty string if the element has no
     *            Namespace URI or if Namespace processing is not being
     *            performed.
     * @param localName
     *            The local name (without prefix), or the empty string if
     *            Namespace processing is not being performed.
     * @param qName
     *            The qualified name (with prefix), or the empty string if
     *            qualified names are not available.
     */
    @Override
    public void endElement(String uri, String localName, String qName) {
        if (inSiteInfo) {
            if (localName.equals("siteinfo")) {
                inSiteInfo = false;
                currentSiteInfo.endSiteInfo(uri, localName, qName);
                setUpWikiModel(currentSiteInfo.getSiteInfo());
                export(currentSiteInfo);
                currentSiteInfo = null;
            } else {
                currentSiteInfo.endElement(uri, localName, qName);
            }
        } else if (inPage) {
            if (localName.equals("page")) {
                inPage = false;
                currentPage.endPage(uri, localName, qName);
                if (!blacklist.contains(currentPage.getPage().getTitle())) {
                    export(currentPage);
                }
                currentPage = null;
            } else {
                currentPage.endElement(uri, localName, qName);
            }
        }
    }
    
    private void setUpWikiModel(SiteInfo siteinfo) {
        wikiModel = new WikiModel(
                Configuration.DEFAULT_CONFIGURATION, null, new MyNamespace(siteinfo), "", "");
    }

    /**
     * Exports the given siteinfo.
     * 
     * @param siteinfo
     *            the siteinfo to export
     */
    protected abstract void export(XmlSiteInfo siteinfo);

    /**
     * Exports the given page (including all revisions).
     * 
     * @param page
     *            the page to export
     */
    protected abstract void export(XmlPage page);
    
    /**
     * Method to be called before using the handler.
     */
    public void setUp() {
    }

    /**
     * Method to be called after using the handler (to clean up).
     */
    public void tearDown() {
    }
}
