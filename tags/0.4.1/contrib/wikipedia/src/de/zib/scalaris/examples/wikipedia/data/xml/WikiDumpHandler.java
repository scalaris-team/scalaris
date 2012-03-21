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

import java.io.PrintStream;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace;
import de.zib.scalaris.examples.wikipedia.bliki.MyParsingWikiModel;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.examples.wikipedia.data.xml.XmlPage.CheckSkipRevisions;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * the standard output.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class WikiDumpHandler extends DefaultHandler implements WikiDump {
    private boolean inSiteInfo = false;
    private boolean inPage = false;

    private XmlSiteInfo currentSiteInfo = null;
    private XmlPage currentPage;

    private Set<String> blacklist = null;
    private Set<String> whitelist = null;
    
    protected MyParsingWikiModel wikiModel;
    
    /**
     * The time at the start of an import operation.
     */
    private long timeAtStart = 0;
    /**
     * The time at the end of an import operation.
     */
    private long timeAtEnd = 0;
    /**
     * The number of (successfully) processed pages.
     */
    protected int pageCount = 0;
    
    protected PrintStream msgOut = System.out;
    
    protected boolean stop = false;

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist.
     * 
     * If a whitelist is given, a skip revision handler is set which ignores
     * pages not in the whitelist. See
     * {@link #setPageCheckSkipRevisions(CheckSkipRevisions)}.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages (pages in this list will be normalised
     *            once the site info is read)
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
     *            omitted) - <tt>null/tt> imports all revisions (useful to
     *            create dumps of a wiki at a specific point in time)
     */
    public WikiDumpHandler(Set<String> blacklist, Set<String> whitelist, int maxRevisions, Calendar minTime, Calendar maxTime) {
        this.blacklist = blacklist;
        this.whitelist = whitelist;
        currentPage = new XmlPage(maxRevisions, minTime, maxTime);
        // if a whitelist is given, do not render any other page:
        if (whitelist != null) {
            currentPage.setCheckSkipRevisions(new CheckSkipRevisions() {
                @Override
                public boolean skipRevisions(String pageTitle) {
                    return !inWhiteList(pageTitle);
                }
            });
        }
    }
    
    /**
     * Checks whether the given page title is in the whitelist.
     * 
     * Note: a title's first character may need to be capitalised in order to be
     * found.
     * 
     * @param pageTitle
     *            the title of a page
     * 
     * @return whether the page should be imported or not
     */
    private boolean inWhiteList(String pageTitle) {
        return WikiDumpHandler.this.whitelist.contains(pageTitle)
                || WikiDumpHandler.this.whitelist.contains(wikiModel.normalisePageTitle(pageTitle));
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
            Attributes attributes) throws SAXException {
        if (stop) {
            throw new SAXParsingInterruptedException();
        }
        // out.println(localName);

        /*
         * <siteinfo> <sitename>Wikipedia</sitename>
         * <base>http://bar.wikipedia.org/wiki/Hauptseitn</base>
         * <generator>MediaWiki 1.11alpha</generator> <case>first-letter</case>
         * <namespaces> <namespace key="-2">Media</namespace> ... </namespaces>
         * </siteinfo> <page></page> ...
         */
        if (localName.equals("mediawiki")) {
            importStart();
        } else if (localName.equals("siteinfo")) {
            inSiteInfo = true;
            currentSiteInfo = new XmlSiteInfo();
            currentSiteInfo.startSiteInfo(uri, localName, qName, attributes);
        } else if (localName.equals("page")) {
            inPage = true;
            currentPage.reset();
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
    public void characters(char[] ch, int start, int length)
            throws SAXException {
        // out.println(new String(ch, start, length));
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
    public void endElement(String uri, String localName, String qName)
            throws SAXException {
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
                if (currentPage.getPage() != null &&
                        !blacklist.contains(currentPage.getPage().getTitle()) &&
                        (whitelist == null || inWhiteList(currentPage.getPage().getTitle()))) {
                    export(currentPage);
                }
                currentPage.reset();
            } else {
                currentPage.endElement(uri, localName, qName);
            }
        } else if (localName.equals("mediawiki")) {
            importEnd();
        }
    }
    
    private void setUpWikiModel(SiteInfo siteinfo) {
        wikiModel = new MyParsingWikiModel("", "", new MyNamespace(siteinfo));
        // we are now able to normalise the page titles in the whitelist:
        if (whitelist != null) {
            Set<String> oldWhitelist = whitelist;
            whitelist = new HashSet<String>(oldWhitelist.size());
            wikiModel.normalisePageTitles(oldWhitelist, whitelist);
        }
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

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#setUp()
     */
    @Override
    public void setUp() {
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#tearDown()
     */
    @Override
    public void tearDown() {
    }

    /**
     * Sets the time the import started.
     */
    final protected void importStart() {
        timeAtStart = System.currentTimeMillis();
    }

    /**
     * Sets the time the import finished.
     */
    final protected void importEnd() {
        timeAtEnd = System.currentTimeMillis();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getTimeAtStart()
     */
    @Override
    public long getTimeAtStart() {
        return timeAtStart;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getTimeAtEnd()
     */
    @Override
    public long getTimeAtEnd() {
        return timeAtEnd;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#getPageCount()
     */
    @Override
    public int getImportCount() {
        return pageCount;
    }

    /**
     * Reports the speed of the import (pages/s) and may be used as a shutdown
     * handler.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public class ReportAtShutDown extends Thread {
        public void run() {
            // import may have been interrupted - get an end time in this case
            if (timeAtEnd == 0) {
                importEnd();
            }
            final long timeTaken = timeAtEnd - timeAtStart;
            final double speed = (((double) pageCount) * 1000) / timeTaken;
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            println("Finished import (" + nf.format(speed) + " pages/s)");
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.data.xml.WikiDump#setMsgOut(java.io.PrintStream)
     */
    @Override
    public void setMsgOut(PrintStream msgOut) {
        this.msgOut = msgOut;
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * 
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    public void print(String message) {
        print(msgOut, message);
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * Includes a newline character at the end.
     * 
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    public void println(String message) {
        println(msgOut, message);
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * 
     * @param msgOut
     *            the output stream to write to
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    static public void print(PrintStream msgOut, String message) {
        msgOut.print("[" + (new Date()).toString() + "] " + message);
    }

    /**
     * Prints a message to the chosen output stream (includes a timestamp).
     * Includes a newline character at the end.
     * 
     * @param msgOut
     *            the output stream to write to
     * @param message
     *            the message to print
     * 
     * @see #setMsgOut(PrintStream)
     */
    static public void println(PrintStream msgOut, String message) {
        msgOut.println("[" + (new Date()).toString() + "] " + message);
    }

    /**
     * @param checkSkipRevisions the checkSkipRevisions to set
     */
    protected void setPageCheckSkipRevisions(CheckSkipRevisions checkSkipRevisions) {
        this.currentPage.setCheckSkipRevisions(checkSkipRevisions);
    }

    /**
     * Tells the parser to stop at the next starting element.
     */
    @Override
    public void stopParsing() {
        this.stop = true;
    }
}
