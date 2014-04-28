/**
 *  Copyright 2011-2013 Zuse Institute Berlin
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

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import de.zib.scalaris.examples.wikipedia.data.Revision;

/**
 * Represents a revision of a page for use by an XML reader.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class XmlRevision extends DefaultHandler {
    protected StringBuilder currentString = new StringBuilder();
    /**
     * the revision's ID
     */
    protected String id;

    /**
     * the revision's date of creation
     */
    protected String timestamp;
    
    /**
     * whether the change is a minor change or not
     */
    protected boolean minorChange;

    /**
     * the revision's contributor
     */
    protected XmlContributor currentContributor = new XmlContributor();

    /**
     * the comment of the revision
     */
    protected String comment;
    
    /**
     * the content (text) of the revision
     */
    protected String text;
    
    private boolean inRevision_contributor;
    
    protected Revision final_revision;

    /**
     * Creates a new revision with an empty id, timestamp, contributor,
     * comment and text.
     */
    public XmlRevision() {
        super();
        init();
    }

    /**
     * (Re-) Initialises all instance variables.
     */
    private void init() {
        currentString.setLength(0);
        id = "";
        timestamp = "";
        minorChange = false;
        currentContributor.reset();
        comment = "";
        text = "";
        inRevision_contributor = false;
        final_revision = null;
    }
    
    /**
     * Resets all instance variables. Afterwards, the object has the same state
     * as a newly created one.
     */
    public void reset() {
        init();
    }

    /**
     * Called to when a starting revision element is encountered.
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
    public void startRevision(String uri, String localName, String qName,
            Attributes attributes) {
        // nothing to do
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

        if (inRevision_contributor) {
            currentContributor.startElement(uri, localName, qName, attributes);
        } else {
            currentString.setLength(0);
            /*
             * <id>2</id> <timestamp>2006-10-01T11:36:54Z</timestamp>
             * <contributor> ... </contributor> <comment>...</comment>
             * <text>...</text>
             */
            if (localName.equals("id")) {
            } else if (localName.equals("timestamp")) {
            } else if (localName.equals("minor")) {
                minorChange = true;
            } else if (localName.equals("contributor")) {
                inRevision_contributor = true;
                currentContributor.reset();
                currentContributor.startContributor(uri, localName, qName, attributes);
            } else if (localName.equals("comment")) {
            } else if (localName.equals("text")) {
            } else if (localName.equals("sha1")) {
            } else {
                System.err.println("unknown revision tag: " + localName);
            }
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

        if (inRevision_contributor) {
            currentContributor.characters(ch, start, length);
        } else {
            currentString.append(ch, start, length);
        }
    }

    /**
     * Called to when an ending revision element is encountered.
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
    public void endRevision(String uri, String localName, String qName) {
        final_revision = new Revision(Integer.parseInt(id), timestamp,
                minorChange, currentContributor.getContributor(), comment);
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
        if (inRevision_contributor) {
            if (localName.equals("contributor")) {
                currentContributor.endContributor(uri, localName, qName);
                inRevision_contributor = false;
            } else {
                currentContributor.endElement(uri, localName, qName);
            }
        } else {
            if (localName.equals("id")) {
                id = currentString.toString();
            } else if (localName.equals("timestamp")) {
                timestamp = currentString.toString();
            } else if (localName.equals("minor")) {
                // nothing to do
            } else if (localName.equals("contributor")) {
                inRevision_contributor = false;
            } else if (localName.equals("comment")) {
                comment = currentString.toString();
            } else if (localName.equals("text")) {
                text = currentString.toString();
            }
        }
    }

    /**
     * Converts the {@link XmlRevision} object to a {@link Revision} object.
     * Throws in case of a malformed XML file.
     * 
     * @return the revision of a page
     */
    public Revision getRevision() {
        return final_revision;
    }

    /**
     * Gets the revision's (uncompressed) text.
     * 
     * @return the text
     */
    public String getText() {
        return text;
    }
}
