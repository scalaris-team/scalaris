/**
 *  Copyright 2011 Zuse Institute Berlin
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

import de.zib.scalaris.examples.wikipedia.data.Contributor;

/**
 * Contributor known as a registered user for use by an XML reader.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class XmlContributor extends DefaultHandler {
    protected StringBuffer currentString = new StringBuffer();
    protected String user = "";
    protected String id = "";
    protected String ip = "";
    
    Contributor final_contributor = null;

    /**
     * Creates a new contributor with a (temporarily) empty username and ID.
     * 
     */
    public XmlContributor() {
        super();
    }

    /**
     * Creates a new contributor with the given ip.
     * 
     * @param ip
     *            an IP address or a custom name
     */
    public XmlContributor(String ip) {
        super();
        this.ip = ip;
    }
    
    /**
     * Called to when a starting contributor element is encountered.
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
    public void startContributor(String uri, String localName, String qName,
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
        currentString.setLength(0);
        /*
         * <ip>127.0.0.1</ip>
         * 
         * or
         * 
         * <username>Melancholie</username> <id>12</id>
         */
        if (localName.equals("username")) {
        } else if (localName.equals("id")) {
        } else if (localName.equals("ip")) {
        } else {
            System.err.println("unknown contributor tag: " + localName);
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
        currentString.append(ch, start, length);
    }

    /**
     * Called to when an ending contributor element is encountered.
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
    public void endContributor(String uri, String localName, String qName) {
//        System.out.println("ip: " + ip + ", user: " + user + ", id: " + id);
        final_contributor = new Contributor();
        final_contributor.setIp(ip);
        if (!id.isEmpty()) {
            final_contributor.setId(Integer.parseInt(id));
        }
        final_contributor.setUser(user);
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
        if (localName.equals("username")) {
            user = currentString.toString();
        } else if (localName.equals("id")) {
            id = currentString.toString();
        } else if (localName.equals("ip")) {
            ip = currentString.toString();
        }
    }

    /**
     * Converts the {@link XmlContributor} object to a {@link Contributor}
     * object.
     * Throws in case of a malformed XML file.
     * 
     * @return the contributor of a revision
     */
    public Contributor getContributor() {
        return final_contributor;
    }
}
