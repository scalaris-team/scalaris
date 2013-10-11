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

import java.util.HashMap;
import java.util.Map;

import org.xml.sax.Attributes;
import org.xml.sax.helpers.DefaultHandler;

import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Represents generic site information for use by an XML reader.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class XmlSiteInfo extends DefaultHandler {
    protected StringBuilder currentString = new StringBuilder();
    
    protected String base = "";
    protected String sitename = "";
    protected String generator = "";
    protected String caseStr = "";
    protected Map<String, Map<String, String>> namespaces = new HashMap<String, Map<String, String>>();
    protected String currentNamespaceKey = null;
    protected Map<String, String> currentNamespace = null;
    
    protected boolean inSiteInfo_namespaces = false;
    
    protected SiteInfo final_siteinfo = null;

    /**
     * Creates an empty site info object.
     */
    public XmlSiteInfo() {
        super();
    }
    
    /**
     * Called to when a starting siteinfo element is encountered.
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
    public void startSiteInfo(String uri, String localName, String qName,
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
         * <sitename>Wikipedia</sitename>
         * <base>http://bar.wikipedia.org/wiki/Hauptseitn</base>
         * <generator>MediaWiki 1.17wmf1</generator>
         * <case>first-letter</case>
         * <namespaces>
         *   <namespace key="-2" case="first-letter">Medium</namespace>
         *   <namespace key="-1" case="first-letter">Spezial</namespace>
         *   <namespace key="0" case="first-letter" />
         *   <namespace key="1" case="first-letter">Diskussion</namespace>
         *   <namespace key="2" case="first-letter">Benutzer</namespace>
         *   <namespace key="3" case="first-letter">Benutzer Diskussion</namespace>
         *   <namespace key="4" case="first-letter">Wikipedia</namespace>
         *   <namespace key="5" case="first-letter">Wikipedia Diskussion</namespace>
         *   <namespace key="6" case="first-letter">Datei</namespace>
         *   <namespace key="7" case="first-letter">Datei Diskussion</namespace>
         *   <namespace key="8" case="first-letter">MediaWiki</namespace>
         *   <namespace key="9" case="first-letter">MediaWiki Diskussion</namespace>
         *   <namespace key="10" case="first-letter">Vorlage</namespace>
         *   <namespace key="11" case="first-letter">Vorlage Diskussion</namespace>
         *   <namespace key="12" case="first-letter">Hilfe</namespace>
         *   <namespace key="13" case="first-letter">Hilfe Diskussion</namespace>
         *   <namespace key="14" case="first-letter">Kategorie</namespace>
         *   <namespace key="15" case="first-letter">Kategorie Diskussion</namespace>
         *   <namespace key="100" case="first-letter">Portal</namespace>
         *   <namespace key="101" case="first-letter">Portal Diskussion</namespace>
         * </namespaces>
         */
        if (localName.equals("sitename")) {
        } else if (localName.equals("base")) {
        } else if (localName.equals("generator")) {
        } else if (localName.equals("case")) {
        } else if (localName.equals("namespaces")) {
            inSiteInfo_namespaces = true;
        } else if (inSiteInfo_namespaces && localName.equals("namespace")) {
            currentNamespace = new HashMap<String, String>();
            currentNamespace.put(SiteInfo.NAMESPACE_CASE, attributes.getValue("", "case"));
            currentNamespaceKey = attributes.getValue("", "key");
        } else {
            System.err.println("unknown siteinfo tag: " + localName);
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
    public void characters(char[] ch, int start, int length) {
        // System.out.println(new String(ch, start, length));
        currentString.append(ch, start, length);
    }

    /**
     * Called to when an ending siteinfo element is encountered.
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
    public void endSiteInfo(String uri, String localName, String qName) {
        final_siteinfo = new SiteInfo(base, sitename, generator, caseStr, namespaces);
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
    public void endElement(String uri, String localName, String qName) {
        if (localName.equals("sitename")) {
            sitename = currentString.toString();
        } else if (localName.equals("base")) {
            base = currentString.toString();
        } else if (localName.equals("generator")) {
            generator = currentString.toString();
        } else if (localName.equals("case")) {
            caseStr = currentString.toString();
        } else if (localName.equals("namespaces")) {
            inSiteInfo_namespaces = false;
        } else if (localName.equals("namespace")) {
            currentNamespace.put(SiteInfo.NAMESPACE_PREFIX, currentString.toString());
            namespaces.put(currentNamespaceKey, currentNamespace);
        }
    }

    /**
     * Converts the {@link XmlSiteInfo} object to a {@link SiteInfo} object.
     * Throws in case of a malformed XML file.
     * 
     * @return the siteinfo of a wiki
     */
    public SiteInfo getSiteInfo() {
        return final_siteinfo;
    }
}
