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
package de.zib.scalaris.examples.wikipedia.bliki;

import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.WikiModel;
import info.bliki.wiki.namespaces.INamespace;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

import de.zib.scalaris.Connection;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.RevisionResult;

/**
 * Wiki model using Scalaris to fetch (new) data, e.g. templates.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyWikiModel extends WikiModel {
    protected Connection connection;
    protected Map<String, String> magicWordCache = new HashMap<String, String>();
    private String fExternalWikiBaseFullURL;
    
    private static final Configuration configuration = Configuration.DEFAULT_CONFIGURATION;
    
    static {
        configuration.addTemplateFunction("fullurl", MyFullurl.CONST);
        configuration.addTemplateFunction("localurl", MyLocalurl.CONST);
        configuration.getInterwikiMap().remove("wiktionary"); // fix [[Wiktionary:...]] links
    }
    
    /**
     * Creates a new wiki model to render wiki text using the given connection
     * to Scalaris.
     * 
     * @param imageBaseURL
     *            base url pointing to images - can contain ${image} for
     *            replacement
     * @param linkBaseURL
     *            base url pointing to links - can contain ${title} for
     *            replacement
     * @param connection
     *            connection to Scalaris
     * @param namespace
     *            namespace of the wiki
     */
    public MyWikiModel(String imageBaseURL, String linkBaseURL, Connection connection, INamespace namespace) {
        super(configuration, null, namespace, imageBaseURL, linkBaseURL);
        this.connection = connection;
        this.fExternalWikiBaseFullURL = linkBaseURL;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#getRawWikiContent(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    public String getRawWikiContent(String namespace, String articleName,
            Map<String, String> templateParameters) {
        if (isTemplateNamespace(namespace)) {
            String magicWord = articleName;
            String parameter = "";
            int index = magicWord.indexOf(':');
            if (index > 0) {
                parameter = magicWord.substring(index + 1).trim();
                magicWord = magicWord.substring(0, index);
            }
            if (MyMagicWord.isMagicWord(magicWord)) {
                // cache values for magic words:
                if (magicWordCache.containsKey(articleName)) {
                    return magicWordCache.get(articleName);
                } else {
                    String value = MyMagicWord.processMagicWord(magicWord, parameter, this);
                    magicWordCache.put(articleName, value);
                    return value;
                }
            } else {
                // retrieve template from Scalaris:
                // note: templates are already cached, no need to cache them here
                if (connection != null) {
                    String pageName = getTemplateNamespace() + ":" + articleName;
                    RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
                    if (getRevResult.success) {
                        return getRevResult.revision.getText();
                    } else {
//                        System.err.println(getRevResult.message);
//                        return "<b>ERROR: template " + pageName + " not available: " + getRevResult.message + "</b>";
                        return null;
                    }
                }
            }
        }
        
        if (getRedirectLink() != null) {
            // requesting a page from a redirect?
            String pageName = getRedirectLink();
            RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
            if (getRevResult.success) {
                // make PAGENAME in the redirected content work as expected
                setPageName(pageName);
                return getRevResult.revision.getText();
            } else {
//                System.err.println(getRevResult.message);
//                return "<b>ERROR: redirect to " + getRedirectLink() + " failed: " + getRevResult.message + "</b>";
                return "#redirect [[" + pageName + "]]";
            }
        }
//        System.out.println("getRawWikiContent(" + namespace + ", " + articleName + ", " +
//            templateParameters + ")");
        return null;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#encodeTitleToUrl(java.lang.String, boolean)
     */
    @Override
    public String encodeTitleToUrl(String wikiTitle,
            boolean firstCharacterAsUpperCase) {
        try {
            // some links may contain '_' which needs to be translated back to ' ':
            wikiTitle = wikiTitle.replace('_', ' ');
            return URLEncoder.encode(wikiTitle, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return super.encodeTitleToUrl(wikiTitle, firstCharacterAsUpperCase);
        }
    }
    
    /**
     * Gets the base URL for images (can contain ${image} for replacement).
     * 
     * @return the base url for images
     */
    public String getImageBaseURL() {
        return fExternalImageBaseURL;
    }

    /**
     * Gets the base URL for links (can contain ${title} for replacement).
     * 
     * @return the base url for links
     */
    public String getLinkBaseURL() {
        return fExternalWikiBaseURL;
    }

    /**
     * Gets the base URL for full links including "http://" (can contain ${title}
     * for replacement).
     * 
     * @return the base url for links
     */
    public String getLinkBaseFullURL() {
        return fExternalWikiBaseFullURL;
    }

    /**
     * Sets the base URL for full links including "http://" (can contain ${title}
     * for replacement).
     * 
     * @param linkBaseFullURL
     *            the full link URL to set
     */
    public void setLinkBaseFullURL(String linkBaseFullURL) {
        this.fExternalWikiBaseFullURL = linkBaseFullURL;
    }
}
