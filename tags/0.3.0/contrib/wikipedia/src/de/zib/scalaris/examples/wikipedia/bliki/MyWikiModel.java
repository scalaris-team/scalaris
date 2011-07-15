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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import de.zib.scalaris.Connection;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;

/**
 * Wiki model using Scalaris to fetch (new) data, e.g. templates.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyWikiModel extends WikiModel {
    protected Connection connection;
    protected Map<String, String> magicWordCache = new HashMap<String, String>();
    private String fExternalWikiBaseFullURL;
    
    private static final Configuration configuration = new Configuration();
    
    static {
        // BEWARE: fields in Configuration are static -> this changes all configurations!
        configuration.addTemplateFunction("fullurl", MyFullurl.CONST);
        configuration.addTemplateFunction("localurl", MyLocalurl.CONST);
        // do not use interwiki links (some may be internal - bliki however favours interwiki links)
        configuration.getInterwikiMap().clear();
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
    public MyWikiModel(String imageBaseURL, String linkBaseURL, Connection connection, MyNamespace namespace) {
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
            return getRedirectContent(getRedirectLink());
        }
//        System.out.println("getRawWikiContent(" + namespace + ", " + articleName + ", " +
//            templateParameters + ")");
        return null;
    }
    
    /**
     * Gets the contents of the newest revision of the page redirected to.
     * 
     * @param pageName
     *            the name of the page redirected to
     * 
     * @return the contents of the newest revision of that page or a placeholder
     *         string
     */
    public String getRedirectContent(String pageName) {
        RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
        if (getRevResult.success) {
            // make PAGENAME in the redirected content work as expected
            setPageName(pageName);
            return getRevResult.revision.getText();
        } else {
//            System.err.println(getRevResult.message);
//            return "<b>ERROR: redirect to " + getRedirectLink() + " failed: " + getRevResult.message + "</b>";
            return "&#35;redirect [[" + pageName + "]]";
        }
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

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#getNamespace()
     */
    @Override
    public MyNamespace getNamespace() {
        return (MyNamespace) super.getNamespace();
    }
    
    /**
     * Formats the given number using the wiki's locale.
     * 
     * Note: Currently, the English locale is always used.
     * 
     * @param rawNumber
     *            whether the raw number should be returned
     * @param number
     *            the number
     * 
     * @return the formatted number
     */
    public String formatStatisticNumber(boolean rawNumber, Number number) {
        if (rawNumber) {
            return number.toString();
        } else {
            // TODO: use locale from Wiki
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            return nf.format(number);
        }
    }

    /**
     * Splits the given full title into its namespace and page title components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * 
     * @return a 2-element array with the namespace (index 0) and the page title
     *         (index 1)
     */
    public static String[] splitNsTitle(String fullTitle) {
        int colonIndex = fullTitle.indexOf(':');
        if (colonIndex != (-1)) {
            return new String[] { fullTitle.substring(0, colonIndex),
                    fullTitle.substring(colonIndex + 1) };
        }
        return new String[] {"", fullTitle};
    }

    /**
     * Returns the namespace of a given page title.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * 
     * @return the namespace part of the title or an empty string if no
     *         namespace
     * 
     * @see #getTitleName(String)
     * @see #splitNsTitle(String)
     */
    public static String getNamespace(String title) {
        return splitNsTitle(title)[0];
    }

    /**
     * Returns the name of a given page title without its namespace.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * 
     * @return the title part of the page title
     * 
     * @see #getNamespace(String)
     * @see #splitNsTitle(String)
     */
    public static String getTitleName(String title) {
        return splitNsTitle(title)[1];
    }
    
    /**
     * Splits the given full title into its namespace, base and sub page
     * components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * 
     * @return a 3-element array with the namespace (index 0), the base page
     *         (index 1) and the sub page (index 2)
     */
    public static String[] splitNsBaseSubPage(String fullTitle) {
        String[] split1 = splitNsTitle(fullTitle);
        String namespace = split1[0];
        String title = split1[1];
        int colonIndex = title.lastIndexOf('/');
        if (colonIndex != (-1)) {
            return new String[] { namespace, title.substring(0, colonIndex),
                    title.substring(colonIndex + 1) };
        }
        return new String[] {namespace, title, ""};
    }
}
