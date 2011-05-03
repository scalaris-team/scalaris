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
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import de.zib.scalaris.Connection;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.PageListResult;
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
    
    private String formatStatisticNumber(String magicWord, int number) {
        if (magicWord.endsWith(":R") || magicWord.endsWith("|R")) {
            return String.valueOf(number);
        } else {
            // TODO: use locale from Wiki
            NumberFormat nf = NumberFormat.getIntegerInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            return nf.format(number);
        }
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#getRawWikiContent(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    public String getRawWikiContent(String namespace, String articleName,
            Map<String, String> templateParameters) {
        String result = super.getRawWikiContent(namespace, articleName, templateParameters);
        if (result != null) {
            // found magic word template
            // http://www.mediawiki.org/wiki/Help:Magic_words
            if (magicWordCache.containsKey(result)) {
                return magicWordCache.get(result);
            }
            // TODO: distinguish articles and pages correctly - see http://www.mediawiki.org/wiki/Manual:Using_custom_namespaces#Content_namespaces
            if (result.startsWith("NUMBEROFARTICLES") || result.startsWith("NUMBEROFPAGES")) {
                PageListResult pageCountResult = ScalarisDataHandler.getPageList(connection);
                if (pageCountResult.success) {
                    String pageCount = formatStatisticNumber(result, pageCountResult.pages.size());
                    magicWordCache.put(result, pageCount);
                    return pageCount;
                } else {
                    return result;
                }
            } else if (result.startsWith("NUMBEROFFILES")) {
                // we currently do not store files:
                String fileCount = formatStatisticNumber(result, 0);
                magicWordCache.put(result, fileCount);
                return fileCount;
            } else if (result.startsWith("NUMBEROFUSERS") ||
                    result.startsWith("NUMBEROFADMINS")) {
                // we currently do not support users:
                String userCount = formatStatisticNumber(result, 0);
                magicWordCache.put(result, userCount);
                return userCount;
            // namespaces:
            } else if (result.equals("NAMESPACE")) {
                String pageNamespace = WikiServlet.getNamespace(getPageName());
                magicWordCache.put(result, pageNamespace);
                return pageNamespace;
            } else if (result.equals("TALKSPACE") || result.equals("TALKSPACEE")) {
                String talkNamespace = getNamespace().getTalkspace(WikiServlet.getNamespace(getPageName()));
                magicWordCache.put(result, talkNamespace);
                return talkNamespace;
//            } else if (result.equals("SUBJECTSPACE") || result.equals("SUBJECTSPACEE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("ARTICLESPACE") || result.equals("ARTICLESPACEE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("CURRENTVERSION")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("PAGESINNAMESPACE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("SUBPAGENAME") || result.equals("SUBPAGENAMEE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("BASEPAGENAME") || result.equals("BASEPAGENAMEE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("SUBJECTPAGENAME") || result.equals("SUBJECTPAGENAMEE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("ARTICLEPAGENAME") || result.equals("ARTICLEPAGENAMEE")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("REVISIONID")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("REVISIONDAY")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("REVISIONDAY2")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("REVISIONMONTH")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("REVISIONYEAR")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("REVISIONTIMESTAMP")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("SITENAME")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("SERVER")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("SCRIPTPATH")) {
//                // TODO: implement
//                return null;
//            } else if (result.equals("SERVERNAME")) {
//                // TODO: implement
//                return null;
            // some MediaWiki-encoded URLs:
            } else if (result.equals("PAGENAMEE")) {
                return "{{PAGENAME}}";
            } else if (result.equals("NAMESPACEE")) {
                return "{{NAMESPACE}}";
            } else if (result.equals("FULLPAGENAMEE")) {
                return "{{FULLPAGENAME}}";
            } else if (result.equals("TALKPAGENAMEE")) {
                return "{{TALKPAGENAME}}";
            }
            
            return result;
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
        } else {
            if (isTemplateNamespace(namespace)) {
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
