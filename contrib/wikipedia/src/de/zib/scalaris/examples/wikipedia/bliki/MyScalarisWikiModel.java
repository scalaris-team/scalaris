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


import java.util.HashMap;
import java.util.Map;

import de.zib.scalaris.Connection;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;

/**
 * Wiki model using Scalaris to fetch (new) data, e.g. templates.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyScalarisWikiModel extends MyWikiModel {
    protected Connection connection;
    protected Map<String, String> magicWordCache = new HashMap<String, String>();
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
    public MyScalarisWikiModel(String imageBaseURL, String linkBaseURL, Connection connection, MyNamespace namespace) {
        super(imageBaseURL, linkBaseURL, namespace);
        this.connection = connection;
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
            if (MyScalarisMagicWord.isMagicWord(magicWord)) {
                // cache values for magic words:
                if (magicWordCache.containsKey(articleName)) {
                    return magicWordCache.get(articleName);
                } else {
                    String value = MyScalarisMagicWord.processMagicWord(magicWord, parameter, this);
                    magicWordCache.put(articleName, value);
                    return value;
                }
            } else {
                // retrieve template from Scalaris:
                // note: templates are already cached, no need to cache them here
                if (connection != null) {
                    // (ugly) fix for template parameter replacement if no parameters given,
                    // e.g. "{{noun}}" in the simple English Wiktionary
                    if (templateParameters != null && templateParameters.isEmpty()) {
                        templateParameters.put("", null);
                    }
                    String pageName = getTemplateNamespace() + ":" + articleName;
                    RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
                    if (getRevResult.success) {
                        String text = getRevResult.revision.getText();
                        text = removeNoIncludeContents(text);
                        return text;
                    } else {
//                        System.err.println(getRevResult.message);
//                        return "<b>ERROR: template " + pageName + " not available: " + getRevResult.message + "</b>";
                        /*
                         * the template was not found and will never be - assume
                         * an empty content instead of letting the model try
                         * again (which is what it does if null is returned)
                         */
                        return "";
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
}
