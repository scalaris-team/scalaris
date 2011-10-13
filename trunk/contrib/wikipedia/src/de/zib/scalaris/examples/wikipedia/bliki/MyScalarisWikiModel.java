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
    protected Map<String, String> pageCache = new HashMap<String, String>();
    
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

    /**
     * Determines if a template name corresponds to a magic word using
     * {@link MyScalarisMagicWord#isMagicWord(String)}.
     * 
     * @param name
     *            the template name
     * 
     * @return whether the template is a magic word or not
     */
    @Override
    protected boolean isMagicWord(String name) {
        return MyScalarisMagicWord.isMagicWord(name);
    }

    /**
     * Retrieves the contents of the given magic word from Scalaris.
     * 
     * @param templateName
     *            the template's name without the namespace, e.g. a magic word
     *            including its parameters
     * @param magicWord
     *            the magic word alone
     * @param parameter
     *            the parameters of the magic word
     * 
     * @return the contents of the magic word (see
     *         {@link MyScalarisMagicWord#processMagicWord(String, String, info.bliki.wiki.model.IWikiModel)})
     */
    @Override
    protected String retrieveMagicWord(String articleName, String magicWord,
            String parameter) {
        return MyScalarisMagicWord.processMagicWord(magicWord, parameter, this);
    }

    /**
     * Retrieves the contents of the given page from Scalaris.
     * Caches retrieved pages in {@link #pageCache}.
     * 
     * @param namespace
     *            the namespace of the page
     * @param articleName
     *            the page's name without the namespace
     * @param templateParameters
     *            template parameters if the page is a template, <tt>null</tt>
     *            otherwise
     * 
     * @return the page's contents or <tt>null</tt> if no connection exists
     */
    @Override
    protected String retrievePage(String namespace, String articleName,
            Map<String, String> templateParameters) {
        if (connection != null) {
            String pageName = createFullPageName(namespace, articleName);
            String text = pageCache.get(pageName);
            if (text == null) {
                RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
                if (getRevResult.success) {
                    text = getRevResult.revision.getText();
                } else {
                    // System.err.println(getRevResult.message);
                    // text = "<b>ERROR: template " + pageName + " not available: " + getRevResult.message + "</b>";
                    /*
                     * the page was not found and will never be - assume
                     * an empty content instead of letting the model try
                     * again (which is what it does if null is returned)
                     */
                    text = "";
                }
                pageCache.put(pageName, text);
            }
            return text;
        } else {
            return null;
        }
    }
    
    /**
     * Gets the contents of the newest revision of the page redirected to.
     * 
     * @param pageName
     *            the name of the page redirected to
     * 
     * @return the contents of the newest revision of that page or a placeholder
     *         string for the redirect
     */
    @Override
    public String getRedirectContent(String pageName) {
        if (connection != null) {
            RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
            if (getRevResult.success) {
                // make PAGENAME in the redirected content work as expected
                setPageName(pageName);
                return getRevResult.revision.getText();
            } else {
//                System.err.println(getRevResult.message);
//                return "<b>ERROR: redirect to " + getRedirectLink() + " failed: " + getRevResult.message + "</b>";
            }
        }
        return "&#35;redirect [[" + pageName + "]]";
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel#setUp()
     */
    @Override
    public void setUp() {
        super.setUp();
        pageCache = new HashMap<String, String>();
    }
}
