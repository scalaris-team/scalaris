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

import java.util.Map;
import java.util.regex.Matcher;

import com.almworks.sqlite4java.SQLiteException;

import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler.Connection;

/**
 * Wiki model using SQLite to fetch (new) data, e.g. templates.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MySQLiteWikiModel extends MyWikiModel {
    final protected Connection connection;
    
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
     * @param dbFileName
     *            the name of the database file to write to
     * @param namespace
     *            namespace of the wiki
     * 
     * @throws SQLiteException
     *             if the DB connection fails
     */
    public MySQLiteWikiModel(String imageBaseURL, String linkBaseURL, String dbFileName, MyNamespace namespace) throws SQLiteException {
        super(imageBaseURL, linkBaseURL, namespace);
        assert (dbFileName != null);
        this.connection = new Connection(dbFileName);
    }

//    /**
//     * Determines if a template name corresponds to a magic word using
//     * {@link MyScalarisMagicWord#isMagicWord(String)}.
//     * 
//     * @param name
//     *            the template name
//     * 
//     * @return whether the template is a magic word or not
//     */
//    @Override
//    protected boolean isMagicWord(String name) {
//        // TODO
//        return false;
////        return MyScalarisMagicWord.isMagicWord(name);
//    }
//
//    /**
//     * Retrieves the contents of the given magic word from Scalaris.
//     * 
//     * @param templateName
//     *            the template's name without the namespace, e.g. a magic word
//     *            including its parameters
//     * @param magicWord
//     *            the magic word alone
//     * @param parameter
//     *            the parameters of the magic word
//     * 
//     * @return the contents of the magic word (see
//     *         {@link MyScalarisMagicWord#processMagicWord(String, String, info.bliki.wiki.model.IWikiModel)})
//     */
//    @Override
//    protected String retrieveMagicWord(String articleName, String magicWord,
//            String parameter) {
//        // TODO
//        return MyMagicWord.processMagicWord(articleName, parameter, this);
////        return MyScalarisMagicWord.processMagicWord(magicWord, parameter, this);
//    }

    /**
     * Retrieves the contents of the given page from Scalaris. Caches retrieved
     * pages in {@link #pageCache}.
     * 
     * @param namespace
     *            the namespace of the page
     * @param articleName
     *            the (unnormalised) page's name without the namespace
     * @param templateParameters
     *            template parameters if the page is a template, <tt>null</tt>
     *            otherwise
     * @param followRedirect
     *            whether to follow a redirect or not (at most one redirect
     *            should be followed)
     * 
     * @return the page's contents or <tt>null</tt> if no connection exists
     */
    protected String retrievePage(String namespace, String articleName,
            Map<String, String> templateParameters, boolean followRedirect) {
        if (articleName.isEmpty()) {
            return null;
        }

        // normalise page name:
        NormalisedTitle pageName = normalisePageTitle(namespace, articleName);
        if (pageCache.containsKey(pageName)) {
            return pageCache.get(pageName);
        } else if (connection != null) {
            String text = null;
            // System.out.println("retrievePage(" + namespace + ", " + articleName + ")");
            RevisionResult getRevResult = SQLiteDataHandler.getRevision(connection, pageName, getNamespace());
            addStats(getRevResult.stats);
            addInvolvedKeys(getRevResult.involvedKeys);
            if (getRevResult.success) {
                text = getRevResult.revision.unpackedText();
                if (followRedirect && getRevResult.page.isRedirect()) {
                    final Matcher matcher = MATCH_WIKI_REDIRECT.matcher(text);
                    if (matcher.matches()) {
                        // see https://secure.wikimedia.org/wikipedia/en/wiki/Help:Redirect#Transclusion
                        String[] redirFullName = splitNsTitle(matcher.group(1));
                        String redirText = retrievePage(redirFullName[0], redirFullName[1], templateParameters, false);
                        if (redirText != null && !redirText.isEmpty()) {
                            text = redirText;
                        }
                    }
                }
            } else {
                // System.err.println(getRevResult.message);
                // text = "<b>ERROR: template " + pageName + " not available: " + getRevResult.message + "</b>";
            }
            pageCache.put(pageName, text);
            return text;
        }
        return null;
    }
}
