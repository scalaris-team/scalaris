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
package de.zib.scalaris.examples.wikipedia.bliki;

import de.zib.scalaris.Connection;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandlerNormalised;

/**
 * Wiki model using Scalaris to fetch (new) data, e.g. templates.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyScalarisWikiModel extends MyWikiModel {
    protected Connection connection;
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
     * @param hasParameter
     *            whether a parameter was given or not (cannot distinguish from
     *            <tt>parameter</tt> value alone)
     * 
     * @return the contents of the magic word (see
     *         {@link MyScalarisMagicWord#processMagicWord(String, String, info.bliki.wiki.model.IWikiModel)})
     */
    @Override
    protected String retrieveMagicWord(String articleName, String magicWord,
            String parameter, boolean hasParameter) {
        return MyScalarisMagicWord.processMagicWord(magicWord, parameter, this, hasParameter);
    }
    
    @Override
    protected boolean hasDBConnection() {
        return connection != null;
    }

    @Override
    protected RevisionResult getRevFromDB(NormalisedTitle pageName) {
        return ScalarisDataHandlerNormalised.getRevision(connection, pageName);
    }

}
