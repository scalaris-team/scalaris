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

/**
 * Wiki model which should be used during parsing of xml dumps.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyParsingWikiModel extends MyWikiModel {

    /**
     * Creates a new wiki model to render wiki text.
     * 
     * @param imageBaseURL
     *            base url pointing to images - can contain ${image} for
     *            replacement
     * @param linkBaseURL
     *            base url pointing to links - can contain ${title} for
     *            replacement
     * @param namespace
     *            namespace of the wiki
     */
    public MyParsingWikiModel(String imageBaseURL, String linkBaseURL,
            MyNamespace namespace) {
        super(imageBaseURL, linkBaseURL, namespace);
    }

    /**
     * Creates a stub template content that has as many parameters as given by
     * the template call. This allows parsing of template contents hidden in
     * parameters.
     * 
     * @param name
     *            the template's name without the namespace
     * @param parameter
     *            the parameters of the template
     * 
     * @return the template's contents
     */
    @Override
    protected String retrieveTemplate(String name,
            Map<String, String> parameters) {
        if (parameters != null) {
            StringBuilder result = new StringBuilder(7 * parameters.size());
            for (int i = 1; i <= parameters.size(); ++i) {
                result.append("{{{");
                result.append(i);
                result.append("}}}");
            }
            return result.toString();
        }
        return null;
    }

}
