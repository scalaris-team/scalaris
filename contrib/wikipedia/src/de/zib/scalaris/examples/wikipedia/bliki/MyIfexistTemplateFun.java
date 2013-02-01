/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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

import info.bliki.wiki.model.IWikiModel;
import info.bliki.wiki.template.ITemplateFunction;
import info.bliki.wiki.template.Ifexist;

import java.util.List;

/**
 * A template parser function for <code>{{ #ifexist: ... }}</code> syntax. See
 * <a href="http://www.mediawiki.org/wiki/Help:Extension:ParserFunctions">
 * Mediwiki's Help:Extension:ParserFunctions</a>
 * 
 * Based on {@link Ifexist} but every title in the Media, Image or File
 * namespace is assumed to exist.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyIfexistTemplateFun extends Ifexist {
    /**
     * Static instance of this template function parser.
     */
    public final static ITemplateFunction CONST = new MyIfexistTemplateFun();

    /**
     * Constructor.
     */
    public MyIfexistTemplateFun() {
        super();
    }

    @Override
    public String parseFunction(List<String> list, IWikiModel model,
            char[] src, int beginIndex, int endIndex, boolean isSubst) {
        if (model instanceof MyWikiModel) {
            MyWikiModel myModel = (MyWikiModel) model;
            if (list.size() > 1) {
                final String[] wikiTopicName = myModel.splitNsTitle(isSubst ? list.get(0) : parseTrim(list.get(0), model));
                if (myModel.isImageNamespace(wikiTopicName[0])
                        || myModel.isMediaNamespace(wikiTopicName[0])
                        || model.getRawWikiContent(wikiTopicName[0], wikiTopicName[1], null) != null) {
                    return isSubst ? list.get(1) : parseTrim(list.get(1), model);
                } else { // non-existing page
                    if (list.size() >= 3) {
                        return isSubst ? list.get(2) : parseTrim(list.get(2), model);
                    }
                }
            }
            return null;
        } else {
            return super.parseFunction(list, model, src, beginIndex, endIndex, isSubst);
        }
    }
}
