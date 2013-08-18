/**
 *  Copyright 2007-2013 Zuse Institute Berlin
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
import info.bliki.wiki.template.AbstractTemplateFunction;
import info.bliki.wiki.template.ITemplateFunction;
import info.bliki.wiki.template.If;

import java.util.List;

/**
 * A template parser function for <code>{{ #if: ... }}</code> syntax.
 * Also works for <tt>#iferror</tt>, <tt>#ifeq</tt>, <tt>#ifexist</tt>,
 * <tt>#ifexpr</tt>.
 * 
 * Based on {@link If} but does not evaluate the condition. This is useful
 * during filtering since the condition needs to be evaluated at runtime which
 * may yield another result.
 */
public class MyParsingIfTemplateFun extends AbstractTemplateFunction {
    /**
     * Static instance of this template function parser.
     */
    public final static ITemplateFunction CONST = new MyParsingIfTemplateFun();

    /**
     * Constructor.
     */
    public MyParsingIfTemplateFun() {
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.template.AbstractTemplateFunction#parseFunction(java.util.List, info.bliki.wiki.model.IWikiModel, char[], int, int)
     */
    @Override
    public String parseFunction(List<String> list, IWikiModel model,
            char[] src, int beginIndex, int endIndex, boolean isSubst) {
        if (list.size() > 1) {
            if (!isSubst) {
                parseTrim(list.get(0), model);
            }
            // parse both parameters
            String result = isSubst ? list.get(1) : parseTrim(list.get(1), model);
            if (list.size() >= 3) {
                result += isSubst ? list.get(2) : parseTrim(list.get(2), model);
            }
            return result;
        }
        return null;
    }

}
