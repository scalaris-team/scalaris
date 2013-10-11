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

import java.math.BigInteger;
import java.util.HashSet;

import de.zib.scalaris.examples.wikipedia.SQLiteDataHandler;
import de.zib.scalaris.examples.wikipedia.ValueResult;

/**
 * Gets values for magic words not handled by {@link MyMagicWord} which need
 * to be retrieved from the SQLite DB.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MySQLiteMagicWord extends MyMagicWord {

    private static HashSet<String> MY_MAGIC_WORDS = new HashSet<String>(20);

    // private HashMap parameterValues = new HashMap();

    static {
        // statistics
        MY_MAGIC_WORDS.add(MAGIC_NUMBER_ARTICLES);
        MY_MAGIC_WORDS.add(MAGIC_NUMBER_PAGES);
        MY_MAGIC_WORDS.add(MAGIC_NUMBER_FILES);
        MY_MAGIC_WORDS.add(MAGIC_PAGES_IN_CATEGORY);
        MY_MAGIC_WORDS.add(MAGIC_PAGES_IN_CAT);
        MY_MAGIC_WORDS.add(MAGIC_NUMBER_USERS);
        MY_MAGIC_WORDS.add(MAGIC_NUMBER_ADMINS);
        MY_MAGIC_WORDS.add(MAGIC_PAGES_IN_NAMESPACE_NS);
        MY_MAGIC_WORDS.add(MAGIC_PAGES_IN_NAMESPACE);
        // page values
        MY_MAGIC_WORDS.add(MAGIC_REVISION_ID);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_DAY);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_DAY2);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_MONTH);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_YEAR);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_TIMESTAMP);
    }
    
    public static boolean isMagicWord(String name) {
        return MyMagicWord.isMagicWord(name) || isMyMagicWord(name);
    }

    /**
     * Determines if a template name corresponds to a magic word that is handled
     * by this class instead of {@link MyMagicWord}.
     * 
     * @param name
     *            the template name
     * 
     * @return whether this class should be favoured over {@link MyMagicWord} for
     *         parsing this template
     */
    public static boolean isMyMagicWord(String name) {
        return MY_MAGIC_WORDS.contains(name);
    }

    /**
     * Process a magic word, returning the value corresponding to the magic
     * word.
     * 
     * @param name
     *            the template name, i.e. a magic word
     * @param origParameter
     *            the template parameters
     * @param model
     *            the currently used model name
     * @param hasParameter
     *            whether a parameter was given or not (cannot distinguish from
     *            <tt>parameter</tt> value alone)
     * 
     * @return the value of the magic word
     * 
     * @see <a
     *      href="http://meta.wikimedia.org/wiki/Help:Magic_words">http://meta.wikimedia.org/wiki/Help:Magic_words</a>
     */
    public static String processMagicWord(final String name,
            final String origParameter, final MySQLiteWikiModel model, boolean hasParameter) {
        if (!isMyMagicWord(name)) {
            return MyMagicWord.processMagicWord(name, origParameter, model, hasParameter);
        }
        
        // check whether numbers should be printed in raw format and
        // remove this tag from the parameter string:
        boolean rawNumber = false;
        String parameter;
        if (origParameter.equals("R")) {
            parameter = "";
            rawNumber = true;
        } else if (origParameter.endsWith("|R")) {
            parameter = origParameter.substring(0, origParameter.length() - 2);
            rawNumber = true;
        } else {
            parameter = origParameter;
        }
        
        /*
         * Technical metadata / Latest revision to current page
         */
//      } else if (name.equals(MAGIC_REVISION_ID)) {
//          // TODO: implement
//          return null;
//      } else if (name.equals(MAGIC_REVISION_DAY)) {
//          // TODO: implement
//          return null;
//      } else if (name.equals(MAGIC_REVISION_DAY2)) {
//          // TODO: implement
//          return null;
//      } else if (name.equals(MAGIC_REVISION_MONTH)) {
//          // TODO: implement
//          return null;
//      } else if (name.equals(MAGIC_REVISION_YEAR)) {
//          // TODO: implement
//          return null;
//      } else if (name.equals(MAGIC_REVISION_TIMESTAMP)) {
//          // TODO: implement
//          return null;
//          {{REVISIONUSER}}
//          {{PROTECTIONLEVEL:action}}

        /*
         * Statistics / Entire wiki
         */
        if (name.equals(MAGIC_NUMBER_PAGES)) {
            ValueResult<BigInteger> pageCountResult = SQLiteDataHandler.getPageCount(model.connection);
            model.addStats(pageCountResult.stats);
            model.addInvolvedKeys(pageCountResult.involvedKeys);
            if (pageCountResult.success) {
                return model.formatStatisticNumber(rawNumber, pageCountResult.value);
            }
        } else if (name.equals(MAGIC_NUMBER_ARTICLES)) {
            ValueResult<BigInteger> pageCountResult = SQLiteDataHandler
                    .getArticleCount(model.connection);
            model.addStats(pageCountResult.stats);
            model.addInvolvedKeys(pageCountResult.involvedKeys);
            if (pageCountResult.success) {
                return model.formatStatisticNumber(rawNumber, pageCountResult.value);
            }
        } else if (name.equals(MAGIC_NUMBER_FILES)) {
            // we currently do not store files:
            return model.formatStatisticNumber(rawNumber, 0);
//            {{NUMBEROFEDITS}}
//            {{NUMBEROFVIEWS}}
        } else if (name.equals(MAGIC_NUMBER_USERS) || name.equals(MAGIC_NUMBER_ADMINS)) {
            // we currently do not support users/admins:
            return model.formatStatisticNumber(rawNumber, 0);
//            {{NUMBEROFACTIVEUSERS}}
        } else if (name.equals(MAGIC_PAGES_IN_CATEGORY) || name.equals(MAGIC_PAGES_IN_CAT)) {
            NormalisedTitle category = new NormalisedTitle(
                    MyNamespace.CATEGORY_NAMESPACE_KEY,
                    MyWikiModel.normaliseName(parameter.trim()));
            ValueResult<BigInteger> catListResult = SQLiteDataHandler
                    .getPagesInCategoryCount(model.connection, category);
            model.addStats(catListResult.stats);
            model.addInvolvedKeys(catListResult.involvedKeys);
            if (catListResult.success) {
                return model.formatStatisticNumber(rawNumber, catListResult.value);
            }
//            {{NUMBERINGROUP:groupname}}
//            {{NUMINGROUP:groupname}}
//        } else if (name.equals(MAGIC_PAGES_IN_NAMESPACE) || name.equals(MAGIC_PAGES_IN_NAMESPACE_NS)) {
//            // TODO: implement
//            return null;
        }
        
        return name;
    }
}
