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

import java.util.ArrayList;
import java.util.List;

import de.zib.scalaris.examples.wikipedia.BigIntegerResult;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;

/**
 * Gets values for magic words not handled by {@link MyMagicWord} which need
 * to be retrieved from Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyScalarisMagicWord extends MyMagicWord {
    // statistics
    @SuppressWarnings("javadoc")
    public static final String MAGIC_NUMBER_ARTICLES = "NUMBEROFARTICLES";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_NUMBER_PAGES = "NUMBEROFPAGES";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_NUMBER_FILES = "NUMBEROFFILES";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGES_IN_CATEGORY = "PAGESINCATEGORY";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGES_IN_CAT = "PAGESINCAT";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_NUMBER_USERS = "NUMBEROFUSERS";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_NUMBER_ADMINS = "NUMBEROFADMINS";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGES_IN_NS = "PAGESINNS";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGES_IN_NAMESPACE = "PAGESINNAMESPACE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_REVISION_ID = "REVISIONID";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_REVISION_DAY = "REVISIONDAY";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_REVISION_DAY2 = "REVISIONDAY2";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_REVISION_MONTH = "REVISIONMONTH";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_REVISION_YEAR = "REVISIONYEAR";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_REVISION_TIMESTAMP = "REVISIONTIMESTAMP";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGE_SIZE = "PAGESIZE";

    private static List<String> MY_MAGIC_WORDS = new ArrayList<String>();

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
        MY_MAGIC_WORDS.add(MAGIC_PAGES_IN_NS);
        MY_MAGIC_WORDS.add(MAGIC_PAGES_IN_NAMESPACE);
        // page values
        MY_MAGIC_WORDS.add(MAGIC_REVISION_ID);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_DAY);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_DAY2);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_MONTH);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_YEAR);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_TIMESTAMP);
        MY_MAGIC_WORDS.add(MAGIC_PAGE_SIZE);
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
     * @param name      the template name, i.e. a magic word
     * @param parameter the template parameters
     * @param model     the currently used model
     * 
     * @return the value of the magic word
     * 
     * @see <a href="http://meta.wikimedia.org/wiki/Help:Magic_words">http://meta.wikimedia.org/wiki/Help:Magic_words</a>
     */
    public static String processMagicWord(String name, String parameter, MyScalarisWikiModel model) {
        if (!isMyMagicWord(name)) {
            return MyMagicWord.processMagicWord(name, parameter, model);
        }
        
        // check whether numbers should be printed in raw format and
        // remove this tag from the parameter string:
        boolean rawNumber = false;
        if (parameter.equals("R")) {
            parameter = "";
            rawNumber = true;
        } else if (parameter.endsWith("|R")) {
            parameter = parameter.substring(0, parameter.length() - 2);
            rawNumber = true;
        }
        
        /*
         * Technical metadata / Latest revision to current page
         */
        if (name.equals(MAGIC_PAGE_SIZE)) {
            RevisionResult getRevResult = ScalarisDataHandler.getRevision(model.connection, parameter);
            int size = 0;
            if (getRevResult.success) {
                size = getRevResult.revision.unpackedText().getBytes().length;
            }
            return model.formatStatisticNumber(rawNumber, size);
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
        } else if (name.equals(MAGIC_NUMBER_PAGES)) {
            BigIntegerResult pageCountResult = ScalarisDataHandler.getPageCount(model.connection);
            if (pageCountResult.success) {
                return model.formatStatisticNumber(rawNumber, pageCountResult.number);
            }
        } else if (name.equals(MAGIC_NUMBER_ARTICLES)) {
            BigIntegerResult pageCountResult = ScalarisDataHandler.getArticleCount(model.connection);
            if (pageCountResult.success) {
                return model.formatStatisticNumber(rawNumber, pageCountResult.number);
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
            String category = MyWikiModel.createFullPageName(model.getCategoryNamespace(), parameter.trim());
            BigIntegerResult catListResult = ScalarisDataHandler.getPagesInCategoryCount(model.connection, category);
            if (catListResult.success) {
                return model.formatStatisticNumber(rawNumber, catListResult.number);
            }
//            {{NUMBERINGROUP:groupname}}
//            {{NUMINGROUP:groupname}}
//        } else if (name.equals(MAGIC_PAGES_IN_NAMESPACE) || name.equals(MAGIC_PAGES_IN_NS)) {
//            // TODO: implement
//            return null;
        }
        
        return name;
    }
}
