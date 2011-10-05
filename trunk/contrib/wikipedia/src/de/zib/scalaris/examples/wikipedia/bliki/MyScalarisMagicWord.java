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

import info.bliki.wiki.filter.MagicWord;

import java.util.ArrayList;
import java.util.List;

import de.zib.scalaris.examples.wikipedia.BigIntegerResult;
import de.zib.scalaris.examples.wikipedia.PageListResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;

/**
 * Gets values for magic words not handled by {@link MagicWord}.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyScalarisMagicWord extends MagicWord {
    // statistics
    @SuppressWarnings("javadoc")
    public static final String MAGIC_CURRENT_VERSION = "CURRENTVERSION";

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

    // page values
    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGE_NAME = "PAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_PAGE_NAME_E = "PAGENAMEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SUB_PAGE_NAME = "SUBPAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SUB_PAGE_NAME_E = "SUBPAGENAMEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_BASE_PAGE_NAME = "BASEPAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_BASE_PAGE_NAME_E = "BASEPAGENAMEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_NAMESPACE = "NAMESPACE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_NAMESPACE_E = "NAMESPACEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_FULL_PAGE_NAME = "FULLPAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_FULL_PAGE_NAME_E = "FULLPAGENAMEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_TALK_SPACE = "TALKSPACE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_TALK_SPACE_E = "TALKSPACEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SUBJECT_SPACE = "SUBJECTSPACE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SUBJECT_SPACE_E = "SUBJECTSPACEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_ARTICLE_SPACE = "ARTICLESPACE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_ARTICLE_SPACE_E = "ARTICLESPACEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_TALK_PAGE_NAME = "TALKPAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_TALK_PAGE_NAME_E = "TALKPAGENAMEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SUBJECT_PAGE_NAME = "SUBJECTPAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SUBJECT_PAGE_NAME_E = "SUBJECTPAGENAMEE";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_ARTICLE_PAGE_NAME = "ARTICLEPAGENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_ARTICLE_PAGE_NAME_E = "ARTICLEPAGENAMEE";

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
    public static final String MAGIC_SITE_NAME = "SITENAME";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SERVER = "SERVER";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SCRIPT_PATH = "SCRIPTPATH";

    @SuppressWarnings("javadoc")
    public static final String MAGIC_SERVER_NAME = "SERVERNAME";

    private static List<String> MY_MAGIC_WORDS = new ArrayList<String>();

    // private HashMap parameterValues = new HashMap();

    static {
        // statistics
        MY_MAGIC_WORDS.add(MAGIC_CURRENT_VERSION);
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
        MY_MAGIC_WORDS.add(MAGIC_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_SUB_PAGE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_SUB_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_BASE_PAGE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_BASE_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_NAMESPACE);
        MY_MAGIC_WORDS.add(MAGIC_NAMESPACE_E);
        MY_MAGIC_WORDS.add(MAGIC_FULL_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_TALK_SPACE);
        MY_MAGIC_WORDS.add(MAGIC_TALK_SPACE_E);
        MY_MAGIC_WORDS.add(MAGIC_SUBJECT_SPACE);
        MY_MAGIC_WORDS.add(MAGIC_SUBJECT_SPACE_E);
        MY_MAGIC_WORDS.add(MAGIC_ARTICLE_SPACE);
        MY_MAGIC_WORDS.add(MAGIC_ARTICLE_SPACE_E);
        MY_MAGIC_WORDS.add(MAGIC_TALK_PAGE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_TALK_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_SUBJECT_PAGE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_SUBJECT_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_ARTICLE_PAGE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_ARTICLE_PAGE_NAME_E);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_ID);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_DAY);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_DAY2);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_MONTH);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_YEAR);
        MY_MAGIC_WORDS.add(MAGIC_REVISION_TIMESTAMP);
        MY_MAGIC_WORDS.add(MAGIC_SITE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_SERVER);
        MY_MAGIC_WORDS.add(MAGIC_SCRIPT_PATH);
        MY_MAGIC_WORDS.add(MAGIC_SERVER_NAME);
    }
    
    public static boolean isMagicWord(String name) {
        return MagicWord.isMagicWord(name) || isMyMagicWord(name);
    }

    /**
     * Determines if a template name corresponds to a magic word that is handled
     * by this class instead of {@link MagicWord}.
     * 
     * @param name
     *            the template name
     * 
     * @return whether this class should be favoured over {@link MagicWord} for
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
            return MagicWord.processMagicWord(name, parameter, model);
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
         * Technical metadata / site
         */
        if (name.equals(MAGIC_SITE_NAME)) {
            return model.getNamespace().getSiteinfo().getSitename();
//        } else if (name.equals(MAGIC_SERVER)) {
//            // TODO: implement
//            return null;
//        } else if (name.equals(MAGIC_SERVER_NAME)) {
//            // TODO: implement
//            return null;
//            {{DIRMARK}}
//            {{DIRECTIONMARK}}
//        } else if (name.equals(MAGIC_SCRIPT_PATH)) {
//            // TODO: implement
//            return null;
//            {{STYLEPATH}}
        } else if (name.equals(MAGIC_CURRENT_VERSION)) {
            return WikiServlet.version;
//            {{CONTENTLANGUAGE}}
//            {{CONTENTLANG}}
        /*
         * Technical metadata / Latest revision to current page
         */
//        } else if (name.equals(MAGIC_REVISION_ID)) {
//            // TODO: implement
//            return null;
//        } else if (name.equals(MAGIC_REVISION_DAY)) {
//            // TODO: implement
//            return null;
//        } else if (name.equals(MAGIC_REVISION_DAY2)) {
//            // TODO: implement
//            return null;
//        } else if (name.equals(MAGIC_REVISION_MONTH)) {
//            // TODO: implement
//            return null;
//        } else if (name.equals(MAGIC_REVISION_YEAR)) {
//            // TODO: implement
//            return null;
//        } else if (name.equals(MAGIC_REVISION_TIMESTAMP)) {
//            // TODO: implement
//            return null;
//            {{REVISIONUSER}}
//            {{PAGESIZE:page name}}
//            {{PROTECTIONLEVEL:action}}
        /*
         * Technical metadata / Affects page content
         */
//            {{DISPLAYTITLE:title}}
//            {{DEFAULTSORT:sortkey}}
//            {{DEFAULTSORTKEY:sortkey}}
//            {{DEFAULTCATEGORYSORT:sortkey}}

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
            PageListResult catListResult = ScalarisDataHandler.getPagesInCategory(model.connection, category);
            int number = 0;
            if (catListResult.success) {
                number = catListResult.pages.size();
            }
            return model.formatStatisticNumber(rawNumber, number);
//            {{NUMBERINGROUP:groupname}}
//            {{NUMINGROUP:groupname}}
//        } else if (name.equals(MAGIC_PAGES_IN_NAMESPACE) || name.equals(MAGIC_PAGES_IN_NS)) {
//            // TODO: implement
//            return null;

        /*
         * Page names
         */
            
        } else if (name.equals(MAGIC_BASE_PAGE_NAME) || name.equals(MAGIC_BASE_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = MyWikiModel.splitNsBaseSubPage(pagename);
            return split[1];
        } else if (name.equals(MAGIC_SUB_PAGE_NAME) || name.equals(MAGIC_SUB_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = MyWikiModel.splitNsBaseSubPage(pagename);
            if (split[2].isEmpty()) {
                return split[1];
            } else {
                return split[2];
            }
        } else if (name.equals(MAGIC_SUBJECT_PAGE_NAME) || name.equals(MAGIC_SUBJECT_PAGE_NAME_E) ||
                name.equals(MAGIC_ARTICLE_PAGE_NAME) || name.equals(MAGIC_ARTICLE_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = MyWikiModel.splitNsTitle(pagename);
            String subjectSpace = model.getNamespace().getSubjectspace(split[0]);
            if (subjectSpace == null || subjectSpace.isEmpty()) {
                subjectSpace = "";
            } else {
                subjectSpace += ':';
            }
            return subjectSpace + split[1];
        } else if (name.equals(MAGIC_TALK_PAGE_NAME) || name.equals(MAGIC_TALK_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = MyWikiModel.splitNsTitle(pagename);
            if (split[0].equals(model.getNamespace().getTalk())) {
                return pagename;
            } else {
                String talkSpace = model.getNamespace().getTalkspace(split[0]);
                if (talkSpace == null) {
                    talkSpace = "";
                } else {
                    talkSpace += ':';
                }
                return talkSpace + split[1];
            }

        /*
         * Namespaces
         */

        } else if (name.equals(MAGIC_NAMESPACE) || name.equals(MAGIC_NAMESPACE_E)) {
            String pagename = getPageName(parameter, model);
            return MyWikiModel.getNamespace(pagename);
        } else if (name.equals(MAGIC_TALK_SPACE) || name.equals(MAGIC_TALK_SPACE_E)) {
            String pagename = getPageName(parameter, model);
            String pageNamespace = MyWikiModel.getNamespace(pagename);
            String talkspace = model.getNamespace().getTalkspace(pageNamespace);
            if (talkspace == null) {
                return "";
            } else {
                return talkspace;
            }
        } else if (name.equals(MAGIC_SUBJECT_SPACE) || name.equals(MAGIC_SUBJECT_SPACE_E) ||
                name.equals(MAGIC_ARTICLE_SPACE) || name.equals(MAGIC_ARTICLE_SPACE_E)) {
            String pagename = getPageName(parameter, model);
            String talkNamespace = MyWikiModel.getNamespace(pagename);
            String subjectspace = model.getNamespace().getSubjectspace(talkNamespace);
            if (subjectspace == null) {
                return "";
            } else {
                return subjectspace;
            }

        // some MediaWiki-encoded URLs -> forward to MagicWord class:
        } else if (name.equals(MAGIC_PAGE_NAME_E)) {
            return MagicWord.processMagicWord(MAGIC_PAGE_NAME, parameter, model);
        } else if (name.equals(MAGIC_FULL_PAGE_NAME_E)) {
            return MagicWord.processMagicWord(MAGIC_FULL_PAGE_NAME, parameter, model);
        } else if (name.equals(MAGIC_TALK_PAGE_NAME_E)) {
            return MagicWord.processMagicWord(MAGIC_TALK_PAGE_NAME, parameter, model);
        }
        
        return name;
    }
    
    private static String getPageName(String parameter, MyScalarisWikiModel model) {
        // parse page name to operate on:
        String pagename = parameter;
        if (pagename.isEmpty()) {
            pagename = model.getPageName();
        }
        return pagename;
    }
}
