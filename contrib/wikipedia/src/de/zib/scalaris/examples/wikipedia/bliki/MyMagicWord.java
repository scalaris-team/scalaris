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

import java.util.HashSet;

import de.zib.scalaris.examples.wikipedia.Options;

/**
 * Gets values for magic words not handled by {@link MagicWord}.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyMagicWord extends MagicWord {

    private static HashSet<String> MY_MAGIC_WORDS = new HashSet<String>(100);

    // private HashMap parameterValues = new HashMap();

    static {
        // statistics
        MY_MAGIC_WORDS.add(MAGIC_CURRENT_VERSION);
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
        MY_MAGIC_WORDS.add(MAGIC_SITE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_SERVER);
        MY_MAGIC_WORDS.add(MAGIC_SCRIPT_PATH);
        MY_MAGIC_WORDS.add(MAGIC_SERVER_NAME);
        MY_MAGIC_WORDS.add(MAGIC_DISPLAY_TITLE);
        MY_MAGIC_WORDS.add(MAGIC_DEFAULT_SORT);
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
    public static String processMagicWord(String name, String parameter, MyWikiModel model) {
        if (!isMyMagicWord(name)) {
            return MagicWord.processMagicWord(name, parameter, model);
        }
        
        // check whether numbers should be printed in raw format and
        // remove this tag from the parameter string:
        @SuppressWarnings("unused")
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
        } else if (name.equals(MAGIC_SERVER)) {
            // {{SERVER}} (//en.wikipedia.org)
            return "//" + Options.getInstance().SERVERNAME;
        } else if (name.equals(MAGIC_SERVER_NAME)) {
            // {{SERVERNAME}} (en.wikipedia.org)
            return Options.getInstance().SERVERNAME;
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
//          {{PROTECTIONLEVEL:action}}
        /*
         * Technical metadata / Affects page content / Behavior switches
         */
        } else if (name.equals(MAGIC_DISPLAY_TITLE)) {
            return "";
        } else if (name.equals(MAGIC_DEFAULT_SORT)) {
            return "";
//            {{DEFAULTSORTKEY:sortkey}}
//            {{DEFAULTCATEGORYSORT:sortkey}}

        /*
         * Page names
         */
            
        } else if (name.equals(MAGIC_BASE_PAGE_NAME) || name.equals(MAGIC_BASE_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = model.splitNsBaseSubPage(pagename);
            return split[1];
        } else if (name.equals(MAGIC_SUB_PAGE_NAME) || name.equals(MAGIC_SUB_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = model.splitNsBaseSubPage(pagename);
            if (split[2].isEmpty()) {
                return split[1];
            } else {
                return split[2];
            }
        } else if (name.equals(MAGIC_SUBJECT_PAGE_NAME) || name.equals(MAGIC_SUBJECT_PAGE_NAME_E) ||
                name.equals(MAGIC_ARTICLE_PAGE_NAME) || name.equals(MAGIC_ARTICLE_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = model.splitNsTitle(pagename);
            String subjectSpace = model.getNamespace().getContentspace(split[0]);
            if (subjectSpace == null || subjectSpace.isEmpty()) {
                subjectSpace = "";
            } else {
                subjectSpace += ':';
            }
            return subjectSpace + split[1];
        } else if (name.equals(MAGIC_TALK_PAGE_NAME) || name.equals(MAGIC_TALK_PAGE_NAME_E)) {
            String pagename = getPageName(parameter, model);
            String[] split = model.splitNsTitle(pagename);
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
            return model.getNamespace(pagename);
        } else if (name.equals(MAGIC_TALK_SPACE) || name.equals(MAGIC_TALK_SPACE_E)) {
            String pagename = getPageName(parameter, model);
            String pageNamespace = model.getNamespace(pagename);
            String talkspace = model.getNamespace().getTalkspace(pageNamespace);
            if (talkspace == null) {
                return "";
            } else {
                return talkspace;
            }
        } else if (name.equals(MAGIC_SUBJECT_SPACE) || name.equals(MAGIC_SUBJECT_SPACE_E) ||
                name.equals(MAGIC_ARTICLE_SPACE) || name.equals(MAGIC_ARTICLE_SPACE_E)) {
            String pagename = getPageName(parameter, model);
            String talkNamespace = model.getNamespace(pagename);
            String subjectspace = model.getNamespace().getContentspace(talkNamespace);
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
    
    private static String getPageName(String parameter, MyWikiModel model) {
        // parse page name to operate on:
        String pagename = parameter;
        if (pagename.isEmpty()) {
            pagename = model.getPageName();
        }
        return pagename;
    }
}
