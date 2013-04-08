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
        MY_MAGIC_WORDS.add(MAGIC_PAGE_SIZE);
        MY_MAGIC_WORDS.add(MAGIC_SITE_NAME);
        MY_MAGIC_WORDS.add(MAGIC_SERVER);
        MY_MAGIC_WORDS.add(MAGIC_SCRIPT_PATH);
        MY_MAGIC_WORDS.add(MAGIC_SERVER_NAME);
        MY_MAGIC_WORDS.add(MAGIC_DISPLAY_TITLE);
        MY_MAGIC_WORDS.add(MAGIC_DEFAULT_SORT);
    }

    /**
     * Determines if a template name corresponds to a magic word (does not
     * recognise magic words with parameters, e.g. <tt>TALKPAGENAME:Title</tt> -
     * use {@link #extractMagicWordPart(String)} for a full title and check for
     * magic word with its result).
     * 
     * @param name
     *            the potential magic word
     * @return <tt>true</tt> if <tt>name</tt> was a magic word, <tt>false</tt>
     *         otherwise
     */
    public static boolean isMagicWord(String name) {
        return MagicWord.isMagicWord(name) || isMyMagicWord(name);
    }

    /**
     * Extract the potential magic word part from a given title name.
     * 
     * @param title
     *            the template's name without the namespace, e.g. a magic word
     *            including its parameters
     * 
     * @return the magic word or another string
     *
     * @see MyWikiModel#processMagicWord(String)
     * @see #isMagicWord(String)
     */
    public static String extractMagicWordPart(String title) {
        int index = title.indexOf(':');
        String magicWord = title;
        if (index > 0) {
            // if it is a magic word, the first part is the word itself, the second its parameters
            magicWord = title.substring(0, index);
        }
        return magicWord;
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
     * @param name
     *            the template name, i.e. a magic word
     * @param parameter
     *            the template parameters
     * @param model
     *            the currently used model
     * @param hasParameter
     *            whether a parameter was given or not (cannot distinguish from
     *            <tt>parameter</tt> value alone)
     * 
     * @return the value of the magic word
     * 
     * @see <a
     *      href="http://meta.wikimedia.org/wiki/Help:Magic_words">http://meta.wikimedia.org/wiki/Help:Magic_words</a>
     */
    public static String processMagicWord(String name, String parameter, MyWikiModel model, boolean hasParameter) {
        if (!isMyMagicWord(name)) {
            return MagicWord.processMagicWord(name, parameter, model, hasParameter);
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
         * Technical metadata / Latest revision to current page
         */
        } else if (name.equals(MAGIC_PAGE_SIZE)) {
            final String revText = model.retrievePage(parameter, null, false);
            int size = 0;
            if (revText != null) {
                size = revText.getBytes().length;
            }
            return model.formatStatisticNumber(rawNumber, size);
        }
        
        return name;
    }
}
