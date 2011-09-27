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

import info.bliki.htmlcleaner.TagNode;
import info.bliki.wiki.filter.Util;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.WikiModel;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import de.zib.scalaris.Connection;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;

/**
 * Wiki model using Scalaris to fetch (new) data, e.g. templates.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyWikiModel extends WikiModel {
    protected Connection connection;
    protected Map<String, String> magicWordCache = new HashMap<String, String>();
    private String fExternalWikiBaseFullURL;

    /**
     * Interwiki links pointing to other wikis in the web
     */
    private static final String[] INTERLANGUAGE_STRINGS = { "en", "de", "fr",
            "it", "pl", "es", "ja", "ru", "nl", "pt", "sv", "zh", "ca", "uk",
            "no", "fi", "hu", "cs", "ro", "tr", "ko", "vi", "da", "ar", "eo",
            "sr", "id", "lt", "vo", "sk", "he", "fa", "bg", "sl", "eu", "war",
            "lmo", "et", "hr", "new", "te", "nn", "th", "gl", "el", "ceb",
            "simple", "ms", "ht", "bs", "bpy", "lb", "ka", "is", "sq", "la",
            "br", "hi", "az", "bn", "mk", "mr", "sh", "tl", "cy", "io", "pms",
            "lv", "ta", "su", "oc", "jv", "nap", "nds", "scn", "be", "ast",
            "ku", "wa", "af", "be-x-old", "an", "ksh", "szl", "fy", "frr",
            "yue", "ur", "ia", "ga", "yi", "sw", "als", "hy", "am", "roa-rup",
            "map-bms", "bh", "co", "cv", "dv", "nds-nl", "fo", "fur", "glk",
            "gu", "ilo", "kn", "pam", "csb", "kk", "km", "lij", "li", "ml",
            "gv", "mi", "mt", "nah", "ne", "nrm", "se", "nov", "qu", "os",
            "pi", "pag", "ps", "pdc", "rm", "bat-smg", "sa", "gd", "sco", "sc",
            "si", "tg", "roa-tara", "tt", "to", "tk", "hsb", "uz", "vec",
            "fiu-vro", "wuu", "vls", "yo", "diq", "zh-min-nan", "zh-classical",
            "frp", "lad", "bar", "bcl", "kw", "mn", "haw", "ang", "ln", "ie",
            "wo", "tpi", "ty", "crh", "jbo", "ay", "zea", "eml", "ky", "ig",
            "or", "mg", "cbk-zam", "kg", "arc", "rmy", "gn", "mo (closed)",
            "so", "kab", "ks", "stq", "ce", "udm", "mzn", "pap", "cu", "sah",
            "tet", "sd", "lo", "ba", "pnb", "iu", "na", "got", "bo", "dsb",
            "chr", "cdo", "hak", "om", "my", "sm", "ee", "pcd", "ug", "as",
            "ti", "av", "bm", "zu", "pnt", "nv", "cr", "pih", "ss", "ve", "bi",
            "rw", "ch", "arz", "xh", "kl", "ik", "bug", "dz", "ts", "tn", "kv",
            "tum", "xal", "st", "tw", "bxr", "ak", "ab", "ny", "fj", "lbe",
            "ki", "za", "ff", "lg", "sn", "ha", "sg", "ii", "cho", "rn", "mh",
            "chy", "ng", "kj", "ho", "mus", "kr", "hz", "mwl", "pa" };
    
    protected static final Set<String> INTERLANGUAGE_KEYS;
    
    static {
        // BEWARE: fields in Configuration are static -> this changes all configurations!
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("fullurl", MyFullurl.CONST);
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("localurl", MyLocalurl.CONST);
        
        // add missing hsb interlanguage link:
        Configuration.DEFAULT_CONFIGURATION.addInterwikiLink("hsb", "http://hsb.wiktionary.org/wiki/?${title}");
        
        // allow style attributes:
        TagNode.addAllowedAttribute("style");
        
        // create set of keys for interlanguage wiki links
        INTERLANGUAGE_KEYS = new HashSet<String>(INTERLANGUAGE_STRINGS.length);
        for (String lang : INTERLANGUAGE_STRINGS) {
            INTERLANGUAGE_KEYS.add(lang);
        }
    }
    
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
    public MyWikiModel(String imageBaseURL, String linkBaseURL, Connection connection, MyNamespace namespace) {
        super(new MyConfiguration(namespace), null, namespace, imageBaseURL, linkBaseURL);
        this.connection = connection;
        this.fExternalWikiBaseFullURL = linkBaseURL;
    }
    
    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#getRawWikiContent(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    public String getRawWikiContent(String namespace, String articleName,
            Map<String, String> templateParameters) {
        if (isTemplateNamespace(namespace)) {
            String magicWord = articleName;
            String parameter = "";
            int index = magicWord.indexOf(':');
            if (index > 0) {
                parameter = magicWord.substring(index + 1).trim();
                magicWord = magicWord.substring(0, index);
            }
            if (MyMagicWord.isMagicWord(magicWord)) {
                // cache values for magic words:
                if (magicWordCache.containsKey(articleName)) {
                    return magicWordCache.get(articleName);
                } else {
                    String value = MyMagicWord.processMagicWord(magicWord, parameter, this);
                    magicWordCache.put(articleName, value);
                    return value;
                }
            } else {
                // retrieve template from Scalaris:
                // note: templates are already cached, no need to cache them here
                if (connection != null) {
                    // (ugly) fix for template parameter replacement if no parameters given,
                    // e.g. "{{noun}}" in the simple English Wiktionary
                    if (templateParameters.isEmpty()) {
                        templateParameters.put("", null);
                    }
                    String pageName = getTemplateNamespace() + ":" + articleName;
                    RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
                    if (getRevResult.success) {
                        String text = getRevResult.revision.getText();
                        text = removeNoIncludeContents(text);
                        return text;
                    } else {
//                        System.err.println(getRevResult.message);
//                        return "<b>ERROR: template " + pageName + " not available: " + getRevResult.message + "</b>";
                        /*
                         * the template was not found and will never be - assume
                         * an empty content instead of letting the model try
                         * again (which is what it does if null is returned)
                         */
                        return "";
                    }
                }
            }
        }
        
        if (getRedirectLink() != null) {
            // requesting a page from a redirect?
            return getRedirectContent(getRedirectLink());
        }
//        System.out.println("getRawWikiContent(" + namespace + ", " + articleName + ", " +
//            templateParameters + ")");
        return null;
    }
    
    /**
     * Fixes noinclude tags inside includeonly/onlyinclude not being filtered
     * out by the template parsing.
     * 
     * @param text
     *            the template's text
     * 
     * @return the text without anything within noinclude tags
     */
    private final String removeNoIncludeContents(String text) {
        // do not alter if showing the template page: 
        if (getRecursionLevel() == 0) {
            return text;
        }
        
        StringBuilder sb = new StringBuilder(text.length());
        int curPos = 0;
        while(true) {
            // starting tag:
            String startString = "<", endString = "noinclude>";
            int index = Util.indexOfIgnoreCase(text, startString, endString, curPos);
            if (index != (-1)) {
                sb.append(text.substring(curPos, index));
                curPos = index;
            } else {
                sb.append(text.substring(curPos));
                return sb.toString();
            }

            // ending tag:
            startString = "</"; endString = "noinclude>";
            index = Util.indexOfIgnoreCase(text, startString, endString, curPos);
            if (index != (-1)) {
                curPos = index + startString.length() + endString.length();
            } else {
                return sb.toString();
            }
        }
    }
    
    /**
     * Gets the contents of the newest revision of the page redirected to.
     * 
     * @param pageName
     *            the name of the page redirected to
     * 
     * @return the contents of the newest revision of that page or a placeholder
     *         string
     */
    public String getRedirectContent(String pageName) {
        RevisionResult getRevResult = ScalarisDataHandler.getRevision(connection, pageName);
        if (getRevResult.success) {
            // make PAGENAME in the redirected content work as expected
            setPageName(pageName);
            return getRevResult.revision.getText();
        } else {
//            System.err.println(getRevResult.message);
//            return "<b>ERROR: redirect to " + getRedirectLink() + " failed: " + getRevResult.message + "</b>";
            return "&#35;redirect [[" + pageName + "]]";
        }
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#encodeTitleToUrl(java.lang.String, boolean)
     */
    @Override
    public String encodeTitleToUrl(String wikiTitle,
            boolean firstCharacterAsUpperCase) {
        try {
            // some links may contain '_' which needs to be translated back to ' ':
            wikiTitle = wikiTitle.replace('_', ' ');
            return URLEncoder.encode(wikiTitle, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            return super.encodeTitleToUrl(wikiTitle, firstCharacterAsUpperCase);
        }
    }
    
    /**
     * Gets the base URL for images (can contain ${image} for replacement).
     * 
     * @return the base url for images
     */
    public String getImageBaseURL() {
        return fExternalImageBaseURL;
    }

    /**
     * Gets the base URL for links (can contain ${title} for replacement).
     * 
     * @return the base url for links
     */
    public String getLinkBaseURL() {
        return fExternalWikiBaseURL;
    }

    /**
     * Gets the base URL for full links including "http://" (can contain ${title}
     * for replacement).
     * 
     * @return the base url for links
     */
    public String getLinkBaseFullURL() {
        return fExternalWikiBaseFullURL;
    }

    /**
     * Sets the base URL for full links including "http://" (can contain ${title}
     * for replacement).
     * 
     * @param linkBaseFullURL
     *            the full link URL to set
     */
    public void setLinkBaseFullURL(String linkBaseFullURL) {
        this.fExternalWikiBaseFullURL = linkBaseFullURL;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#getNamespace()
     */
    @Override
    public MyNamespace getNamespace() {
        return (MyNamespace) super.getNamespace();
    }
    
    /**
     * Formats the given number using the wiki's locale.
     * 
     * Note: Currently, the English locale is always used.
     * 
     * @param rawNumber
     *            whether the raw number should be returned
     * @param number
     *            the number
     * 
     * @return the formatted number
     */
    public String formatStatisticNumber(boolean rawNumber, Number number) {
        if (rawNumber) {
            return number.toString();
        } else {
            // TODO: use locale from Wiki
            NumberFormat nf = NumberFormat.getNumberInstance(Locale.ENGLISH);
            nf.setGroupingUsed(true);
            return nf.format(number);
        }
    }

    /**
     * Splits the given full title into its namespace and page title components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * 
     * @return a 2-element array with the namespace (index 0) and the page title
     *         (index 1)
     */
    public static String[] splitNsTitle(String fullTitle) {
        int colonIndex = fullTitle.indexOf(':');
        if (colonIndex != (-1)) {
            return new String[] { fullTitle.substring(0, colonIndex),
                    fullTitle.substring(colonIndex + 1) };
        }
        return new String[] {"", fullTitle};
    }

    /**
     * Returns the namespace of a given page title.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * 
     * @return the namespace part of the title or an empty string if no
     *         namespace
     * 
     * @see #getTitleName(String)
     * @see #splitNsTitle(String)
     */
    public static String getNamespace(String title) {
        return splitNsTitle(title)[0];
    }

    /**
     * Returns the name of a given page title without its namespace.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * 
     * @return the title part of the page title
     * 
     * @see #getNamespace(String)
     * @see #splitNsTitle(String)
     */
    public static String getTitleName(String title) {
        return splitNsTitle(title)[1];
    }
    
    /**
     * Splits the given full title into its namespace, base and sub page
     * components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * 
     * @return a 3-element array with the namespace (index 0), the base page
     *         (index 1) and the sub page (index 2)
     */
    public static String[] splitNsBaseSubPage(String fullTitle) {
        String[] split1 = splitNsTitle(fullTitle);
        String namespace = split1[0];
        String title = split1[1];
        int colonIndex = title.lastIndexOf('/');
        if (colonIndex != (-1)) {
            return new String[] { namespace, title.substring(0, colonIndex),
                    title.substring(colonIndex + 1) };
        }
        return new String[] {namespace, title, ""};
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#appendRedirectLink(java.lang.String)
     */
    @Override
    public boolean appendRedirectLink(String redirectLink) {
        // do not add redirection if we are parsing a template:
        if (getRecursionLevel() == 0) {
            return super.appendRedirectLink(redirectLink);
        }
        return true;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#appendInterWikiLink(java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public void appendInterWikiLink(String namespace, String title, String linkText) {
        appendInterWikiLink(namespace, title, linkText, true);
    }
    
    public void appendInterWikiLink(String namespace, String title, String linkText, boolean ignoreInterLang) {
        if (INTERLANGUAGE_KEYS.contains(namespace)) {
            // also check if this is an inter wiki link to an external wiki in another language
            // -> only ignore inter language links to the same wiki
            String namespace2 = getNamespace(title);
            if (!ignoreInterLang || (!namespace2.isEmpty() && isInterWiki(namespace2))) {
                // bliki is not able to parse language-specific interwiki links
                // -> use default language
                super.appendInterWikiLink(namespace2, title, linkText);
            } else {
                // ignore interlanguage keys
            }
        } else {
            super.appendInterWikiLink(namespace, title, linkText);
        }
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#addLink(java.lang.String)
     */
    @Override
    public void addLink(String topicName) {
        /*
         * to not add links like [[:w:nl:User:WinContro|Dutch Wikipedia]] to
         * the internal links
         */
        String namespace = getNamespace(topicName);
        if (namespace.isEmpty() || !isInterWiki(namespace)) {
            super.addLink(topicName);
        }
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#appendInternalLink(java.lang.String, java.lang.String, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public void appendInternalLink(String topic, String hashSection, String topicDescription, String cssClass, boolean parseRecursive) {
        /*
         * convert links like [[:w:nl:User:WinContro|Dutch Wikipedia]] to
         * external links if the link is an interwiki link
         */
        String[] nsTitle = splitNsTitle(topic);
        if (!nsTitle[0].isEmpty() && isInterWiki(nsTitle[0])) {
            appendInterWikiLink(nsTitle[0], nsTitle[1], topicDescription, false);
        } else {
            super.appendInternalLink(topic, hashSection, topicDescription, cssClass, parseRecursive);
        }
    }
}
