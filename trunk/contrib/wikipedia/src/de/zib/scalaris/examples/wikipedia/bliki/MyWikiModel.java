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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Wiki model fixing some bugs of {@link WikiModel} and adding some
 * functionality.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyWikiModel extends WikiModel {

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
            "chy", "ng", "kj", "ho", "mus", "kr", "hz", "mwl", "pa", "ace",
            "bat-smg", "bjn", "cbk-zam", "cdo", "ceb", "crh", "dsb", "eml",
            "ext", "fiu-vro", "frp", "frr", "gag", "gan", "hak", "kaa", "kab",
            "kbd", "koi", "krc", "ksh", "lad", "lbe", "lmo", "map-bms", "mdf",
            "mrj", "mwl", "nap", "nds-nl", "nrm", "pcd", "pfl", "pih", "pnb", "rmy",
            "roa-tara", "rue", "sah", "sco", "stq", "szl", "udm",
            "war", "wuu", "xmf", "zea", "zh-classical", "zh-yue",
            "ckb", "hif", "mhr", "myv", "srn"};
    
    protected static final Set<String> INTERLANGUAGE_KEYS;
    
    /**
     * Cache of processed magic words.
     */
    protected Map<String, String> magicWordCache = new HashMap<String, String>();

    protected Set<String> includes = new HashSet<String>();

    protected Map<String, List<Long>> stats = new LinkedHashMap<String, List<Long>>();

    static {
        // BEWARE: fields in Configuration are static -> this changes all configurations!
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("fullurl", MyFullurl.CONST);
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("localurl", MyLocalurl.CONST);
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("#time", MyTimeTemplateFun.CONST);
        
        // allow style attributes:
        TagNode.addAllowedAttribute("style");
        
        // create set of keys for interlanguage wiki links
        // also add missing hsb interlanguage link:
        Map<String, String> interWikiMap = Configuration.DEFAULT_CONFIGURATION.getInterwikiMap();
        INTERLANGUAGE_KEYS = new HashSet<String>(INTERLANGUAGE_STRINGS.length);
        for (String lang : INTERLANGUAGE_STRINGS) {
            INTERLANGUAGE_KEYS.add(lang);
            // if there is no interwiki link for it, create one and guess the URL:
            if (!interWikiMap.containsKey(lang)) {
                Configuration.DEFAULT_CONFIGURATION.addInterwikiLink(lang, "http://" + lang + ".wiktionary.org/wiki/?${title}");
            }
        }
    }
    
    /**
     * Creates a new wiki model to render wiki text fixing some bugs of
     * {@link WikiModel}.
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
    public MyWikiModel(String imageBaseURL, String linkBaseURL, MyNamespace namespace) {
        super(new MyConfiguration(namespace), null, namespace, imageBaseURL, linkBaseURL);
        this.fExternalWikiBaseFullURL = linkBaseURL;
    }
    
    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#getRawWikiContent(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    public String getRawWikiContent(String namespace, String articleName,
            Map<String, String> templateParameters) {
        // Remove article from templates and add it by our conditions (see below).
        // (was added by AbstractWikiModel#substituteTemplateCall)
        templates.remove(articleName);
        /* AbstractWikiModel#substituteTemplateCall sets the template
         * namespace even for texts like "{{MediaWiki:Noarticletext NS Other}}"
         * or "{{:Main Page/Introduction}}" which transcludes the page but does
         * not need "Template:" prepended 
         */
        String namespace2;
        String articleName2;
        String processedMagicWord = null;
        int index = articleName.indexOf(':');
        if (index > 0) {
            namespace2 = articleName.substring(0, index);
            articleName2 = articleName.substring(index + 1).trim();
            if (isTemplateNamespace(namespace)) {
                // if it is a magic word, the first part is the word itself, the second its parameters
                processedMagicWord = getMagicWord(articleName, namespace2, articleName2);
            }
        } else {
            namespace2 = namespace;
            articleName2 = articleName;
            if (isTemplateNamespace(namespace)) {
                // if it is a magic word, there are no parameters
                processedMagicWord = getMagicWord(articleName, articleName2, "");
            }
        }

        if (processedMagicWord != null) {
            return processedMagicWord;
        }
        
        if (getRedirectLink() != null) {
            // requesting a page from a redirect?
            return getRedirectContent(getRedirectLink());
        }
        
        // set the corrected namespace and article name (see above)
        namespace = namespace2;
        articleName = articleName2;
        if (isTemplateNamespace(namespace)) {
            addTemplate(articleName);
        } else {
            addInclude(createFullPageName(namespace, articleName));
        }
        
        // note: cannot cache templates here since the implementation-specific
        // retrievePage() method may depend in the exact parameters or not

        // (ugly) fix for template parameter replacement if no parameters given,
        // e.g. "{{noun}}" in the simple English Wiktionary
        if (templateParameters != null && templateParameters.isEmpty()) {
            templateParameters.put("", null);
        }

        String text = retrievePage(namespace, articleName, templateParameters);
        if (text != null && !text.isEmpty()) {
            text = removeNoIncludeContents(text);
        }
        return text;
    }

    /**
     * Checks whether the given template name is a magic word and if this is the
     * case, processes it and returns its value.
     * 
     * Retrieves magic word contents using
     * {@link #retrieveMagicWord(String, String, String)} and caches the
     * contents in {@link #magicWordCache}.
     * 
     * @param templateName
     *            the template's name without the namespace, e.g. a magic word
     *            including its parameters
     * 
     * @return the contents of the magic word or <tt>null</tt> if the template
     *         is no magic word
     */
    private String getMagicWord(String templateName, String magicWord, String parameter) {
        if (isMagicWord(magicWord)) {
            // cache values for magic words:
            if (magicWordCache.containsKey(templateName)) {
                return magicWordCache.get(templateName);
            } else {
                String value = retrieveMagicWord(templateName, magicWord, parameter);
                magicWordCache.put(templateName, value);
                return value;
            }
        } else {
            return null;
        }
    }

    /**
     * Determines if a template name corresponds to a magic word using
     * {@link MyMagicWord#isMagicWord(String)}.
     * 
     * @param name
     *            the template name
     * 
     * @return whether the template is a magic word or not
     */
    protected boolean isMagicWord(String name) {
        return MyMagicWord.isMagicWord(name);
    }
    
    /**
     * Retrieves the contents of the given magic word using
     * {@link MyMagicWord#processMagicWord(String, String, info.bliki.wiki.model.IWikiModel)}.
     * 
     * @param templateName
     *            the template's name without the namespace, e.g. a magic word
     *            including its parameters
     * @param magicWord
     *            the magic word alone
     * @param parameter
     *            the parameters of the magic word
     * 
     * @return the contents of the magic word
     */
    protected String retrieveMagicWord(String templateName, String magicWord,
            String parameter) {
        return MyMagicWord.processMagicWord(templateName, parameter, this);
    }
    
    /**
     * Gets the contents of the newest revision of the page redirected to
     * (override in sub-classes!).
     * 
     * @param pageName
     *            the name of the page redirected to
     * 
     * @return a placeholder string for the redirect
     */
    public String getRedirectContent(String pageName) {
        return "&#35;redirect [[" + pageName + "]]";
    }
    
    /**
     * Creates the full pagename including the namespace (if non-empty).
     * 
     * @param namespace
     *            the namespace of a page
     * @param articleName
     *            the name of a page
     * 
     * @return the full name of the page
     */
    public static String createFullPageName(String namespace, String articleName) {
        String pageName;
        if (namespace.isEmpty()) {
            pageName = articleName;
        } else {
            pageName = namespace + ":" + articleName;
        }
        return pageName;
    }
    
    /**
     * Retrieves the contents of the given page (override in sub-classes!).
     * 
     * @param namespace
     *            the namespace of the page
     * @param articleName
     *            the page's name without the namespace
     * @param templateParameters
     *            template parameters if the page is a template, <tt>null</tt>
     *            otherwise
     * 
     * @return <tt>null</tt>
     */
    protected String retrievePage(String namespace, String articleName,
            Map<String, String> templateParameters) {
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
    protected final String removeNoIncludeContents(String text) {
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

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#encodeTitleToUrl(java.lang.String, boolean)
     */
    @Override
    public String encodeTitleToUrl(String wikiTitle, boolean firstCharacterAsUpperCase) {
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
    public String[] splitNsTitle(String fullTitle) {
        return splitNsTitle(fullTitle, getNamespace());
    }

    /**
     * Splits the given full title into its namespace and page title components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return a 2-element array with the namespace (index 0) and the page title
     *         (index 1)
     */
    public static String[] splitNsTitle(String fullTitle, final MyNamespace nsObject) {
        return splitNsTitle(fullTitle, nsObject, true);
    }

    /**
     * Splits the given full title into its namespace and page title components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * @param nsObject
     *            the namespace for determining how to split the title
     * @param onlyValidNs
     *            whether only valid namespaces should be split off
     * 
     * @return a 2-element array with the namespace (index 0) and the page title
     *         (index 1)
     */
    private static String[] splitNsTitle(String fullTitle, final MyNamespace nsObject, boolean onlyValidNs) {
        int colonIndex = fullTitle.indexOf(':');
        if (colonIndex != (-1)) {
            String maybeNs = fullTitle.substring(0, colonIndex);
            if (!onlyValidNs || nsObject.getNumberByName(maybeNs) != null) {
                // this is a real namespace
                return new String[] { maybeNs,
                        fullTitle.substring(colonIndex + 1) };
            } else {
                // page belongs to the main namespace and only contains a colon
                return new String[] { "", fullTitle };
            }
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
     * @see #getTitleName(String, MyNamespace)
     * @see #splitNsTitle(String, MyNamespace)
     */
    public String getNamespace(String title) {
        return getNamespace(title, getNamespace());
    }

    /**
     * Returns the namespace of a given page title.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return the namespace part of the title or an empty string if no
     *         namespace
     * 
     * @see #getTitleName(String, MyNamespace)
     * @see #splitNsTitle(String, MyNamespace)
     */
    public static String getNamespace(String title, final MyNamespace nsObject) {
        return splitNsTitle(title, nsObject)[0];
    }

    /**
     * Returns the name of a given page title without its namespace.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * 
     * @return the title part of the page title
     * 
     * @see #getNamespace(String, MyNamespace)
     * @see #splitNsTitle(String, MyNamespace)
     */
    public String getTitleName(String title) {
        return getTitleName(title, getNamespace());
    }

    /**
     * Returns the name of a given page title without its namespace.
     * 
     * @param title
     *            the (full) title including a namespace (if present)
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return the title part of the page title
     * 
     * @see #getNamespace(String, MyNamespace)
     * @see #splitNsTitle(String, MyNamespace)
     */
    public static String getTitleName(String title, final MyNamespace nsObject) {
        return splitNsTitle(title, nsObject)[1];
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
    public String[] splitNsBaseSubPage(String fullTitle) {
        return splitNsBaseSubPage(fullTitle, getNamespace());
    }

    /**
     * Splits the given full title into its namespace, base and sub page
     * components.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return a 3-element array with the namespace (index 0), the base page
     *         (index 1) and the sub page (index 2)
     */
    public static String[] splitNsBaseSubPage(String fullTitle, final MyNamespace nsObject) {
        String[] split1 = splitNsTitle(fullTitle, nsObject);
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
    
    protected void appendInterWikiLink(String namespace, String title, String linkText, boolean ignoreInterLang) {
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
    public void appendInternalLink(String topic, String hashSection, String topicDescription,
            String cssClass, boolean parseRecursive) {
        /*
         * convert links like [[:w:nl:User:WinContro|Dutch Wikipedia]] to
         * external links if the link is an interwiki link
         */
        String[] nsTitle = splitNsTitle(topic);
        if (!nsTitle[0].isEmpty() && isInterWiki(nsTitle[0])) {
            appendInterWikiLink(nsTitle[0], nsTitle[1], topicDescription, nsTitle[1].isEmpty() && topicDescription.equals(topic));
        } else {
            super.appendInternalLink(topic, hashSection, topicDescription, cssClass, parseRecursive);
        }
    }

    /**
     * Normalises the given string, i.e. capitalises the first letter and
     * replaces underscores with spaces.
     * 
     * @param value
     *            the string
     * 
     * @return a normalised string
     */
    private static String normaliseName(final String value) {
        StringBuilder sb = new StringBuilder(value.length());
        boolean whiteSpace = true;
        boolean first = true;
        for (int i = 0; i < value.length(); ++i) {
            char c = value.charAt(i);
            switch (c) {
                case ' ':
                case '_':
                    if (!whiteSpace) {
                        sb.append(' ');
                    }
                    whiteSpace = true;
                    break;
                default:
                    if (first) {
                        sb.append(Character.toUpperCase(c));
                        first = false;
                    } else {
                        sb.append(c);
                    }
                    whiteSpace = false;
                    break;
            }
        }
        return sb.toString().trim();
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param title
     *            the original page title
     * 
     * @return the normalised page title
     */
    public String normalisePageTitle(final String title) {
        return normalisePageTitle(title, getNamespace());
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param title
     *            the original page title
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return the normalised page title
     */
    public static String normalisePageTitle(final String title, final MyNamespace nsObject) {
        String[] parts = splitNsTitle(title, nsObject);
        return createFullPageName(
                nsObject.getNumberByName(parts[0]).toString(),
                normaliseName(parts[1]));
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param <T>
     * 
     * @param titles
     *            the original page titles
     * @param normalisedTitles
     *            the container to write the normalised titles to
     * 
     * @return the normalised page titles
     */
    public <T extends Collection<String>> T normalisePageTitles(final T titles, T normalisedTitles) {
        return MyWikiModel.<T>normalisePageTitles(titles, getNamespace(), normalisedTitles);
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param <T>
     * 
     * @param titles
     *            the original page titles
     * @param nsObject
     *            the namespace for determining how to split the title
     * @param normalisedTitles
     *            the container to write the normalised titles to
     * 
     * @return the normalised page titles
     */
    public static <T extends Collection<String>> T normalisePageTitles(final Collection<String> titles, final MyNamespace nsObject, T normalisedTitles) {
        for (String title: titles) {
            normalisedTitles.add(MyWikiModel.normalisePageTitle(title, nsObject));
        }
        return normalisedTitles;
    }
    
    /**
     * De-normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param title
     *            the normalised page title
     * 
     * @return the original page title
     */
    public String denormalisePageTitle(final String title) {
        return denormalisePageTitle(title, getNamespace());
    }
    
    /**
     * De-normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param title
     *            the normalised page title
     * @param nsObject
     *            the namespace for determining how to split the title
     * 
     * @return the original page title
     */
    public static String denormalisePageTitle(final String title, final MyNamespace nsObject) {
        String[] parts = splitNsTitle(title, nsObject, false);
        return createFullPageName(
                nsObject.getNamespaceByNumber(Integer.parseInt(parts[0])),
                normaliseName(parts[1]));
    }
    
    /**
     * De-normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param <T>
     * 
     * @param titles
     *            the normalised page titles
     * @param denormalisedTitles
     *            the container to write the de-normalised titles to
     * 
     * @return the original page title
     */
    public <T extends Collection<String>> T denormalisePageTitles(final T titles, T denormalisedTitles) {
        return MyWikiModel.<T>denormalisePageTitles(titles, getNamespace(), denormalisedTitles);
    }
    
    /**
     * De-normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param <T>
     * 
     * @param titles
     *            the normalised page titles
     * @param nsObject
     *            the namespace for determining how to split the title
     * @param denormalisedTitles
     *            the container to write the de-normalised titles to
     * 
     * @return the original page title
     */
    public static <T extends Collection<String>> T denormalisePageTitles(final T titles, final MyNamespace nsObject, T denormalisedTitles) {
        for (String title: titles) {
            denormalisedTitles.add(MyWikiModel.denormalisePageTitle(title, nsObject));
        }
        return denormalisedTitles;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#setUp()
     */
    @Override
    public void setUp() {
        // WikiModel forgets to reset fRedirectLink
        super.setUp();
        fRedirectLink = null;
        magicWordCache = new HashMap<String, String>();
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#isTemplateTopic()
     */
    @Override
    public boolean isTemplateTopic() {
        // best to declare every page as a template topic
        // -> e.g. does not remove <noinclude> tags in ordinary pages 
        return true;
//        return isTemplateNamespace(getNamespace(getPageName()));
    }

    /**
     * Adds an inclusion to the currently parsed page.
     * 
     * @param includedName
     *            the name of the article being included
     */
    public void addInclude(String includedName) {
        includes.add(includedName);
    }

    /**
     * @return the references
     */
    public Set<String> getIncludes() {
        return includes;
    }

    /**
     * Gets information about the time needed to look up pages.
     * 
     * @return a mapping of page titles to retrieval times
     */
    public Map<String, List<Long>> getStats() {
        return stats;
    }

    /**
     * Adds the time needed to retrieve the given page to the collected
     * statistics.
     * 
     * @param title
     *            the title of the page
     * @param value
     *            the number of milliseconds it took to retrieve the page
     */
    public void addStat(String title, long value) {
        List<Long> l = stats.get(title);
        if (l == null) {
            stats.put(title, l = new ArrayList<Long>(2));
        }
        l.add(value);
    }
    
    
}
