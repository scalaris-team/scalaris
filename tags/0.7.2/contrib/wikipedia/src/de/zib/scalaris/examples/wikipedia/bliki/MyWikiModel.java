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

import info.bliki.htmlcleaner.TagNode;
import info.bliki.wiki.filter.Encoder;
import info.bliki.wiki.filter.HTMLConverter;
import info.bliki.wiki.filter.ITextConverter;
import info.bliki.wiki.model.Configuration;
import info.bliki.wiki.model.WikiModel;
import info.bliki.wiki.namespaces.Namespace;
import info.bliki.wiki.tags.IgnoreTag;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;

import de.zib.scalaris.examples.wikipedia.InvolvedKey;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.tools.LinkedMultiHashMap;

/**
 * Wiki model fixing some bugs of {@link WikiModel} and adding some
 * functionality.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyWikiModel extends WikiModel {

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
     * Localised prefixes for special pages, e.g. "Special". 
     */
    public static final Map<String, String> SPECIAL_PREFIX = new HashMap<String, String>();
    /**
     * Localised suffixes for special pages, e.g. "AllPages".
     */
    public static final Map<String, EnumMap<SpecialPage, String>> SPECIAL_SUFFIX = new HashMap<String, EnumMap<SpecialPage, String>>();

    /**
     * Enum for all available special pages.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static enum SpecialPage {
        /**
         * Redirect to random page.
         */
        SPECIAL_RANDOM,
        /**
         * List of all pages.
         */
        SPECIAL_ALLPAGES,
        /**
         * List of all pages with a given prefix.
         */
        SPECIAL_PREFIXINDEX,
        /**
         * Search for a page.
         */
        SPECIAL_SEARCH,
        /**
         * Pages linking to another page.
         */
        SPECIAL_WHATLINKSHERE,
        /**
         * List of available special pages.
         */
        SPECIAL_SPECIALPAGES,
        /**
         * Some statistics.
         */
        SPECIAL_STATS,
        /**
         * Version information.
         */
        SPECIAL_VERSION;
    }
    
    /**
     * Cache of processed magic words.
     */
    protected Map<String, String> magicWordCache = new HashMap<String, String>();

    protected LinkedMultiHashMap<String, Long> stats = new LinkedMultiHashMap<String, Long>();
    
    /**
     * All keys that have been read or written during the current operation.
     */
    protected final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
    
    protected ExistingPagesCache existingPages = ExistingPagesCache.NULL_CACHE;

    protected Map<NormalisedTitle, String> pageCache = new HashMap<NormalisedTitle, String>();

    /**
     * Text of the page to render, i.e. given to
     * {@link #renderPageWithCache(String)} or
     * {@link #renderPageWithCache(ITextConverter, String)}.
     */
    private String renderWikiText = null;
    
    protected static final Pattern MATCH_WIKI_FORBIDDEN_TITLE_CHARS =
            Pattern.compile("^.*?([\\p{Cc}\\p{Cn}\\p{Co}#<>\\[\\]|{}\\n\\r]).*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    /**
     * Pattern to check whether a wikitext is redirecting or not.
     */
    public static final Pattern MATCH_WIKI_REDIRECT = Pattern.compile("^\\s*#REDIRECT[ ]?\\[\\[:?([^\\]#]*)[^\\]]*\\]\\].*$", Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

    static {
        // BEWARE: fields in Configuration are static -> this changes all configurations!
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("fullurl", MyFullurl.CONST);
        Configuration.DEFAULT_CONFIGURATION.addTemplateFunction("localurl", MyLocalurl.CONST);
        
        // do not put these into the HTML text (they are not rendered anyway)
        Configuration.DEFAULT_CONFIGURATION.addTokenTag("inputbox", new IgnoreTag("inputbox"));
        Configuration.DEFAULT_CONFIGURATION.addTokenTag("imagemap", new IgnoreTag("imagemap"));
        Configuration.DEFAULT_CONFIGURATION.addTokenTag("timeline", new IgnoreTag("timeline"));
        
        Configuration.AVOID_PAGE_BREAK_IN_TABLE = false;
        
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
        
        // localised special pages titles (prefix + suffix)
        // BEWARE: keep SPECIAL_PREFIX and SPECIAL_SUFFIX in sync!
        SPECIAL_PREFIX.put("en", "Special");
        SPECIAL_PREFIX.put("simple", "Special");
        SPECIAL_PREFIX.put("de", "Spezial");
        SPECIAL_PREFIX.put("bar", "Spezial");
        SPECIAL_PREFIX.put("es", "Especial");
        SPECIAL_PREFIX.put("pl", "Specjalna");
        // BEWARE: include normalised page titles!

        EnumMap<SpecialPage, String> SPECIAL_SUFFIX_EN = new EnumMap<SpecialPage, String>(SpecialPage.class);
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_RANDOM, "Random");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_ALLPAGES, "AllPages");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_PREFIXINDEX, "PrefixIndex");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_SEARCH, "Search");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_WHATLINKSHERE, "WhatLinksHere");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_SPECIALPAGES, "SpecialPages");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_STATS, "Statistics");
        SPECIAL_SUFFIX_EN.put(SpecialPage.SPECIAL_VERSION, "Version");

        EnumMap<SpecialPage, String> SPECIAL_SUFFIX_DE = new EnumMap<SpecialPage, String>(SpecialPage.class);
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_RANDOM, "Zufällige Seite");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_ALLPAGES, "Alle Seiten");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_PREFIXINDEX, "Präfixindex");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_SEARCH, "Suche");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_WHATLINKSHERE, "Linkliste");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_SPECIALPAGES, "Spezialseiten");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_STATS, "Statistik");
        SPECIAL_SUFFIX_DE.put(SpecialPage.SPECIAL_VERSION, "Version");

        EnumMap<SpecialPage, String> SPECIAL_SUFFIX_ES = new EnumMap<SpecialPage, String>(SpecialPage.class);
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_RANDOM, "Aleatoria");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_ALLPAGES, "Todas");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_PREFIXINDEX, "PáginasPorPrefijo");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_SEARCH, "Buscar");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_WHATLINKSHERE, "LoQueEnlazaAquí");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_SPECIALPAGES, "PáginasEspeciales");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_STATS, "Estadísticas");
        SPECIAL_SUFFIX_ES.put(SpecialPage.SPECIAL_VERSION, "Versión");
        
        SPECIAL_SUFFIX.put("en", SPECIAL_SUFFIX_EN);
        SPECIAL_SUFFIX.put("simple", SPECIAL_SUFFIX_EN);
        SPECIAL_SUFFIX.put("de", SPECIAL_SUFFIX_DE);
        SPECIAL_SUFFIX.put("bar", SPECIAL_SUFFIX_DE);
        SPECIAL_SUFFIX.put("es", SPECIAL_SUFFIX_ES);
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
        addTemplateFunction("#ifexist", MyIfexistTemplateFun.CONST);
    }
    
    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#getRawWikiContent(java.lang.String, java.lang.String, java.util.Map)
     */
    @Override
    public String getRawWikiContent(String namespace, String articleName,
            Map<String, String> templateParameters) {
        if (isTemplateNamespace(namespace)) {
            String processedMagicWord = null;
            processedMagicWord = processMagicWord(articleName);
            if (processedMagicWord != null) {
                return processedMagicWord;
            }
        }
        
        if (!isValidTitle(createFullPageName(namespace, articleName))) {
            return null;
        }

        // (ugly) fix for template parameter replacement if no parameters given,
        // e.g. "{{noun}}" in the simple English Wiktionary
        if (templateParameters != null && templateParameters.isEmpty()) {
            templateParameters.put("", null);
        }

        // note: cannot cache templates here since the text returned by the
        // implementation-specific retrievePage() method may depend in the exact
        // parameters or not
        return retrievePage(namespace, articleName, templateParameters);
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
    private String processMagicWord(String templateName) {
        int index = templateName.indexOf(':');
        String magicWord = templateName;
        String parameter = "";
        boolean hasParameter = false;
        if (index > 0) {
            hasParameter = true;
            // if it is a magic word, the first part is the word itself, the second its parameters
            magicWord = templateName.substring(0, index);
            parameter = templateName.substring(index + 1).trim();
        }
        
        if (isMagicWord(magicWord)) {
            // cache values for magic words:
            if (magicWordCache.containsKey(templateName)) {
                return magicWordCache.get(templateName);
            } else {
                String value = retrieveMagicWord(templateName, magicWord, parameter, hasParameter);
                magicWordCache.put(templateName, value);
                return value;
            }
        } else {
            return null;
        }
    }

    /**
     * Determines if a template name corresponds to a magic word using
     * {@link MyMagicWord#isMagicWord(String)} (does not recognise magic
     * words with parameters, e.g. <tt>TALKPAGENAME:Title</tt>).
     * 
     * @param name
     *            the template name (without the template namespace)
     * 
     * @return whether the template is a magic word or not
     */
    protected boolean isMagicWord(String name) {
        return MyMagicWord.isMagicWord(name);
    }

    /**
     * Determines if a template name corresponds to a magic word using
     * {@link #isMagicWord(String)} (also recognises magic
     * words with parameters, e.g. <tt>TALKPAGENAME:Title</tt>).
     * 
     * @param name
     *            the template name (without the template namespace)
     * 
     * @return whether the template is a magic word or not
     */
    public final boolean isMagicWordFull(String name) {
        return isMagicWord(MyMagicWord.extractMagicWordPart(name));
    }

    /**
     * Gets the names of all included pages in the template namespace (excluding
     * magic words).
     * 
     * @return page names without the template namespace prefix
     * @see #getTemplates()
     */
    public final Set<String> getTemplatesNoMagicWords() {
        final Set<String> result = new HashSet<String>(templates.size());
        // remove magic words:
        for (String template : templates) {
            if (!isMagicWordFull(template)) {
                result.add(template);
            }
        }
        return result;
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
     *            the parameters of the magic word name
     * @param hasParameter
     *            whether a parameter was given or not (cannot distinguish from
     *            <tt>parameter</tt> value alone)
     * 
     * @return the contents of the magic word
     */
    protected String retrieveMagicWord(String templateName, String magicWord,
            String parameter, boolean hasParameter) {
        return MyMagicWord.processMagicWord(templateName, parameter, this, hasParameter);
    }
    
    /**
     * Renders the "redirect to" content in case no auto-redirection is used.
     * 
     * @param pageTitle
     *            the title of the page being redirected to
     * 
     * @return HTML redirect note
     */
    public String renderRedirectPage(String pageTitle) {
        final String redirectUrl = getWikiBaseURL().replace("${title}", pageTitle);
        final String safeRedirectTitle = StringEscapeUtils.escapeHtml(pageTitle);
        return "<div class=\"redirectMsg\">"
                + "<img src=\"skins/redirectltr.png\" alt=\"#REDIRECT\" />"
                + "<span class=\"redirectText\">"
                + "<a href=\"" + redirectUrl + "\" title=\"" + safeRedirectTitle + "\">" + pageTitle + "</a>"
                + "</span></div>";
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
     * Retrieves the contents of the given page.
     * 
     * @param namespace
     *            the namespace of the page
     * @param articleName
     *            the (unnormalised) page's name without the namespace
     * @param templateParameters
     *            template parameters if the page is a template, <tt>null</tt>
     *            otherwise
     * 
     * @return <tt>null</tt>
     * @see #retrievePage(String, String, Map, boolean)
     */
    final protected String retrievePage(String namespace, String articleName,
            Map<String, String> templateParameters) {
        return retrievePage(namespace, articleName, templateParameters, true);
    }

    /**
     * Retrieves the contents of the given page.
     * 
     * @param pageName0
     *            the unnormalised name of the page
     * @param templateParameters
     *            template parameters if the page is a template, <tt>null</tt>
     *            otherwise
     * @param followRedirect
     *            whether to follow a redirect or not (at most one redirect
     *            should be followed)
     * 
     * @return the page's contents or <tt>null</tt> if no connection exists
     * @see #retrievePage(String, String, Map, boolean)
     */
    final protected String retrievePage(String pageName0,
            Map<String, String> templateParameters, boolean followRedirect) {
        String[] parts = splitNsTitle(pageName0);
        return retrievePage(parts[0], parts[1], templateParameters, followRedirect);
    }

    /**
     * Retrieves the contents of the given page.
     * 
     * If {@link #hasDBConnection()} is <tt>true</tt>, uses
     * {@link #getRevFromDB(NormalisedTitle)} to get the content from the DB. If
     * <tt>followRedirect</tt> is set, resolves redirects by including the
     * redirected content instead.
     * 
     * Caches retrieved pages in {@link #pageCache}.
     * 
     * @param namespace
     *            the namespace of the page
     * @param articleName
     *            the (unnormalised) page's name without the namespace
     * @param templateParameters
     *            template parameters if the page is a template, <tt>null</tt>
     *            otherwise
     * @param followRedirect
     *            whether to follow a redirect or not (at most one redirect
     *            should be followed)
     * 
     * @return the page's contents or <tt>null</tt> if no connection exists
     */
    protected String retrievePage(String namespace, String articleName,
            Map<String, String> templateParameters, boolean followRedirect) {

        if (articleName.isEmpty()) {
            return null;
        }
        
        // normalise page name:
        NormalisedTitle pageName = normalisePageTitle(namespace, articleName);
        if (pageCache.containsKey(pageName)) {
            return pageCache.get(pageName);
        } else if (hasDBConnection()) {
            String text = null;
            // System.out.println("retrievePage(" + namespace + ", " + articleName + ")");
            RevisionResult getRevResult = getRevFromDB(pageName);
            addStats(getRevResult.stats);
            addInvolvedKeys(getRevResult.involvedKeys);
            if (getRevResult.success) {
                text = getRevResult.revision.unpackedText();
                if (getRevResult.page.isRedirect()) {
                    final Matcher matcher = MATCH_WIKI_REDIRECT.matcher(text);
                    if (matcher.matches()) {
                        String[] redirFullName = splitNsTitle(matcher.group(1));
                        if (followRedirect) {
                            // see https://secure.wikimedia.org/wikipedia/en/wiki/Help:Redirect#Transclusion
                            String redirText = retrievePage(redirFullName[0], redirFullName[1], templateParameters, false);
                            if (redirText != null && !redirText.isEmpty()) {
                                text = redirText;
                            } else {
                                text = "<ol><li>REDIRECT [["
                                        + createFullPageName(redirFullName[0],
                                                redirFullName[1]) + "]]</li></ol>";
                            }
                        } else {
                            // we must disarm the redirect here!
                            text = "<ol><li>REDIRECT [["
                                    + createFullPageName(redirFullName[0],
                                            redirFullName[1]) + "]]</li></ol>";
                        }
                    } else {
                        // we must disarm the redirect here!
                        System.err
                                .println("Couldn't parse the redirect title of \""
                                        + createFullPageName(namespace, articleName)
                                        + "\" in \""
                                        + createFullPageName(
                                                getNamespaceName(),
                                                getPageName())
                                        + "\" from: "
                                        + text.substring(0, Math.min(100, text.length())));
                        text = null;
                    }
                }
            } else {
                // NOTE: must return null for non-existing pages in order for #ifexist to work correctly!
                // System.err.println(getRevResult.message);
                // text = "<b>ERROR: template " + pageName + " not available: " + getRevResult.message + "</b>";
            }
            pageCache.put(pageName, text);
            return text;
        }
        return null;
    }
    
    protected boolean hasDBConnection() {
        return false;
    }
    
    protected RevisionResult getRevFromDB(NormalisedTitle title) {
        return new RevisionResult(false, new ArrayList<InvolvedKey>(),
                "no DB connection", true, title, null, null, false,
                false, title.toString(), 0l);
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
     * Splits the given full title at the first colon.
     * 
     * @param fullTitle
     *            the (full) title including a namespace (if present)
     * 
     * @return a 2-element array with the two components - the first may be
     *         empty if no colon is found
     */
    protected static String[] splitAtColon(String fullTitle) {
        int colonIndex = fullTitle.indexOf(':');
        if (colonIndex != (-1)) {
            return new String[] { fullTitle.substring(0, colonIndex),
                    fullTitle.substring(colonIndex + 1) };
        }
        return new String[] { "", fullTitle };
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#appendRedirectLink(java.lang.String)
     */
    @Override
    public boolean appendRedirectLink(String redirectLink) {
        // do not add redirection if we are parsing a template:
        if (getRecursionLevel() == 0) {
            // remove "#section" from redirect links (this form of redirects is unsupported)
            if (redirectLink != null) {
                return super.appendRedirectLink(redirectLink.replaceFirst("#.*$", ""));
            }
            return super.appendRedirectLink(null);
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
            String namespace2 = splitAtColon(title)[0];
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
         * do not add links like [[:w:nl:User:WinContro|Dutch Wikipedia]] to
         * the internal links
         */
        String[] nsTitle = splitAtColon(topicName);
        if (!nsTitle[0].isEmpty() && isInterWiki(nsTitle[0])) {
            appendInterWikiLink(nsTitle[0], nsTitle[1], "");
        } else {
            super.addLink(topicName);
        }
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#appendInternalLink(java.lang.String, java.lang.String, java.lang.String, java.lang.String, boolean)
     */
    @Override
    public void appendInternalLink(String topic0, String hashSection, String topicDescription,
            String cssClass, boolean parseRecursive) {
        /*
         * convert links like [[:w:nl:User:WinContro|Dutch Wikipedia]] to
         * external links if the link is an interwiki link
         */
        String[] nsTitle = splitAtColon(topic0);
        if (!nsTitle[0].isEmpty() && isInterWiki(nsTitle[0])) {
            appendInterWikiLink(nsTitle[0], nsTitle[1], topicDescription, nsTitle[1].isEmpty() && topicDescription.equals(topic0));
        } else {
            boolean pageExists = true;
            if (existingPages.hasContains()) {
                pageExists = existingPages.contains(normalisePageTitle(topic0));
            }
            super.appendInternalLink(topic0, hashSection, topicDescription, cssClass, parseRecursive, pageExists);
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
    public static String normaliseName(final String value) {
        return Encoder.normaliseTitle(value, true, ' ', true);
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
    public NormalisedTitle normalisePageTitle(final String title) {
        return NormalisedTitle.fromUnnormalised(title, getNamespace());
    }
    
    /**
     * Normalises the given page title by capitalising its first letter after
     * the namespace.
     * 
     * @param maybeNs
     *            the namespace of the page
     * @param articleName
     *            the (unnormalised) page's name without the namespace
     * 
     * @return the normalised page title
     */
    public NormalisedTitle normalisePageTitle(final String maybeNs, final String articleName) {
        return NormalisedTitle.fromUnnormalised(maybeNs, articleName, getNamespace());
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
    public <T extends Collection<NormalisedTitle>> T normalisePageTitles(final Collection<String> titles, T normalisedTitles) {
        return MyWikiModel.<T>normalisePageTitles(titles, getNamespace(), normalisedTitles);
    }
    
    /**
     * Normalises the given page titles by capitalising their first letter after
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
    public static <T extends Collection<NormalisedTitle>> T normalisePageTitles(final Collection<String> titles, final MyNamespace nsObject, T normalisedTitles) {
        for (String title: titles) {
            normalisedTitles.add(NormalisedTitle.fromUnnormalised(title, nsObject));
        }
        return normalisedTitles;
    }
    
    /**
     * De-normalises the given page titles.
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
    public <T extends Collection<String>> T denormalisePageTitles(final Collection<NormalisedTitle> titles, T denormalisedTitles) {
        return MyWikiModel.<T>denormalisePageTitles(titles, getNamespace(), denormalisedTitles);
    }
    
    /**
     * De-normalises the given page titles.
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
    public static <T extends Collection<String>> T denormalisePageTitles(final Collection<NormalisedTitle> titles, final MyNamespace nsObject, T denormalisedTitles) {
        for (NormalisedTitle title: titles) {
            denormalisedTitles.add(title.denormalise(nsObject));
        }
        return denormalisedTitles;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.WikiModel#setUp()
     */
    @Override
    public void setUp() {
        super.setUp();
        magicWordCache = new HashMap<String, String>();
        pageCache = new HashMap<NormalisedTitle, String>();
        if (renderWikiText != null) {
            pageCache.put(normalisePageTitle(getPageName()), renderWikiText );
        }
    }

    /**
     * Checks whether the given namespace is a valid media namespace.
     * 
     * @param namespace
     *            the namespace to check
     * 
     * @return <tt>true</tt> if it is one of the two media namespace strings
     */
    public boolean isMediaNamespace(String namespace) {
        return namespace.equalsIgnoreCase(fNamespace.getMedia()) || namespace.equalsIgnoreCase(fNamespace.getMedia2());
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
        stats.put1(title, value);
    }

    /**
     * Adds the time needed to retrieve the given page to the collected
     * statistics.
     * 
     * @param title
     *            the title of the page
     * @param value
     *            multiple number of milliseconds it took to retrieve the page
     */
    public void addStats(String title, List<Long> value) {
        stats.put(title, value);
    }

    /**
     * Adds the time needed to retrieve the given page to the collected
     * statistics.
     * 
     * @param values
     *            a mapping between page titles and the number of milliseconds
     *            it took to retrieve the page
     */
    public void addStats(Map<String, List<Long>> values) {
        stats.putAll(values);
    }
    
    /**
     * Adds an involved key to the collected statistics.
     * 
     * @param key
     *            the key to add
     */
    public void addInvolvedKey(InvolvedKey key) {
        involvedKeys.add(key);
    }
    
    /**
     * Adds a number of involved keys to the collected statistics.
     * 
     * @param keys
     *            the keys to add
     */
    public void addInvolvedKeys(Collection<? extends InvolvedKey> keys) {
        involvedKeys.addAll(keys);
    }

    /**
     * Gets the list of all keys that have been read or written during the
     * current operation.
     * 
     * @return the involvedKeys
     */
    public List<InvolvedKey> getInvolvedKeys() {
        return involvedKeys;
    }
    
    /**
     * @return the existingPages
     */
    public ExistingPagesCache getExistingPages() {
        return existingPages;
    }

    /**
     * @param existingPages the existingPages to set
     */
    public void setExistingPages(ExistingPagesCache existingPages) {
        this.existingPages = existingPages;
    }
    
    /**
     * The following characters are forbidden in page titles:
     * <tt># &lt; &gt; [ ] | { }</tt>. Any line breaks and non-printable unicode
     * characters are also forbidden here.
     * 
     * @param title
     *            the title to check
     * 
     * @return <tt>true</tt> if the title contains a forbidden character,
     *         <tt>false</tt> otherwise
     * 
     * @see #MATCH_WIKI_FORBIDDEN_TITLE_CHARS
     */
    public static boolean isValidTitle(String title) {
        if (title == null || title.isEmpty() || title.length() >= 256) {
            return false;
        }
        final Matcher matcher = MATCH_WIKI_FORBIDDEN_TITLE_CHARS.matcher(title);
        return !matcher.matches();
    }

    /**
     * Determines whether a page with the given properties is an article.
     * 
     * A new page in the main namespace will be counted as an article if it
     * contains at least one wiki link or is categorised to at least one
     * category.
     * 
     * @param namespace
     *            the ID of the namespace of the page
     * @param links
     *            the links in this page
     * @param categories
     *            categories of this page
     * 
     * @return whether the page is an article or not
     * 
     * @see <a
     *      href="https://www.mediawiki.org/wiki/Manual:Article_count">MediaWiki
     *      explanation</a>
     */
    public static boolean isArticle(int namespace, Collection<String> links,
            Collection<String> categories) {
        return (namespace == 0) && (!links.isEmpty() || !categories.isEmpty());
    }
    
    /**
     * Gets all localised variants for the given special page.
     * 
     * @param page
     *            the special page
     * 
     * @return localised variants including the English names
     */
    public static Collection<String> getLocalisedSpecialPageNames(SpecialPage page) {
        ArrayList<String> result = new ArrayList<String>();
        
        for (Entry<String, String> prefix : SPECIAL_PREFIX.entrySet()) {
            EnumMap<SpecialPage, String> localisedSuffix = SPECIAL_SUFFIX.get(prefix.getKey());
            if (localisedSuffix != null) {
                result.add(MyWikiModel.createFullPageName(prefix.getValue(), localisedSuffix.get(page)));
            }
            // Also add the English suffix for the localised prefix.
            EnumMap<SpecialPage, String> englishSuffix = SPECIAL_SUFFIX.get("en");
            if (englishSuffix != null) {
                result.add(MyWikiModel.createFullPageName(prefix.getValue(), englishSuffix.get(page)));
            }
        }
        return result;
    }
    
    /**
     * Gets the localised variants for the given special page.
     * 
     * @param page
     *            the special page
     * @param language
     *            the language to get the variants for
     * 
     * @return localised variants including the English names
     */
    public static Collection<String> getLocalisedSpecialPageNames(SpecialPage page, String language) {
        ArrayList<String> result = new ArrayList<String>(4);
        
        String localisedPrefix = SPECIAL_PREFIX.get("en");
        EnumMap<SpecialPage, String> localisedSuffix = SPECIAL_SUFFIX.get("en");
        result.add(MyWikiModel.createFullPageName(localisedPrefix, localisedSuffix.get(page)));
        
        localisedPrefix = SPECIAL_PREFIX.get(language);
        localisedSuffix = SPECIAL_SUFFIX.get(language);
        if (localisedPrefix != null && localisedSuffix != null) {
            result.add(MyWikiModel.createFullPageName(localisedPrefix, localisedSuffix.get(page)));
        }
        return result;
    }

    /* (non-Javadoc)
     * @see info.bliki.wiki.model.AbstractWikiModel#isImageNamespace(java.lang.String)
     */
    @Override
    public boolean isImageNamespace(String namespace) {
        return super.isImageNamespace(namespace) ||
                ((MyNamespace) fNamespace).getNumberByName(namespace) == Namespace.FILE_NAMESPACE_KEY;
    }

    /**
     * Renders the raw Wikipedia text into a string for a given converter
     * (renders the wiki text as if a template topic will be displayed
     * directly).
     * 
     * @param converter
     *            a text converter. <b>Note</b> the converter may be
     *            <code>null</code>, if you only would like to analyze the raw
     *            wiki text and don't need to convert. This speeds up the
     *            parsing process.
     * @param rawWikiText
     *            a raw wiki text
     * @return <code>null</code> if an IOException occurs or
     *         <code>converter==null</code>
     * 
     * @see info.bliki.wiki.model.AbstractWikiModel#render(info.bliki.wiki.filter.ITextConverter,
     *      java.lang.String, boolean)
     */
    public String renderPageWithCache(ITextConverter converter, String rawWikiText) {
        renderWikiText = rawWikiText;
        return super.render(converter, rawWikiText, true);
    }

    /**
     * Renders the raw Wikipedia text into an HTML string and use the default
     * HTMLConverter (renders the wiki text as if a template topic will be
     * displayed directly).
     * 
     * @param rawWikiText
     *            a raw wiki text
     * @return <code>null</code> if an IOException occurs
     * 
     * @see info.bliki.wiki.model.AbstractWikiModel#render(java.lang.String,
     *      boolean)
     */
    public String renderPageWithCache(String rawWikiText) {
        renderWikiText = rawWikiText;
        return super.render(new HTMLConverter(), rawWikiText, true);
    }
}
