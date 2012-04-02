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

import info.bliki.Messages;
import info.bliki.wiki.namespaces.Namespace;

import java.util.ListResourceBundle;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import de.zib.scalaris.examples.wikipedia.NamespaceUtils;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Namespace implementation using a {@link SiteInfo} backend.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyNamespace extends Namespace implements NamespaceUtils {
    private SiteInfo siteinfo;
    protected final Map<String, Integer> NAMESPACE_INT_MAP =
            new TreeMap<String, Integer>(String.CASE_INSENSITIVE_ORDER);
    
    /**
     * Smallest namespace ID (for iterating over all namespaces).
     */
    public final static Integer MIN_NAMESPACE_ID = -2;
    /**
     * Highest namespace ID (for iterating over all namespaces).
     */
    public final static Integer MAX_NAMESPACE_ID = 15;
    
    /**
     * Provides all namespace values as an enum type.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static enum NamespaceEnum {
        /**
         * Alias for direct links to media files.
         */
        MEDIA_NAMESPACE_KEY(MyNamespace.MEDIA_NAMESPACE_KEY),
        /**
         * Holds special pages.
         */
        SPECIAL_NAMESPACE_KEY(MyNamespace.SPECIAL_NAMESPACE_KEY),
        /**
         * "Real" content; articles. Has no prefix.
         */
        MAIN_NAMESPACE_KEY(MyNamespace.MAIN_NAMESPACE_KEY),
        /**
         * Talk pages of "Real" content
         */
        TALK_NAMESPACE_KEY(MyNamespace.TALK_NAMESPACE_KEY),
        /**
         * 
         */
        USER_NAMESPACE_KEY(MyNamespace.USER_NAMESPACE_KEY),
        /**
         * Talk pages for User Pages
         */
        USER_TALK_NAMESPACE_KEY(MyNamespace.USER_TALK_NAMESPACE_KEY),
        /**
         * Information about the wiki. Prefix is the same as $wgSitename of the PHP
         * installation.
         */
        PROJECT_NAMESPACE_KEY(MyNamespace.PROJECT_NAMESPACE_KEY),
        /**
         * 
         */
        PROJECT_TALK_NAMESPACE_KEY(MyNamespace.PROJECT_TALK_NAMESPACE_KEY),
        /**
         * Media description pages.
         */
        FILE_NAMESPACE_KEY(MyNamespace.FILE_NAMESPACE_KEY),
        /**
         * 
         */
        FILE_TALK_NAMESPACE_KEY(MyNamespace.FILE_TALK_NAMESPACE_KEY),
        /**
         * Site interface customisation. Protected.
         */
        MEDIAWIKI_NAMESPACE_KEY(MyNamespace.MEDIAWIKI_NAMESPACE_KEY),
        /**
         * 
         */
        MEDIAWIKI_TALK_NAMESPACE_KEY(MyNamespace.MEDIAWIKI_TALK_NAMESPACE_KEY),
        /**
         * Template pages.
         */
        TEMPLATE_NAMESPACE_KEY(MyNamespace.TEMPLATE_NAMESPACE_KEY),
        /**
         * 
         */
        TEMPLATE_TALK_NAMESPACE_KEY(MyNamespace.TEMPLATE_TALK_NAMESPACE_KEY),
        /**
         * Help pages.
         */
        HELP_NAMESPACE_KEY(MyNamespace.HELP_NAMESPACE_KEY),
        /**
         * 
         */
        HELP_TALK_NAMESPACE_KEY(MyNamespace.HELP_TALK_NAMESPACE_KEY),
        /**
         * Category description pages.
         */
        CATEGORY_NAMESPACE_KEY(MyNamespace.CATEGORY_NAMESPACE_KEY),
        /**
         * 
         */
        CATEGORY_TALK_NAMESPACE_KEY(MyNamespace.CATEGORY_TALK_NAMESPACE_KEY);
        
        private final int id;
        NamespaceEnum(int id) {
            this.id = id;
        }
        
        /**
         * Returns the namespace's ID.
         * 
         * @return the ID of the namespace
         */
        public int getId() {
            return id;
        }

        /**
         * Tries to convert a namespace ID to the according enum value.
         * 
         * @param id  the ID to convert
         * 
         * @return the according enum value
         */
        public static NamespaceEnum fromId(int id) {
            // manual 'switch' is less elegant but faster than going through all
            // values and comparing the IDs
            switch (id) {
                case -2: return MEDIA_NAMESPACE_KEY;
                case -1: return SPECIAL_NAMESPACE_KEY;
                case 0:  return MAIN_NAMESPACE_KEY;
                case 1:  return TALK_NAMESPACE_KEY;
                case 2:  return USER_NAMESPACE_KEY;
                case 3:  return USER_TALK_NAMESPACE_KEY;
                case 4:  return PROJECT_NAMESPACE_KEY;
                case 5:  return PROJECT_TALK_NAMESPACE_KEY;
                case 6:  return FILE_NAMESPACE_KEY;
                case 7:  return FILE_TALK_NAMESPACE_KEY;
                case 8:  return MEDIAWIKI_NAMESPACE_KEY;
                case 9:  return MEDIAWIKI_TALK_NAMESPACE_KEY;
                case 10: return TEMPLATE_NAMESPACE_KEY;
                case 11: return TEMPLATE_TALK_NAMESPACE_KEY;
                case 12: return HELP_NAMESPACE_KEY;
                case 13: return HELP_TALK_NAMESPACE_KEY;
                case 14: return CATEGORY_NAMESPACE_KEY;
                case 15: return CATEGORY_TALK_NAMESPACE_KEY;
                default: throw new IllegalArgumentException(
                        "No constant with id " + id + " found");
            }
        }
    }
    
    /**
     * Resource bundle derived from a given {@link SiteInfo} object.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public static class MyResourceBundle extends ListResourceBundle {
        protected SiteInfo siteinfo;
        
        /**
         * Creates a new bundle using the namespace contents from the given
         * siteinfo.
         * 
         * @param siteinfo
         *            the siteinfo
         */
        public MyResourceBundle(SiteInfo siteinfo) {
            super();
            this.siteinfo = siteinfo;
        }
        
        @Override
        protected Object[][] getContents() {
            /*
             * from: http://www.mediawiki.org/wiki/Manual:Namespace#Built-in_namespaces
             * Index    Name    Purpose     Comments
             * 0   Main    "Real" content; articles    Has no prefix
             * 1   Talk    Talk pages of "Real" content    
             * 2   User    User pages
             * 3   User talk   Talk pages for User Pages   
             * 4   Project     Information about the wiki  Prefix is the same as $wgSitename[1]
             * 5   Project talk    
             * 6   File    Media description pages
             * 7   File talk   
             * 8   MediaWiki   Site interface customisation    Protected
             * 9   MediaWiki talk  
             * 10  Template    Template pages
             * 11  Template talk   
             * 12  Help    Help pages
             * 13  Help talk   
             * 14  Category    Category description pages
             * 15  Category talk   
             * -1  Special     Holds special pages
             * -2  Media   Alias for direct links to media files
             */
            return new Object[][] {
                    {Messages.WIKI_API_MEDIA1, getNsPref(Namespace.MEDIA_NAMESPACE_KEY)},
                    {Messages.WIKI_API_SPECIAL1, getNsPref(Namespace.SPECIAL_NAMESPACE_KEY)},
                    // Main - nothing to add for this
                    {Messages.WIKI_API_TALK1, getNsPref(Namespace.TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_USER1, getNsPref(Namespace.USER_NAMESPACE_KEY)},
                    {Messages.WIKI_API_USERTALK1, getNsPref(Namespace.USER_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_META1, getNsPref(Namespace.PROJECT_NAMESPACE_KEY)},
                    {Messages.WIKI_API_METATALK1, getNsPref(Namespace.PROJECT_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_IMAGE1, getNsPref(Namespace.FILE_NAMESPACE_KEY)},
                    {Messages.WIKI_API_IMAGETALK1, getNsPref(Namespace.FILE_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_MEDIAWIKI1, getNsPref(Namespace.MEDIAWIKI_NAMESPACE_KEY)},
                    {Messages.WIKI_API_MEDIAWIKITALK1, getNsPref(Namespace.MEDIAWIKI_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_TEMPLATE1, getNsPref(Namespace.TEMPLATE_NAMESPACE_KEY)},
                    {Messages.WIKI_API_TEMPLATETALK1, getNsPref(Namespace.TEMPLATE_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_HELP1, getNsPref(Namespace.HELP_NAMESPACE_KEY)},
                    {Messages.WIKI_API_HELPTALK1, getNsPref(Namespace.HELP_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_API_CATEGORY1, getNsPref(Namespace.CATEGORY_NAMESPACE_KEY)},
                    {Messages.WIKI_API_CATEGORYTALK1, getNsPref(Namespace.CATEGORY_TALK_NAMESPACE_KEY)},
                    {Messages.WIKI_TAGS_TOC_CONTENT, "Contents"}, // TODO: internationalise!
                    // Aliases as defined by
                    // https://secure.wikimedia.org/wikipedia/en/wiki/Wikipedia:Namespace#Aliases
                    {Messages.WIKI_API_META2, "WP"}, // also: "Project" but we can only add one here
                    {Messages.WIKI_API_METATALK2, "WT"}, // also: "Project talk" but we can only add one here
                    {Messages.WIKI_API_IMAGE2, "Image"},
                    {Messages.WIKI_API_IMAGETALK2, "Image talk"},
            };
        }

        /**
         * Gets the namespace prefix of the given key.
         * 
         * @param key
         *            the key of the namespace to retrieve
         * @return
         */
        private String getNsPref(Integer key) {
            return siteinfo.getNamespaces().get(key.toString()).get(SiteInfo.NAMESPACE_PREFIX);
        }
    }

    /**
     * Creates a default namespace as in {@link Namespace#Namespace()}.
     */
    public MyNamespace() {
        super();
    }
    
    /**
     * Creates a wikipedia namespace based on the given site info.
     * 
     * @param siteinfo
     *            the site info
     * 
     */
    public MyNamespace(SiteInfo siteinfo) {
        super(new MyResourceBundle(siteinfo));
        this.siteinfo = siteinfo;
        initializeIntNamespaces();
        initializeTalkNamespaces();
    }

    /**
     * Fixes {@link Namespace} not initialising the talk namespaces depending on
     * the resource bundle.
     */
    private void initializeTalkNamespaces() {
        // remove (false) mappings set by Namespace#Namespace(java.util.ResourceBundle)
        TALKSPACE_MAP.clear();
        // add correct mappings:
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.MEDIA_NAMESPACE_KEY), null); // media
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.SPECIAL_NAMESPACE_KEY), null); // special
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.MAIN_NAMESPACE_KEY), getTalk()); // ""
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.TALK_NAMESPACE_KEY), getTalk()); // talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.USER_NAMESPACE_KEY), getUser_talk()); // user
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.USER_TALK_NAMESPACE_KEY), getUser_talk()); // user_talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.PROJECT_NAMESPACE_KEY), getMeta_talk()); // project
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.PROJECT_TALK_NAMESPACE_KEY), getMeta_talk()); // project_talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.FILE_NAMESPACE_KEY), getImage_talk()); // image
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.FILE_TALK_NAMESPACE_KEY), getImage_talk()); // image_talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.MEDIAWIKI_NAMESPACE_KEY), getMediaWiki_talk()); // mediawiki
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.MEDIAWIKI_TALK_NAMESPACE_KEY), getMediaWiki_talk()); // mediawiki_talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.TEMPLATE_NAMESPACE_KEY), getTemplate_talk()); // template
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.TEMPLATE_TALK_NAMESPACE_KEY), getTemplate_talk()); // template_talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.HELP_NAMESPACE_KEY), getHelp_talk()); // help
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.HELP_TALK_NAMESPACE_KEY), getHelp_talk()); // help_talk
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.CATEGORY_NAMESPACE_KEY), getCategory_talk()); // category
        TALKSPACE_MAP.put(getNamespaceByNumber(Namespace.CATEGORY_TALK_NAMESPACE_KEY), getCategory_talk()); // category_talk
    }

    /**
     * Initialises a mapping of namespace strings to integers.
     */
    private void initializeIntNamespaces() {
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.MEDIA_NAMESPACE_KEY), Namespace.MEDIA_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.SPECIAL_NAMESPACE_KEY), Namespace.SPECIAL_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.MAIN_NAMESPACE_KEY), Namespace.MAIN_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.TALK_NAMESPACE_KEY), Namespace.TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.USER_NAMESPACE_KEY), Namespace.USER_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.USER_TALK_NAMESPACE_KEY), Namespace.USER_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.PROJECT_NAMESPACE_KEY), Namespace.PROJECT_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.PROJECT_TALK_NAMESPACE_KEY), Namespace.PROJECT_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.FILE_NAMESPACE_KEY), Namespace.FILE_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.FILE_TALK_NAMESPACE_KEY), Namespace.FILE_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.MEDIAWIKI_NAMESPACE_KEY), Namespace.MEDIAWIKI_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.MEDIAWIKI_TALK_NAMESPACE_KEY), Namespace.MEDIAWIKI_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.TEMPLATE_NAMESPACE_KEY), Namespace.TEMPLATE_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.TEMPLATE_TALK_NAMESPACE_KEY), Namespace.TEMPLATE_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.HELP_NAMESPACE_KEY), Namespace.HELP_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.HELP_TALK_NAMESPACE_KEY), Namespace.HELP_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.CATEGORY_NAMESPACE_KEY), Namespace.CATEGORY_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put(getNamespaceByNumber(Namespace.CATEGORY_TALK_NAMESPACE_KEY), Namespace.CATEGORY_TALK_NAMESPACE_KEY);
        // Aliases as defined by
        // https://secure.wikimedia.org/wikipedia/en/wiki/Wikipedia:Namespace#Aliases
        NAMESPACE_INT_MAP.put("WP", Namespace.PROJECT_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put("Project", Namespace.PROJECT_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put("WT", Namespace.PROJECT_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put("Project talk", Namespace.PROJECT_TALK_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put("Image", Namespace.FILE_NAMESPACE_KEY);
        NAMESPACE_INT_MAP.put("Image talk", Namespace.FILE_TALK_NAMESPACE_KEY);
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getSubjectspace(java.lang.String)
     */
    @Override
    public String getSubjectspace(String talkNamespace) {
        for (Entry<String, String> entry: TALKSPACE_MAP.entrySet() ) {
            String value = entry.getValue();
            if (value != null && value.equals(talkNamespace)) {
                return NAMESPACE_MAP.get(entry.getKey());
            }
        }
        return null;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getSiteinfo()
     */
    @Override
    public SiteInfo getSiteinfo() {
        return siteinfo;
    }

    /**
     * Gets the integer number of the given namespace.
     * 
     * @param namespace
     *            the namespace
     * 
     * @return an integer or <tt>null</tt> if this is no namespace
     */
    public Integer getNumberByName(String namespace) {
        return NAMESPACE_INT_MAP.get(namespace);
    }
    
    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getTalkPageFromPageName(java.lang.String)
     */
    @Override
    public String getTalkPageFromPageName(String pageName) {
        String[] pnSplit = MyWikiModel.splitNsTitle(pageName, this);
        String talkspace = getTalkspace(pnSplit[0]);
        if (talkspace == null) {
            return pnSplit[1];
        } else {
            return talkspace + ":" + pnSplit[1];
        }
    }
    
    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getPageNameFromTalkPage(java.lang.String)
     */
    @Override
    public String getPageNameFromTalkPage(String talkPageName) {
        String[] pnSplit = MyWikiModel.splitNsTitle(talkPageName, this);
        String namespace = pnSplit[0];
        String talkspace = getTalkspace(namespace);
        if (talkspace == null || !namespace.equals(talkspace)) {
            return talkPageName;
        } else {
            return pnSplit[1];
        }
    }
    
    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#isTalkPage(java.lang.String)
     */
    @Override
    public boolean isTalkPage(String pageName) {
        String namespace = MyWikiModel.getNamespace(pageName, this);
        String talkspace = getTalkspace(namespace);
        if (talkspace == null || !namespace.equals(talkspace)) {
            return false;
        } else {
            return true;
        }
    }
}
