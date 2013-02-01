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

import info.bliki.Messages;
import info.bliki.wiki.namespaces.Namespace;

import java.util.ListResourceBundle;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;

import de.zib.scalaris.examples.wikipedia.NamespaceUtils;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Namespace implementation using a {@link SiteInfo} backend.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class MyNamespace extends Namespace implements NamespaceUtils {
    private SiteInfo siteinfo;
    
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
         * Talk pages for portal pages.
         */
        CATEGORY_TALK_NAMESPACE_KEY(MyNamespace.CATEGORY_TALK_NAMESPACE_KEY),
        /**
         * Portal pages.
         */
        PORTAL_NAMESPACE_KEY(MyNamespace.PORTAL_NAMESPACE_KEY),
        /**
         * 
         */
        PORTAL_TALK_NAMESPACE_KEY(MyNamespace.PORTAL_TALK_NAMESPACE_KEY);
        
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
                case 100: return PORTAL_NAMESPACE_KEY;
                case 101: return PORTAL_TALK_NAMESPACE_KEY;
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
        protected ResourceBundle langResBundle = null;
        protected ResourceBundle enBundle;
        
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
            Locale locale = siteinfo.extractLolace();
            if (locale != null) {
                langResBundle = Messages.getResourceBundle(locale);
            }
            enBundle = Messages.getResourceBundle(Locale.ENGLISH);
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
             * 100 Portal
             * 101 Portal talk
             * -1  Special     Holds special pages
             * -2  Media   Alias for direct links to media files
             */
            return new Object[][] {
                    {Messages.WIKI_API_MEDIA1, getNsPref(Namespace.MEDIA_NAMESPACE_KEY, Messages.WIKI_API_MEDIA1)},
                    {Messages.WIKI_API_SPECIAL1, getNsPref(Namespace.SPECIAL_NAMESPACE_KEY, Messages.WIKI_API_SPECIAL1)},
                    // Main - nothing to add for this
                    {Messages.WIKI_API_TALK1, getNsPref(Namespace.TALK_NAMESPACE_KEY, Messages.WIKI_API_TALK1)},
                    {Messages.WIKI_API_USER1, getNsPref(Namespace.USER_NAMESPACE_KEY, Messages.WIKI_API_USER1)},
                    {Messages.WIKI_API_USERTALK1, getNsPref(Namespace.USER_TALK_NAMESPACE_KEY, Messages.WIKI_API_USERTALK1)},
                    {Messages.WIKI_API_META1, getNsPref(Namespace.PROJECT_NAMESPACE_KEY, Messages.WIKI_API_META1)},
                    {Messages.WIKI_API_METATALK1, getNsPref(Namespace.PROJECT_TALK_NAMESPACE_KEY, Messages.WIKI_API_METATALK1)},
                    {Messages.WIKI_API_IMAGE1, getNsPref(Namespace.FILE_NAMESPACE_KEY, Messages.WIKI_API_IMAGE1)},
                    {Messages.WIKI_API_IMAGETALK1, getNsPref(Namespace.FILE_TALK_NAMESPACE_KEY, Messages.WIKI_API_IMAGETALK1)},
                    {Messages.WIKI_API_MEDIAWIKI1, getNsPref(Namespace.MEDIAWIKI_NAMESPACE_KEY, Messages.WIKI_API_MEDIAWIKI1)},
                    {Messages.WIKI_API_MEDIAWIKITALK1, getNsPref(Namespace.MEDIAWIKI_TALK_NAMESPACE_KEY, Messages.WIKI_API_MEDIAWIKITALK1)},
                    {Messages.WIKI_API_TEMPLATE1, getNsPref(Namespace.TEMPLATE_NAMESPACE_KEY, Messages.WIKI_API_TEMPLATE1)},
                    {Messages.WIKI_API_TEMPLATETALK1, getNsPref(Namespace.TEMPLATE_TALK_NAMESPACE_KEY, Messages.WIKI_API_TEMPLATETALK1)},
                    {Messages.WIKI_API_HELP1, getNsPref(Namespace.HELP_NAMESPACE_KEY, Messages.WIKI_API_HELP1)},
                    {Messages.WIKI_API_HELPTALK1, getNsPref(Namespace.HELP_TALK_NAMESPACE_KEY, Messages.WIKI_API_HELPTALK1)},
                    {Messages.WIKI_API_CATEGORY1, getNsPref(Namespace.CATEGORY_NAMESPACE_KEY, Messages.WIKI_API_CATEGORY1)},
                    {Messages.WIKI_API_CATEGORYTALK1, getNsPref(Namespace.CATEGORY_TALK_NAMESPACE_KEY, Messages.WIKI_API_CATEGORYTALK1)},
                    {Messages.WIKI_API_PORTAL1, getNsPref(Namespace.PORTAL_NAMESPACE_KEY, Messages.WIKI_API_PORTAL1)},
                    {Messages.WIKI_API_PORTALTALK1, getNsPref(Namespace.PORTAL_TALK_NAMESPACE_KEY, Messages.WIKI_API_PORTALTALK1)},
                    {Messages.WIKI_TAGS_TOC_CONTENT, langResBundle == null ? "Contents" : Messages.getString(langResBundle, Messages.WIKI_TAGS_TOC_CONTENT, "Contents")}
            };
        }

        /**
         * Gets the namespace prefix of the given key.
         * 
         * @param key
         *            the key of the namespace to retrieve
         * @param msgkey
         *            the key used by {@link Messages}
         * @return namespace name
         */
        private String getNsPref(Integer key, String msgkey) {
            final Map<String, String> nsMap = siteinfo.getNamespaces().get(key.toString());
            if (nsMap != null) {
                return nsMap.get(SiteInfo.NAMESPACE_PREFIX);
            }
            // note: the result MUST NOT BE NULL!
            // otherwise Messages#getString() will return <tt>"!" + key + "!"</tt>
            String fallBackName = null;
            if (langResBundle != null) {
                fallBackName = Messages.getString(langResBundle, msgkey, null);
            }
            if (fallBackName == null) {
                fallBackName = Messages.getString(enBundle, msgkey, null);
            }
            return fallBackName;
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
        Locale locale = siteinfo.extractLolace();
        if (locale != null) {
            initializeAliases(Messages.getResourceBundle(locale));
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getSiteinfo()
     */
    @Override
    public SiteInfo getSiteinfo() {
        return siteinfo;
    }
    
    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getTalkPageFromPageName(java.lang.String)
     */
    @Override
    public String getTalkPageFromPageName(String pageName) {
        String[] pnSplit = splitNsTitle(pageName);
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
        String[] pnSplit = splitNsTitle(talkPageName);
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
        String namespace = splitNsTitle(pageName)[0];
        String talkspace = getTalkspace(namespace);
        if (talkspace == null || !namespace.equals(talkspace)) {
            return false;
        } else {
            return true;
        }
    }
}
