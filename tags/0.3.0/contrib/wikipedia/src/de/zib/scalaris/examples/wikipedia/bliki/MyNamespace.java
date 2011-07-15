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
import java.util.Map.Entry;

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
                    {Messages.WIKI_TAGS_TOC_CONTENT, "Contents"} // TODO: internationalise!
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
        initializeTalkNamespaces();
    }

    /**
     * Fixes {@link Namespace} not initialising the talk namespaces depending on
     * the resource bundle.
     */
    private void initializeTalkNamespaces() {
        TALKSPACE_MAP.put(fNamespacesLowercase[0], null); // media
        TALKSPACE_MAP.put(fNamespacesLowercase[1], null); // special
        TALKSPACE_MAP.put(fNamespacesLowercase[2], getTalk()); // ""
        TALKSPACE_MAP.put(fNamespacesLowercase[3], getTalk()); // talk
        TALKSPACE_MAP.put(fNamespacesLowercase[4], getUser_talk()); // user
        TALKSPACE_MAP.put(fNamespacesLowercase[5], getUser_talk()); // user_talk
        TALKSPACE_MAP.put(fNamespacesLowercase[6], getMeta_talk()); // project
        TALKSPACE_MAP.put(fNamespacesLowercase[7], getMeta_talk()); // project_talk
        TALKSPACE_MAP.put(fNamespacesLowercase[8], getImage_talk()); // image
        TALKSPACE_MAP.put(fNamespacesLowercase[9], getImage_talk()); // image_talk
        TALKSPACE_MAP.put(fNamespacesLowercase[10], getMediaWiki_talk()); // mediawiki
        TALKSPACE_MAP.put(fNamespacesLowercase[11], getMediaWiki_talk()); // mediawiki_talk
        TALKSPACE_MAP.put(fNamespacesLowercase[12], getTemplate_talk()); // template
        TALKSPACE_MAP.put(fNamespacesLowercase[13], getTemplate_talk()); // template_talk
        TALKSPACE_MAP.put(fNamespacesLowercase[14], getHelp_talk()); // help
        TALKSPACE_MAP.put(fNamespacesLowercase[15], getHelp_talk()); // help_talk
        TALKSPACE_MAP.put(fNamespacesLowercase[16], getCategory_talk()); // category
        TALKSPACE_MAP.put(fNamespacesLowercase[17], getCategory_talk()); // category_talk
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getSubjectspace(java.lang.String)
     */
    @Override
    public String getSubjectspace(String talkNamespace) {
        for (Entry<String, String> entry: TALKSPACE_MAP.entrySet() ) {
            if (entry.getValue().equals(talkNamespace)) {
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
    
    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.NamespaceUtils#getTalkPageFromPageName(java.lang.String)
     */
    @Override
    public String getTalkPageFromPageName(String pageName) {
        String[] pnSplit = MyWikiModel.splitNsTitle(pageName);
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
        String[] pnSplit = MyWikiModel.splitNsTitle(talkPageName);
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
        String namespace = MyWikiModel.getNamespace(pageName);
        String talkspace = getTalkspace(namespace);
        if (talkspace == null || !namespace.equals(talkspace)) {
            return false;
        } else {
            return true;
        }
    }
}
