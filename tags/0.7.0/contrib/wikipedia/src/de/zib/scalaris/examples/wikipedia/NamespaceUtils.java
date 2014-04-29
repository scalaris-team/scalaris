package de.zib.scalaris.examples.wikipedia;

import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Interface for namespace implementations using a {@link SiteInfo} backend.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public interface NamespaceUtils {

    /**
     * Gets the siteinfo object used for this namespace.
     * 
     * @return the siteinfo
     */
    public abstract SiteInfo getSiteinfo();

    /**
     * Gets the talk page's name corresponding to the given page name.
     * 
     * @param pageName
     *            a page name, i.e. a title
     * 
     * @return the name of the talk page
     */
    public abstract String getTalkPageFromPageName(String pageName);

    /**
     * Gets the content page's name corresponding to the given talk page name.
     * 
     * @param talkPageName
     *            a talk page name, i.e. a title
     * 
     * @return the name of the content page
     */
    public abstract String getPageNameFromTalkPage(String talkPageName);

    /**
     * Checks whether the page's name is in a talk space.
     * 
     * @param pageName
     *            a page name, i.e. a title
     * 
     * @return <tt>true</tt> if the page is a talk page, <tt>false</tt>
     *         otherwise
     */
    public abstract boolean isTalkPage(String pageName);

}