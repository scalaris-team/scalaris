/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.zib.scalaris.examples.wikipedia.data.ShortRevision;

/**
 * Bean with the content to display in the jsp. 
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiPageBean {
    /**
     * the content of the site
     */
    private String page = "";

    /**
     * the title of the site
     */
    private String title = "";
    
    private Set<String> categories = new LinkedHashSet<String>();
    /**
     * Version of the page (the revision id)
     */
    private int version = 0;
    
    /**
     * signals that the requested page was not available
     * (maybe a fallback-page is shown, but the original one does not exist)
     */
    private boolean notAvailable = false;
    
    /**
     * represents the date of the revision (last page change)
     */
    private Calendar date = new GregorianCalendar();
    
    private String notice = "";
    
    private String wikiTitle = "Wikipedia";
    private String wikiLang = "en";
    private String wikiLangDir = "ltr";
    
    private List<ShortRevision> revisions = new LinkedList<ShortRevision>();
    
    private List<String> subCategories = new LinkedList<String>();
    private List<String> categoryPages = new LinkedList<String>();
    
    private String redirectedTo = "";
    
    private boolean isEditRestricted = false;

    /**
     * @return the notice
     */
    public String getNotice() {
        return notice;
    }

    /**
     * @param notice the notice to set
     */
    public void setNotice(String notice) {
        this.notice = notice;
    }

    /**
     * constructor
     */
    public WikiPageBean() {

    }

    /**
     * gets the page content
     * 
     * @return the content
     */
    public String getPage() {
        return page;
    }

    /**
     * sets the page content
     * 
     * @param page
     *            the content
     */
    public void setPage(String page) {
        this.page = page;
    }

    /**
     * gets the page title
     * 
     * @return the title
     */
    public String getTitle() {
        return title;
    }

    /**
     * sets the page title
     * 
     * @param title
     *            the title
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * returns whether the originally requested page is available
     * 
     * @return the availability status
     */
    public boolean isNotAvailable() {
        return notAvailable;
    }

    /**
     * sets that the originally requested page is not available
     * 
     * @param notAvailable the status to set
     */
    public void setNotAvailable(boolean notAvailable) {
        this.notAvailable = notAvailable;
    }

    /**
     * returns the wiki version of the page
     * 
     * @return the version
     */
    public int getVersion() {
        return version;
    }

    /**
     * sets the wiki version of the page
     * 
     * @param version the version to set
     */
    public void setVersion(int version) {
        this.version = version;
    }

    /**
     * returns the date of the currently shown revision
     * 
     * @return the date
     */
    public Calendar getDate() {
        return date;
    }

    /**
     * sets the 'last changed' date of the page
     * 
     * @param date the date
     */
    public void setDate(Calendar date) {
        this.date = date;
    }

    /**
     * @return the categories
     */
    public Set<String> getCategories() {
        return categories;
    }

    /**
     * @param categories the categories to set
     */
    public void setCategories(Set<String> categories) {
        this.categories = categories;
    }

    /**
     * @return the wikiLang
     */
    public String getWikiLang() {
        return wikiLang;
    }

    /**
     * @param wikiLang the wikiLang to set
     */
    public void setWikiLang(String wikiLang) {
        this.wikiLang = wikiLang;
    }

    /**
     * @return the wikiLangDir
     */
    public String getWikiLangDir() {
        return wikiLangDir;
    }

    /**
     * @param wikiLangDir the wikiLangDir to set
     */
    public void setWikiLangDir(String wikiLangDir) {
        this.wikiLangDir = wikiLangDir;
    }

    /**
     * @return the wikiTitle
     */
    public String getWikiTitle() {
        return wikiTitle;
    }

    /**
     * @param wikiTitle the wikiTitle to set
     */
    public void setWikiTitle(String wikiTitle) {
        this.wikiTitle = wikiTitle;
    }

    /**
     * @return the revisions
     */
    public List<ShortRevision> getRevisions() {
        return revisions;
    }

    /**
     * @param revisions the revisions to set
     */
    public void setRevisions(List<ShortRevision> revisions) {
        this.revisions = revisions;
    }

    /**
     * @return the subCategories
     */
    public List<String> getSubCategories() {
        return subCategories;
    }

    /**
     * @param subCategories the subCategories to set
     */
    public void setSubCategories(List<String> subCategories) {
        this.subCategories = subCategories;
    }

    /**
     * @return the categoryPages
     */
    public List<String> getCategoryPages() {
        return categoryPages;
    }

    /**
     * @param categoryPages the categoryPages to set
     */
    public void setCategoryPages(List<String> categoryPages) {
        this.categoryPages = categoryPages;
    }

    /**
     * @return the redirectedFrom
     */
    public String getRedirectedTo() {
        return redirectedTo;
    }

    /**
     * @param redirectedTo the redirectedFrom to set
     */
    public void setRedirectedTo(String redirectedTo) {
        this.redirectedTo = redirectedTo;
    }

    /**
     * @return the isEditRestricted
     */
    public boolean isEditRestricted() {
        return isEditRestricted;
    }

    /**
     * @param isEditRestricted the isEditRestricted to set
     */
    public void setEditRestricted(boolean isEditRestricted) {
        this.isEditRestricted = isEditRestricted;
    }
}
