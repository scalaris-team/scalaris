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


/**
 * Bean with common content to display in a jsp. 
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiPageBeanBase {

    /**
     * the title of the site
     */
    private String title = "";
    /**
     * Version of the page (the revision id)
     */
    private int version = 0;
    private String notice = "";
    private String wikiTitle = "Wikipedia";
    private String wikiLang = "en";
    private String wikiLangDir = "ltr";
    private MyNamespace wikiNamespace = new MyNamespace();
    protected String redirectedTo = "";
    private boolean isEditRestricted = false;
    /**
     * the content of the site
     */
    private String page = "";

    /**
     * Creates a new (empty) bean.
     */
    public WikiPageBeanBase() {
        super();
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
     * @return the wikiTalkNamespace
     */
    public MyNamespace getWikiNamespace() {
        return wikiNamespace;
    }

    /**
     * @param wikiNamespace the wikiTalkNamespace to set
     */
    public void setWikiNamespace(MyNamespace wikiNamespace) {
        this.wikiNamespace = wikiNamespace;
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