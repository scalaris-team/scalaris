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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


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
    private String error = "";
    private String wikiTitle = "Wikipedia";
    private String wikiLang = "en";
    private String wikiLangDir = "ltr";
    private MyNamespace wikiNamespace = new MyNamespace();
    protected String redirectedTo = "";
    private boolean isEditRestricted = false;
    protected Map<String, List<Long>> stats = new LinkedHashMap<String, List<Long>>();
    
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
     * Creates a page bean from a given {@link WikiPageBeanBase}.
     * 
     * @param other
     *            the page bean to copy properties from
     */
    public WikiPageBeanBase(WikiPageBeanBase other) {
        super();
        this.title = other.title;
        version = other.version;
        notice = other.notice;
        error = other.error;
        wikiTitle = other.wikiTitle;
        wikiLang = other.wikiLang;
        wikiLangDir = other.wikiLangDir;
        wikiNamespace = other.wikiNamespace;
        redirectedTo = other.redirectedTo;
        isEditRestricted = other.isEditRestricted;
        stats = new LinkedHashMap<String, List<Long>>(other.stats);
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

    /**
     * @return the stats
     */
    public Map<String, List<Long>> getStats() {
        return stats;
    }

    /**
     * @param stats the stats to set
     */
    public void setStats(Map<String, List<Long>> stats) {
        this.stats = stats;
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
        List<Long> l = stats.get(title);
        if (l == null) {
            stats.put(title, l = new ArrayList<Long>(value.size()));
        }
        l.addAll(value);
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
        for (Entry<String, List<Long>> value : values.entrySet()) {
            addStats(value.getKey(), value.getValue());
        }
    }

    /**
     * @return the error
     */
    public String getError() {
        return error;
    }

    /**
     * @param error the error to set
     */
    public void setError(String error) {
        this.error = error;
    }
}