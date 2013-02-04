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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import de.zib.scalaris.examples.wikipedia.InvolvedKey;
import de.zib.tools.LinkedMultiHashMap;

/**
 * Bean with common content to display in a jsp. 
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiPageBeanBase {

    /**
     * the service user
     */
    private String serviceUser = "";
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
    private boolean isEditRestricted = false;
    protected LinkedMultiHashMap<String, Long> stats = new LinkedMultiHashMap<String, Long>();
    private long startTime;
    
    /**
     * the content of the site
     */
    private String page = "";
    /**
     * number of attempts of saving a wiki page
     */
    private int saveAttempts = 0;
    /**
     * All keys that have been read or written during the current operation.
     */
    public List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
    /**
     * In cases of failed page-save commits, contains a list of failed keys for
     * each save attempt.
     */
    protected LinkedMultiHashMap<Integer, String> failedKeys = new LinkedMultiHashMap<Integer, String>();

    /**
     * Creates a new (empty) bean.
     */
    public WikiPageBeanBase() {
        super();
        startTime = System.currentTimeMillis();
    }

    /**
     * Creates a new (empty) bean with the given start time.
     * 
     * @param serviceUser
     *            service user
     * @param startTime
     *            the time when the request reached the servlet (in ms)
     */
    public WikiPageBeanBase(String serviceUser, long startTime) {
        super();
        this.serviceUser = serviceUser;
        this.startTime = startTime;
    }
    
    /**
     * Creates a page bean from a given {@link WikiPageBeanBase}.
     * 
     * @param other
     *            the page bean to copy properties from
     */
    public WikiPageBeanBase(WikiPageBeanBase other) {
        super();
        this.serviceUser = other.serviceUser;
        this.title = other.title;
        version = other.version;
        notice = other.notice;
        error = other.error;
        wikiTitle = other.wikiTitle;
        wikiLang = other.wikiLang;
        wikiLangDir = other.wikiLangDir;
        wikiNamespace = other.wikiNamespace;
        isEditRestricted = other.isEditRestricted;
        stats = new LinkedMultiHashMap<String, Long>(other.stats);
        startTime = other.startTime;
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
     * Gets information about the time needed to look up pages.
     * 
     * @return a mapping of page titles to retrieval times
     */
    public Map<String, List<Long>> getStats() {
        return stats;
    }

    /**
     * @param stats the stats to set
     */
    public void setStats(Map<String, List<Long>> stats) {
        this.stats = new LinkedMultiHashMap<String, Long>(stats);
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

    /**
     * @return the saveAttempts
     */
    public int getSaveAttempts() {
        return saveAttempts;
    }

    /**
     * @param saveAttempts the saveAttempts to set
     */
    public void setSaveAttempts(int saveAttempts) {
        this.saveAttempts = saveAttempts;
    }

    /**
     * @return the failedKeys
     */
    public LinkedMultiHashMap<Integer, String> getFailedKeys() {
        return failedKeys;
    }

    /**
     * @param failedKeys the failedKeys to set
     */
    public void setFailedKeys(LinkedMultiHashMap<Integer, String> failedKeys) {
        this.failedKeys = failedKeys;
    }

    /**
     * @return the involvedKeys
     */
    public List<InvolvedKey> getInvolvedKeys() {
        return involvedKeys;
    }

    /**
     * @param involvedKeys the involvedKeys to set
     */
    public void setInvolvedKeys(List<InvolvedKey> involvedKeys) {
        this.involvedKeys = involvedKeys;
    }

    /**
     * @return the startTime
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * @param startTime the startTime to set
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * @return the serviceUser
     */
    public String getServiceUser() {
        return serviceUser;
    }

    /**
     * @param serviceUser the serviceUser to set
     */
    public void setServiceUser(String serviceUser) {
        this.serviceUser = serviceUser;
    }
}