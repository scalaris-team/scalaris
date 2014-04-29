/**
 *  Copyright 2007-2013 Zuse Institute Berlin
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
import java.util.Collection;
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
public class WikiPageBean extends WikiPageBeanBase {
    private Set<String> categories = new LinkedHashSet<String>();
    /**
     * signals that the requested page was not available
     * (maybe a fallback-page is shown, but the original one does not exist)
     */
    private boolean notAvailable = false;
    
    /**
     * represents the date of the revision (last page change)
     */
    private Calendar date = new GregorianCalendar();
    
    private List<ShortRevision> revisions = new LinkedList<ShortRevision>();
    
    private Collection<String> subCategories = new LinkedList<String>();
    private Collection<String> categoryPages = new LinkedList<String>();
    
    private String contentSub = "";

    /**
     * Creates an empty page bean.
     */
    public WikiPageBean() {
        super();
    }

    /**
     * Creates a new (empty) bean with the given start time.
     * 
     * @param serviceUser
     *            service user
     * @param startTime
     *            the time when the request reached the servlet (in ms)
     */
    public WikiPageBean(String serviceUser, long startTime) {
        super(serviceUser, startTime);
    }
    
    /**
     * Creates a page bean from a given {@link WikiPageBeanBase}.
     * 
     * @param other
     *            the page bean to copy properties from
     */
    public WikiPageBean(WikiPageBeanBase other) {
        super(other);
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
    public Collection<String> getSubCategories() {
        return subCategories;
    }

    /**
     * @param subCategories the subCategories to set
     */
    public void setSubCategories(Collection<String> subCategories) {
        this.subCategories = subCategories;
    }

    /**
     * @return the categoryPages
     */
    public Collection<String> getCategoryPages() {
        return categoryPages;
    }

    /**
     * @param categoryPages the categoryPages to set
     */
    public void setCategoryPages(Collection<String> categoryPages) {
        this.categoryPages = categoryPages;
    }
    
    /**
     * @return the contentSub
     */
    public String getContentSub() {
        return contentSub;
    }

    /**
     * @param contentSub the contentSub to set
     */
    public void setContentSub(String contentSub) {
        this.contentSub = contentSub;
    }
}
