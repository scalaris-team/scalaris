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
package de.zib.scalaris.examples.wikipedia.data;

import java.io.Serializable;

/**
 * Represents a revision of a page.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Contribution implements Serializable {
    /**
     * Version for serialisation.
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * the edited revision's ID (note: 1 means a new page was created)
     */
    protected int id;

    /**
     * the edited revision's date of creation
     */
    protected String timestamp;
    
    /**
     * whether the change is a minor change or not
     */
    protected boolean minor;

    /**
     * the revision's contributor
     */
    protected String pageName;

    /**
     * the comment of the revision
     */
    protected String comment;

    /**
     * the size of the content (text) of the revision before the edit
     */
    protected int sizeBefore;

    /**
     * the size of the content (text) of the revision after the edit
     */
    protected int sizeAfter;

    /**
     * Creates an empty short revision.
     */
    public Contribution() {
        this.id = 0;
        this.timestamp = "";
        this.minor = false;
        this.pageName = "";
        this.comment = "";
        this.sizeBefore = 0;
        this.sizeAfter = 0;
    }

    /**
     * Creates a new short revision from the given revision.
     * 
     * @param oldPage
     *            the page object before the edit (including the revision)
     * @param newPage
     *            the page object after the edit (including the revision)
     */
    public Contribution(Page oldPage, Page newPage) {
        this.id = newPage.getCurRev().getId();
        this.timestamp = newPage.getCurRev().getTimestamp();
        this.minor = newPage.getCurRev().isMinor();
        this.pageName = newPage.getTitle();
        this.comment = newPage.getCurRev().getComment();
        if (oldPage != null) {
            this.sizeBefore = oldPage.getCurRev().unpackedText().length();
        } else {
            this.sizeBefore = 0;
        }
        this.sizeAfter = newPage.getCurRev().unpackedText().length();
    }

    /**
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * @return the timestamp
     */
    public String getTimestamp() {
        return timestamp;
    }

    /**
     * @return the pageName
     */
    public String getPageName() {
        return pageName;
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @return the sizeBefore
     */
    public int getSizeBefore() {
        return sizeBefore;
    }

    /**
     * @return the sizeAfter
     */
    public int getSizeAfter() {
        return sizeAfter;
    }

    /**
     * Gets whether the change is a minor change or not.
     * 
     * @return <tt>true</tt> if the revision is a minor change, <tt>false</tt>
     *         otherwise
     */
    public boolean isMinor() {
        return minor;
    }

    /**
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @param timestamp the timestamp to set
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @param minor the minorChange to set
     */
    public void setMinor(boolean minor) {
        this.minor = minor;
    }

    /**
     * @param pageName the pageName to set
     */
    public void setPageName(String pageName) {
        this.pageName = pageName;
    }

    /**
     * @param comment the comment to set
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @param sizeBefore the sizeBefore to set
     */
    public void setSizeBefore(int sizeBefore) {
        this.sizeBefore = sizeBefore;
    }

    /**
     * @param sizeAfter the sizeAfter to set
     */
    public void setSizeAfter(int sizeAfter) {
        this.sizeAfter = sizeAfter;
    }
}
