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
package de.zib.scalaris.examples.wikipedia.data;

import info.bliki.wiki.model.WikiModel;

import java.util.Calendar;
import java.util.Collection;

/**
 * Represents a revision of a page.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Revision {
    /**
     * the revision's ID
     */
    protected int id = 0;

    /**
     * the revision's date of creation
     */
    protected String timestamp = "";
    
    /**
     * whether the change is a minor change or not
     */
    protected boolean minor = false;

    /**
     * the revision's contributor
     */
    protected Contributor contributor = new Contributor();

    /**
     * the comment of the revision
     */
    protected String comment = "";

    /**
     * the content (text) of the revision
     */
    protected String text = "";

    /**
     * Creates a new revision with invalid data. Use the setters to make it a
     * valid revision.
     */
    public Revision() {
    }

    /**
     * Creates a new revision with the given data.
     * 
     * @param id
     *            the id of the revision
     * @param timestamp
     *            the time the revision was created
     * @param minorChange
     *            whether the change is a minor change or not
     * @param contributor
     *            the contributor of the revision
     * @param comment
     *            a comment entered when the revision was created
     * @param text
     *            the text of the revision
     */
    public Revision(int id, String timestamp, boolean minorChange,
            Contributor contributor, String comment, String text) {
        this.id = id;
        this.timestamp = timestamp;
        this.minor = minorChange;
        this.contributor = contributor;
        this.comment = comment;
        this.text = text;
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
     * @return the contributor
     */
    public Contributor getContributor() {
        return contributor;
    }

    /**
     * @return the comment
     */
    public String getComment() {
        return comment;
    }

    /**
     * @return the text
     */
    public String getText() {
        return text;
    }

    /**
     * Parses the revision's text and returns all categories it belongs to.
     * NOTE: the entire text of the revision is parsed which may take a while.
     * 
     * @param wikiModel
     *            the wiki model to parse the categories from the revision's
     *            text
     * 
     * @return a list of categories of the revision
     */
    public Collection<String> parseCategories(WikiModel wikiModel) {
        wikiModel.render(null, text);
        return wikiModel.getCategories().keySet();
    }

    /**
     * Parses the revision's text and returns all categories it belongs to.
     * NOTE: the entire text of the revision is parsed which may take a while.
     * 
     * @param wikiModel
     *            the wiki model to parse the categories from the revision's
     *            text
     * 
     * @return a list of categories of the revision
     */
    public Collection<String> parseTemplates(WikiModel wikiModel) {
        wikiModel.render(null, text);
        return wikiModel.getTemplates();
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
     * @param contributor the contributor to set
     */
    public void setContributor(Contributor contributor) {
        this.contributor = contributor;
    }

    /**
     * @param comment the comment to set
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @param text the text to set
     */
    public void setText(String text) {
        this.text = text;
    }
    
    /**
     * Converts a timestamp in ISO8601 format to a {@link Calendar} object.
     * 
     * @param timestamp
     *            the timestamp to convert
     * 
     * @return a {@link Calendar} with the same date
     */
    public static Calendar stringToCalendar(String timestamp) {
        return javax.xml.bind.DatatypeConverter.parseDateTime(timestamp
                .toString());
    }
    
    /**
     * Converts a {@link Calendar} object to a timestamp in ISO8601 format.
     * 
     * @param calendar
     *            the calendar to convert
     * 
     * @return a timestamp string with the same date
     */
    public static String calendarToString(Calendar calendar) {
        return javax.xml.bind.DatatypeConverter.printDateTime(calendar);
    }
}
