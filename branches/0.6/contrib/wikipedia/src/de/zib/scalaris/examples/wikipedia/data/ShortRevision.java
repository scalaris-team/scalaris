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
import java.util.LinkedList;
import java.util.List;

/**
 * Represents a revision of a page.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class ShortRevision implements Serializable {
    /**
     * Version for serialisation.
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * the revision's ID
     */
    protected int id;

    /**
     * the revision's date of creation
     */
    protected String timestamp;
    
    /**
     * whether the change is a minor change or not
     */
    protected boolean minor;

    /**
     * the revision's contributor
     */
    protected Contributor contributor;

    /**
     * the comment of the revision
     */
    protected String comment;

    /**
     * the size of the content (text) of the revision
     */
    protected int size;

    /**
     * Creates an empty short revision.
     */
    public ShortRevision() {
        this.id = 0;
        this.timestamp = "";
        this.minor = false;
        this.contributor = new Contributor();
        this.comment = "";
        this.size = 0;
    }

    /**
     * Creates a new short revision from the given revision.
     * 
     * @param revision
     *            the revision to describe
     */
    public ShortRevision(Revision revision) {
        this.id = revision.getId();
        this.timestamp = revision.getTimestamp();
        this.minor = revision.isMinor();
        this.contributor = revision.getContributor();
        this.comment = revision.getComment();
        this.size = revision.unpackedText().length();
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
     * @return the size of the revision's text
     */
    public int getSize() {
        return size;
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
     * @param size the size to set
     */
    public void setSize(int size) {
        this.size = size;
    }
    
    /**
     * Converts a list of {@link Revision} objects into a list of
     * {@link ShortRevision} objects.
     * 
     * @param revisions
     *            the revision list to convert
     * 
     * @return a short revision list
     */
    public static List<ShortRevision> fromRevisions(final List<Revision> revisions) {
        List<ShortRevision> result = new LinkedList<ShortRevision>();
        for (Revision rev : revisions) {
            result.add(new ShortRevision(rev));
        }
        return result;
    }
}
