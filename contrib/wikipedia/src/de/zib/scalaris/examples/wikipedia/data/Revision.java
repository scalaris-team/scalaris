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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Calendar;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.codec.binary.Base64;

/**
 * Represents a revision of a page.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Revision implements Serializable {
    /**
     * Version for serialisation.
     */
    private static final long serialVersionUID = 1L;

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
     * the content (text) of the revision (compressed)
     */
    protected byte[] pText = packText("");

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
     */
    public Revision(int id, String timestamp, boolean minorChange,
            Contributor contributor, String comment) {
        this.id = id;
        this.timestamp = timestamp;
        this.minor = minorChange;
        this.contributor = contributor;
        this.comment = comment;
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
     * Gets the compressed revision text.
     * 
     * @return the text
     */
    public String getB64pText() {
        Base64 b64 = new Base64(0);
        return b64.encodeToString(pText);
    }

    /**
     * Gets the compressed revision text.
     * 
     * @return the text
     */
    public byte[] packedText() {
        return pText;
    }

    /**
     * Gets the un-compressed revision text.
     * 
     * @return the text
     */
    public String unpackedText() {
        return unpackText(pText);
    }

    /**
     * De-compresses the given text and returns it as a string.
     * 
     * @param text
     *            the compressed text
     * 
     * @return the de-compressed text
     * 
     * @throws RuntimeException
     *             if de-compressing the text did not work
     */
    protected static String unpackText(byte[] text) throws RuntimeException {
        try {
            ByteArrayOutputStream unpacked = new ByteArrayOutputStream();
            ByteArrayInputStream bis = new ByteArrayInputStream(text);
            GZIPInputStream gis = new GZIPInputStream(bis);
            byte[] bbuf = new byte[256];
            int read = 0;
            while ((read = gis.read(bbuf)) >= 0) {
                unpacked.write(bbuf, 0, read);
            }
            gis.close();
            return new String(unpacked.toString("UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Compresses the given text and returns it as a byte array.
     * 
     * @param text
     *            the un-compressed text
     * 
     * @return the compressed text
     * 
     * @throws RuntimeException
     *             if compressing the text did not work
     */
    protected static byte[] packText(String text) throws RuntimeException {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            GZIPOutputStream gos = new GZIPOutputStream(bos);
            gos.write(text.getBytes("UTF-8"));
            gos.flush();
            gos.close();
            return bos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
     * @param id
     *            the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * @param timestamp
     *            the timestamp to set
     */
    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @param minor
     *            the minorChange to set
     */
    public void setMinor(boolean minor) {
        this.minor = minor;
    }

    /**
     * @param contributor
     *            the contributor to set
     */
    public void setContributor(Contributor contributor) {
        this.contributor = contributor;
    }

    /**
     * @param comment
     *            the comment to set
     */
    public void setComment(String comment) {
        this.comment = comment;
    }

    /**
     * @param text
     *            the base64-encoded packed text to set
     */
    public void setB64pText(String text) {
        Base64 b64 = new Base64(0);
        this.pText = b64.decode(text);
    }

    /**
     * @param text
     *            the (packed) text to set
     */
    public void setPackedText(byte[] text) {
        this.pText = text;
    }

    /**
     * @param text
     *            the (unpacked) text to set
     */
    public void setUnpackedText(String text) {
        this.pText = packText(text);
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
