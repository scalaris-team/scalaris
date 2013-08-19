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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Represents a page including its revisions.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Page implements Serializable {
    /**
     * Version for serialisation.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The page's title.
     */
    protected String title = "";

    /**
     * The page's ID.
     */
    protected int id = -1;
    
    /**
     * Whether the page's newest revision redirects or not.
     */
    protected boolean redirect = false;
    
    /**
     * Page restrictions, e.g. for moving/editing the page.
     */
    protected Map<String, String> restrictions = new LinkedHashMap<String, String>();
    
    /**
     * Current revision (cached).
     */
    protected Revision curRev = null;

    /**
     * Creates a new page with default values (this page is invalid until all of
     * them have been set!).
     */
    public Page() {
    }

    /**
     * Creates a new page with the given title and ID and a single revision.
     * 
     * @param title
     *            the title of the page
     * @param id
     *            the id of the page
     * @param redirect
     *            whether the page's newest revision redirects or not
     * @param restrictions
     *            page restrictions
     * @param curRev
     *            current revision
     */
    public Page(String title, int id, boolean redirect, Map<String, String> restrictions, Revision curRev) {
        this.title = title;
        this.id = id;
        this.redirect = redirect;
        this.restrictions = restrictions;
        this.curRev = curRev;
    }

    /**
     * Gets the page's title.
     * 
     * @return the title of the page
     */
    public String getTitle() {
        return title;
    }

    /**
     * Gets the page's ID.
     * 
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Gets whether the page's newest revision redirects or not.
     * 
     * @return <tt>true</tt> if redirecting, otherwise <tt>false</tt>
     */
    public boolean isRedirect() {
        return redirect;
    }

    /**
     * Gets all page restrictions, e.g. for moving/editing the page.
     * 
     * @return the restrictions
     */
    public Map<String, String> getRestrictions() {
        return restrictions;
    }

    /**
     * Gets the current revision.
     * 
     * @return the curRev
     */
    public Revision getCurRev() {
        return curRev;
    }

    /**
     * Sets the current revision.
     * 
     * @param curRev the curRev to set
     */
    public void setCurRev(Revision curRev) {
        this.curRev = curRev;
    }

    /**
     * Sets the title of the page.
     * 
     * @param title the title to set
     */
    public void setTitle(String title) {
        this.title = title;
    }

    /**
     * Sets the page's ID.
     * 
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }

    /**
     * Sets whether the page is a redirect.
     * 
     * @param redirect the redirect to set
     */
    public void setRedirect(boolean redirect) {
        this.redirect = redirect;
    }

    /**
     * Sets page restrictions.
     * 
     * @param restrictions the restrictions to set
     */
    public void setRestrictions(Map<String, String> restrictions) {
        this.restrictions = restrictions;
    }

    /**
     * Checks if a user is allows to edit the given page.
     * 
     * @param username
     *            the name of a user
     * 
     * @return whether edit is allowed for the user or not
     */
    public boolean checkEditAllowed(String username) {
        // System.out.println(result.page.getRestrictions());
        String all = restrictions.get("all");
        if (all != null && !all.equals(username)) {
            return false;
        }
        String edit = restrictions.get("edit");
        if (edit != null && !edit.equals(username)) {
            return false;
        }
        return true;
    }

    /**
     * Converts a restrictions map to its corresponding string.
     * 
     * @param restrictions
     *            a map of restrictions
     * 
     * @return a string of the form <tt>key1=value1,key2=value2</tt>
     */
    public static String restrictionsToString(Map<String, String> restrictions) {
        String restrictionsStr;
        if (restrictions.isEmpty()) {
            restrictionsStr = "";
        } else {
            StringBuilder sb = new StringBuilder();
            for (Entry<String, String> restr : restrictions.entrySet()) {
                sb.append(restr.getKey());
                sb.append('=');
                sb.append(restr.getValue());
                sb.append(',');
            }
            restrictionsStr = sb.substring(0, sb.length() - 1);
        }
        return restrictionsStr;
    }

    /**
     * Converts a restrictions string to its corresponding map.
     * 
     * @param restrictionsStr
     *            a string of the form <tt>key1=value1,key2=value2</tt>
     * 
     * @return a map of restrictions
     */
    public static Map<String, String> restrictionsFromString(String restrictionsStr) {
        return restrictionsFromString(restrictionsStr, ":");
    }

    /**
     * Converts a restrictions string to its corresponding map.
     * 
     * @param restrictionsStr
     *            a string of the form
     *            <tt>key1=value1&lt;SEPARATOR&gt;key2=value2</tt>
     * @param separator
     *            the separator between key value pairs
     * 
     * @return a map of restrictions
     */
    public static Map<String, String> restrictionsFromString(
            String restrictionsStr, final String separator) {
        LinkedHashMap<String, String> restrictions_map = new LinkedHashMap<String, String>();
        if (!restrictionsStr.isEmpty()) {
            String[] restrictions_array = restrictionsStr.split(separator);
            for (int i = 0; i < restrictions_array.length; ++i) {
                String[] restriction = restrictions_array[i].split("=");
                if (restriction.length == 2) {
                    restrictions_map.put(restriction[0], restriction[1]);
                } else if (restriction.length == 1) {
                    restrictions_map.put("all", restriction[0]);
                } else {
                    System.err.println("Unknown restriction: " + restrictions_array[i]);
                }
            }
        }
        return restrictions_map;
    }
}
