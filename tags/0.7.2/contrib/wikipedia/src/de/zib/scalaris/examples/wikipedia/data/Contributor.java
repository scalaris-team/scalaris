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
 * Contributor known as a registered user or as an IP address.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Contributor implements Serializable {
    /**
     * Version for serialisation.
     */
    private static final long serialVersionUID = 1L;
    
    /**
     * IP address or custom name of a contributor, e.g. "127.0.0.1" or
     * "Incubator import".
     */
    
    protected String ip = "";
    
    /**
     * User name (if known).
     */
    protected String user = "";
    
    /**
     * User ID (if known).
     */
    protected int id = -1;

    /**
     * Creates a new (empty) contributor. Use the setters to make it a valid
     * contributor.
     */
    public Contributor() {
    }

    /**
     * Gets the IP address of the contributor.
     * 
     * @return the ip
     */
    public String getIp() {
        return ip;
    }

    /**
     * Gets the contributor's username.
     * 
     * @return the username
     */
    public String getUser() {
        return user;
    }

    /**
     * Gets the contributor's ID.
     * 
     * @return the id
     */
    public int getId() {
        return id;
    }

    /**
     * Sets the IP of the contributor.
     * 
     * @param ip the ip to set
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * Sets the user name.
     * 
     * @param user the user to set
     */
    public void setUser(String user) {
        this.user = user;
    }

    /**
     * Sets the user ID.
     * 
     * @param id the id to set
     */
    public void setId(int id) {
        this.id = id;
    }
    
    public String toString() {
        if (ip.isEmpty() || !user.isEmpty()) {
            return user;
        } else {
            return ip;
        }
    }
}
