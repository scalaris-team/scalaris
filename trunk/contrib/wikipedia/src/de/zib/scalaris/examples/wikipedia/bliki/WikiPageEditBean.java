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

import java.util.HashSet;
import java.util.Set;

/**
 * Bean with the content to display in the jsp (only for editing articles).
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiPageEditBean extends WikiPageBeanBase {
    /**
     * the page did not exist before
     */
    private boolean isNewPage = false;
    
    /**
     * the preview part of the site
     */
    private String preview = "";
    
    /**
     * set of templates for the review content
     */
    private Set<String> templates = new HashSet<String>();
    
    /**
     * set of transcludes (other than templates) for the review content
     */
    private Set<String> includes = new HashSet<String>();
    
    /**
     * the summary field of the site
     */
    private String summary = "";
    
    /**
     * Creates a new (empty) bean.
     */
    public WikiPageEditBean() {
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
    public WikiPageEditBean(String serviceUser, long startTime) {
        super(serviceUser, startTime);
    }

    /**
     * Creates a page bean from a given {@link WikiPageBeanBase}.
     * 
     * @param other
     *            the page bean to copy properties from
     */
    public WikiPageEditBean(WikiPageBeanBase other) {
        super(other);
    }

    /**
     * @return the preview
     */
    public String getPreview() {
        return preview;
    }

    /**
     * @param preview the preview to set
     */
    public void setPreview(String preview) {
        this.preview = preview;
    }

    /**
     * @return the summary
     */
    public String getSummary() {
        return summary;
    }

    /**
     * @param summary the summary to set
     */
    public void setSummary(String summary) {
        this.summary = summary;
    }

    /**
     * @return the isNewPage
     */
    public boolean isNewPage() {
        return isNewPage;
    }

    /**
     * @param isNewPage the isNewPage to set
     */
    public void setNewPage(boolean isNewPage) {
        this.isNewPage = isNewPage;
    }

    /**
     * @param templates
     */
    public void setTemplates(Set<String> templates) {
        this.templates = templates;
    }

    /**
     * @return the templates
     */
    public Set<String> getTemplates() {
        return templates;
    }

    /**
     * @param includes
     */
    public void setIncludes(Set<String> includes) {
        this.includes = includes;
    }

    /**
     * @return the includes
     */
    public Set<String> getIncludes() {
        return includes;
    }
}
