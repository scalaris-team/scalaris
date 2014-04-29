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

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Collection;
import java.util.LinkedList;

/**
 * Generic bean for lists of pages, i.e. page titles.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiPageListBean extends WikiPageBeanBase {
    /**
     * Distinguishes between different form types on the page lists.
     * 
     * @author Nico Kruber, kruber@zib.de
     */
    public enum FormType {
        /**
         * Do not show a form.
         */
        NoForm,
        /**
         * Form with From and To fields.
         */
        FromToForm,
        /**
         * Form for a single page.
         */
        TargetPageForm,
        /**
         * Form for pages with a given prefix.
         */
        PagePrefixForm,
        /**
         * Form for pages with a given substring (for searching).
         */
        PageSearchForm
    }
    private Collection<String> pages = new LinkedList<String>();

    private String fromPage = "";
    private String toPage = "";
    private String formTitle = "Pages";
    private FormType formType = FormType.NoForm;
    /**
     * Title for the heading of the page.
     */
    private String pageHeading = "";
    
    private String target = "";
    private String prefix = "";
    private String search = "";
    private boolean foundFullMatch = false;
    private int namespaceId = 0;
    private boolean showAllPages = false;

    /**
     * Creates a new (empty) bean.
     */
    public WikiPageListBean() {
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
    public WikiPageListBean(String serviceUser, long startTime) {
        super(serviceUser, startTime);
    }

    /**
     * Creates a page bean from a given {@link WikiPageBeanBase}.
     * 
     * @param other
     *            the page bean to copy properties from
     */
    public WikiPageListBean(WikiPageBeanBase other) {
        super(other);
    }

    /**
     * @return the subCategories
     */
    public Collection<String> getPages() {
        return pages;
    }

    /**
     * @param pages the pages to set
     */
    public void setPages(Collection<String> pages) {
        this.pages = pages;
    }

    /**
     * @return the fromChar
     */
    public String getFromPage() {
        return fromPage;
    }

    /**
     * @param fromPage the fromPage to set
     */
    public void setFromPage(String fromPage) {
        this.fromPage = fromPage;
    }

    /**
     * @return the toChar
     */
    public String getToPage() {
        return toPage;
    }

    /**
     * @param toPage the toPage to set
     */
    public void setToPage(String toPage) {
        this.toPage = toPage;
    }

    /**
     * @return the formTitle
     */
    public String getFormTitle() {
        return formTitle;
    }

    /**
     * @param formTitle the formTitle to set
     */
    public void setFormTitle(String formTitle) {
        this.formTitle = formTitle;
    }
    
    /**
     * @return the formType
     */
    public FormType getFormType() {
        return formType;
    }

    /**
     * @param formType the formType to set
     */
    public void setFormType(FormType formType) {
        this.formType = formType;
    }

    /**
     * @return the pageTitle
     */
    public String getPageHeading() {
        return pageHeading;
    }

    /**
     * @param pageHeading the pageTitle to set
     */
    public void setPageHeading(String pageHeading) {
        this.pageHeading = pageHeading;
    }

    /**
     * @return the target
     */
    public String getTarget() {
        return target;
    }

    /**
     * @param target the target to set
     */
    public void setTarget(String target) {
        this.target = target;
    }

    /**
     * @return the prefix
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * @param prefix the prefix to set
     */
    public void setPrefix(String prefix) {
        this.prefix = prefix;
    }

    /**
     * @return the namespaceId
     */
    public int getNamespaceId() {
        return namespaceId;
    }

    /**
     * @param namespaceId the namespaceId to set
     */
    public void setNamespaceId(int namespaceId) {
        this.namespaceId = namespaceId;
    }

    /**
     * @return the search
     */
    public String getSearch() {
        return search;
    }

    /**
     * @param search the search to set
     */
    public void setSearch(String search) {
        this.search = search;
    }

    /**
     * @return the showAllPages
     */
    public boolean isShowAllPages() {
        return showAllPages;
    }

    /**
     * @param showAllPages the showAllPages to set
     */
    public void setShowAllPages(boolean showAllPages) {
        this.showAllPages = showAllPages;
    }
    
    /**
     * Gets a version of the title string with all parameters needed to
     * re-create the form.
     * 
     * Note: Form parameters are URL-encoded, the "&" connecting them are not!
     * 
     * @return a title string with all parameters to be used in a URL
     */
    public String titleWithParameters() {
        try {
        String title = URLEncoder.encode(getTitle(), "UTF-8");
            switch (formType) {
                case PageSearchForm:
                    if (search.isEmpty()) {
                        return title + "&namespace=" + namespaceId;
                    } else {
                        return title 
                                + "&search=" + URLEncoder.encode(search, "UTF-8")
                                + "&namespace=" + namespaceId;
                    }
                case FromToForm:
                    if (!showAllPages && fromPage.isEmpty() && toPage.isEmpty()) {
                        return title + "&namespace=" + namespaceId;
                    } else {
                        return title
                                + "&from=" + URLEncoder.encode(fromPage, "UTF-8")
                                + "&to=" + URLEncoder.encode(toPage, "UTF-8")
                                + "&namespace=" + namespaceId;
                    }
                case PagePrefixForm:
                    if (!showAllPages && prefix.isEmpty()) {
                        return title + "&namespace=" + namespaceId;
                    } else {
                        return title
                                + "&prefix=" + URLEncoder.encode(prefix, "UTF-8")
                                + "&namespace=" + namespaceId;
                    }
                case TargetPageForm:
                    if (!showAllPages && target.isEmpty()) {
                        return title;
                    } else {
                        return title
                                + "&target=" + URLEncoder.encode(target, "UTF-8");
                    }
                default:
            }
            return title;
        } catch (UnsupportedEncodingException e) {
            return "";
        }
    }

    /**
     * @return the searchFoundMatch
     */
    public boolean isFoundFullMatch() {
        return foundFullMatch;
    }

    /**
     * @param foundFullMatch the foundFullMatch to set
     */
    public void setFoundFullMatch(boolean foundFullMatch) {
        this.foundFullMatch = foundFullMatch;
    }
}
