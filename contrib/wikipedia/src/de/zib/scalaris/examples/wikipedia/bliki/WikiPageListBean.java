/**
 *  Copyright 2011 Zuse Institute Berlin
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

import java.util.LinkedList;
import java.util.List;

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
         * Form with From and To fields.
         */
        FromToForm,
        /**
         * Form for a single page.
         */
        SinglePageForm
    }
    private List<String> pages = new LinkedList<String>();

    private String fromPage = "A";
    private String toPage = "z";
    private String formTitle = "Pages";
    private FormType formType = FormType.FromToForm;
    /**
     * Title for the heading of the page.
     */
    private String pageHeading = "";

    /**
     * @return the subCategories
     */
    public List<String> getPages() {
        return pages;
    }

    /**
     * @param pages the pages to set
     */
    public void setPages(List<String> pages) {
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
}
