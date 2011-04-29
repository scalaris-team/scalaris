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
 * @author Nico Kruber, kruber@zib.de
 *
 */
public class WikiPageListBean extends WikiPageBeanBase {
    private List<String> pages = new LinkedList<String>();
    private String fromPage = "A";
    private String toPage = "z";

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
}
