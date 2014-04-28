/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia.plugin.fourcaast;

import java.net.MalformedURLException;
import java.net.URL;

import javax.servlet.ServletConfig;

import de.zib.scalaris.examples.wikipedia.WikiServletContext;
import de.zib.scalaris.examples.wikipedia.plugin.WikiPlugin;

/**
 * Registers the 4CaaSt accounting plugin.
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class FourCaastAccountingPlugin implements WikiPlugin {

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.plugin.WikiPlugin#init(de.zib.scalaris.examples.wikipedia.WikiServletContext, javax.servlet.ServletConfig)
     */
    @Override
    public void init(WikiServletContext servlet, ServletConfig config) {
        System.out.println("Activating 4CaaSt accounting plugin...");
        final String accountingServer = config.getInitParameter("4CaaSt.accounting");
        if (accountingServer != null) {
            try {
                servlet.registerEventHandler(new FourCaastAccounting(servlet,
                        new URL(accountingServer)));
            } catch (MalformedURLException e) {
                System.err.println("Invalid URL in \"4CaaSt.accounting\": " + e.getMessage());
            }
        } else {
            System.err.println("Accounting plugin activated but no \"4CaaSt.accounting\" parameter present - disabled plugin");
        }
    }
}
