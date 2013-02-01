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
package de.zib.scalaris.examples.wikipedia.plugin;

import javax.servlet.ServletConfig;

import de.zib.scalaris.examples.wikipedia.WikiServletContext;

/**
 * Simple plug-in interface for plug-ins in the {@link de.zib.scalaris.examples.wikipedia.bliki.WikiServlet} class.
 * 
 * Note: this API is not stable and will probably change in future.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public interface WikiPlugin {
    /**
     * Initialises the plugin.
     * 
     * @param servlet
     *            the servlet using the plugin
     * @param config
     *            servlet config object
     */
    public void init(WikiServletContext servlet, ServletConfig config);
}
