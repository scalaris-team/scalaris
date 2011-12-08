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
package de.zib.scalaris.examples.wikipedia;

/**
 * @author Nico Kruber, kruber@zib.de
 *
 */
public class Options {
    /**
     * Use the
     * {@link de.zib.scalaris.Transaction#addDelOnList(String, java.util.List, java.util.List)}
     * and {@link de.zib.scalaris.Transaction#addOnNr(String, Object)} for page
     * list updates.
     */
    public static boolean WIKI_USE_NEW_SCALARIS_OPS = true;
    
    /**
     * Whether to support back-links ("what links here?") or not.
     */
    public static boolean WIKI_USE_BACKLINKS = true;
    
}
