/**
 *  Copyright 2007-2011 Zuse Institute Berlin
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
/**
 * This package contains some generic tools useful for most implementations.
 *
 * <h3>The PropertyLoader class</h3>
 * <p>
 * The {@link de.zib.tools.PropertyLoader} class provides methods to load
 * property files with look-up mechanisms to find a file given only by its name.
 * </p>
 *
 * <h4>Example:</h4>
 * <code style="white-space:pre;">
 *   Properties properties = new Properties();
 *   PropertyLoader.loadProperties(properties, "PropertiesFile.properties");
 * </code>
 *
 * <p>See the {@link de.zib.tools.PropertyLoader} class documentation for more
 * details.</p>
 *
 * <h3>MultiMap classes</h3>
 *
 * Provides a multi-map, i.e. a map associating multiple values with a single
 * key.
 *
 * <p>See the {@link de.zib.tools.MultiMap} class documentation for more
 * details.</p>
 */
package de.zib.tools;
