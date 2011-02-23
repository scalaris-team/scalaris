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
package de.zib.tools;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.util.Properties;

/**
 * Provides methods to load property files with default look up mechanisms.
 * 
 * <h3>Example:</h3>
 * <code style="white-space:pre;">
 *   Properties properties = new Properties();
 *   PropertyLoader.loadProperties(properties, "PropertiesFile.properties"); // {@link #loadProperties(java.util.Properties, String)}
 * </code>
 * 
 * @author Nico Kruber, kruber@zib.de
 * @version 1.0
 */
public class PropertyLoader {
	/**
	 * Tries to locate the file given by {@code filename} and loads it into the
	 * given properties parameter.
	 * 
	 * @param properties
	 *            the {@link Properties} object to load the file into
	 * @param filename
	 *            the filename of the file containing the properties
	 * 
	 * @return indicates whether the properties have been successfully loaded
	 */
	public static boolean loadProperties(Properties properties, String filename) {
		FileInputStream fis = null;
		try {
			ClassLoader classLoader = PropertyLoader.class
					.getClassLoader();
			if (classLoader != null) {
				URL url = classLoader.getResource(filename);
				if (url != null) {
					String path = url.getFile();
					fis = new FileInputStream(path);
					properties.load(fis);
					properties.setProperty("PropertyLoader.loadedfile", path);
					fis.close();
					return true;
				}
			}
			// try default path if the file was not found
			fis = new FileInputStream(filename);
			properties.load(fis);
			properties.setProperty("PropertyLoader.loadedfile", filename);
			fis.close();
			return true;
		} catch (FileNotFoundException e) {
			// TODO add logging
			// e.printStackTrace();
		} catch (IOException e) {
			// TODO add logging
			// e.printStackTrace();
		} finally {
			if (fis != null) {
				try {
					fis.close();
				} catch (IOException e) {
				}
			}
		}
		return false;
	}
}
