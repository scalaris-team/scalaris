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

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Properties;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for the {@link PropertyLoader} class.
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class PropertyLoaderTest {

    /**
     * @throws java.lang.Exception
     */
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @AfterClass
    public static void tearDownAfterClass() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
    }

    /**
     * @throws java.lang.Exception
     */
    @After
    public void tearDown() throws Exception {
    }

    /**
     * Test method for {@link PropertyLoader#loadProperties(Properties, String)}
     * that tries to load a properties file using an relative file name.
     */
    @Test
    public final void testLoadProperties1() {
        final Properties properties = new Properties();
        assertTrue(PropertyLoader.loadProperties(properties, "de/zib/tools/test.properties"));
        assertEquals("ahz2ieSh", properties.get("cs.node"));
        assertEquals("wooPhu8u", properties.get("cs.cookie"));
    }

    /**
     * Test method for {@link PropertyLoader#loadProperties(Properties, String)}
     * that tries to load a properties file using an absolute file name.
     *
     * @throws UnsupportedEncodingException
     *             if the path cannot be decoded using UTF-8
     */
    @Test
    public final void testLoadProperties2() throws UnsupportedEncodingException {
        System.out.println("Working Directory = " + System.getProperty("user.dir"));

        final URL url = ClassLoader.getSystemResource("de/zib/tools/test.properties");
        assertNotNull(url);
        final Properties properties = new Properties();
        assertTrue(PropertyLoader.loadProperties(properties, URLDecoder.decode(url.getFile(), "UTF-8")));
        assertEquals("ahz2ieSh", properties.get("cs.node"));
        assertEquals("wooPhu8u", properties.get("cs.cookie"));
    }

    /**
     * Test method for {@link PropertyLoader#loadProperties(Properties, String)}.
     */
    @Test
    public final void testLoadProperties3() {
        final Properties properties = new Properties();
        assertFalse(PropertyLoader.loadProperties(properties, "de/zib/tools/ahz2ieSh.wooPhu8u"));
    }

}
