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
package de.zib.scalaris.examples.wikipedia.data.xml;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintStream;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Provides abilities to read an xml wiki dump file and write its contents to
 * Scalaris by pre-processing key/value pairs into several files.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiDumpPreparedToScalarisWithFilesHandler extends WikiDumpPreparedToScalarisHandler {
    NumberFormat formatter = new DecimalFormat("0000000000");
    Map<String, String> keyToFileMap = new HashMap<String, String>();

    /**
     * Sets up a SAX XmlHandler exporting all parsed pages except the ones in a
     * blacklist to stdout.
     * 
     * @param blacklist
     *            a number of page titles to ignore
     * @param whitelist
     *            only import these pages
     * @param maxRevisions
     *            maximum number of revisions per page (starting with the most
     *            recent) - <tt>-1/tt> imports all revisions
     *            (useful to speed up the import / reduce the DB size)
     * @param maxTime
     *            maximum time a revision should have (newer revisions are
     *            omitted) - <tt>null/tt> imports all revisions
     *            (useful to create dumps of a wiki at a specific point in time)
     * @param dirname
     *            the name of the directory to write temporary files to
     * 
     * @throws RuntimeException
     *             if the connection to Scalaris fails
     */
    public WikiDumpPreparedToScalarisWithFilesHandler(Set<String> blacklist,
            Set<String> whitelist, int maxRevisions, Calendar maxTime,
            String dirname) throws RuntimeException {
        super(blacklist, whitelist, maxRevisions, maxTime, dirname);
    }

    /**
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    protected <T> void writeObject(String key, T value, String fileName)
            throws RuntimeException {
        try {
            ObjectOutputStream oos = new ObjectOutputStream(
                    new GZIPOutputStream(new FileOutputStream(dirname
                            + File.separatorChar + fileName + ".bin.gz")));
            oos.writeObject(value);
            oos.flush();
            oos.close();
            keyToFileMap.put(key, fileName);
        } catch (FileNotFoundException e) {
            System.err.println("write of " + key + " failed (file not found)");
            // file not found should not occur during writes and should not happen
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("write of " + key + " failed");
        }
    }

    /**
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Override
    protected <T> void writeObject(String key, T value)
            throws RuntimeException {
        String fileName = keyToFileMap.get(key);
        if (fileName == null) {
            writeObject(key, value, formatter.format(lastFile++));
        } else {
            writeObject(key, value, fileName);
        }
    }

    /**
     * @param <T>
     * @param siteinfo
     * @param key
     * @throws IOException
     * @throws FileNotFoundException
     */
    @Override
    protected <T> T readObject(String key)
            throws RuntimeException, FileNotFoundException {
        String fileName = keyToFileMap.get(key);
        try {
            ObjectInputStream ois = new ObjectInputStream(
                    new GZIPInputStream(new FileInputStream(dirname
                            + File.separatorChar + fileName + ".bin.gz")));
            @SuppressWarnings("unchecked")
            T result = (T) ois.readObject();
            ois.close();
            return result;
        } catch (FileNotFoundException e) {
            throw e;
        } catch (ClassNotFoundException e) {
            System.err.println("read of " + key + " failed (class not found)");
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.err.println("read of " + key + " failed");
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void updatePageLists() {
        super.updatePageLists();
        try {
            TreeSet<String> sortedKeys = new TreeSet<String>(keyToFileMap.keySet());
            PrintStream ps = new PrintStream(new FileOutputStream(dirname
                    + File.separatorChar + "keys.txt"));
            for (String key: sortedKeys) {
                ps.println(key);
            }
            ps.flush();
            ps.close();

            TreeSet<String> sortedFiles = new TreeSet<String>(keyToFileMap.values());
            ps = new PrintStream(new FileOutputStream(dirname
                    + File.separatorChar + "files.txt"));
            for (String file: sortedFiles) {
                ps.println(file);
            }
            ps.flush();
            ps.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    protected void writeToScalaris() {
        try {
            Set<String> keys = keyToFileMap.keySet();
            for (String key: keys) {
                writeToScalaris(key, readObject(key));
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}
