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
package de.zib.scalaris.examples.wikipedia.data.xml;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

/**
 * Provides abilities to read an xml wiki dump file and write Wiki pages to
 * Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Main {
    /**
     * The main function of the application. Some articles are blacklisted and
     * will not be processed (see implementation for a list of them).
     * 
     * @param args
     *            first argument should be the xml file to convert.
     */
    public static void main(String[] args) {
        try {
            Set<String> blacklist = new HashSet<String>();

            XMLReader myReader = XMLReaderFactory.createXMLReader();

            String filename = args[0];
            int maxRevisions = -1;
            if (args.length > 1) {
                try {
                    maxRevisions = Integer.parseInt(args[1]);
                } catch (NumberFormatException e) {
                    System.err.println("no number: " + args[1]);
                    System.exit(-1);
                }
            }

            WikiDumpHandler handler = new WikiDumpToScalarisHandler(blacklist, maxRevisions);
            handler.setUp();
            myReader.setContentHandler(handler);
            myReader.parse(getFileReader(filename));
            handler.tearDown();
        } catch (SAXException e) {
            System.err.println(e.getMessage());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private static InputSource getFileReader(String filename) throws FileNotFoundException, IOException {
        InputStream is;
        if (filename.endsWith(".xml.gz")) {
            is = new GZIPInputStream(new FileInputStream(filename));
        } else if (filename.endsWith(".xml.bz2")) {
            is = new BZip2CompressorInputStream(new FileInputStream(filename));
        } else if (filename.endsWith(".xml")) {
            is = new FileInputStream(filename);
        } else {
            System.err.println("Unsupported file: " + filename + ". Supported: *.xml.gz, *.xml.bz2, *.xml");
            System.exit(-1);
            return null; // will never be reached but is necessary to keep javac happy
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
        return new InputSource(br);
    }
}
