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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.zib.scalaris.examples.wikipedia.data.Revision;

/**
 * Provides abilities to read an xml wiki dump file and write Wiki pages to
 * Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class Main {
    /**
     * Default blacklist - pages with these names are not imported
     */
    public final static Set<String> blacklist = new HashSet<String>();
    
    /**
     * The main function of the application. Some articles are blacklisted and
     * will not be processed (see implementation for a list of them).
     * 
     * @param args
     *            first argument should be the xml file to convert.
     */
    public static void main(String[] args) {
        try {
            String filename = args[0];
            
            if (args.length > 1) {
                if (args[1].equals("filter")) {
                    doFilter(filename, Arrays.copyOfRange(args, 2, args.length));
                } else if (args[1].equals("import")) {
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length));
                }
            }
        } catch (SAXException e) {
            System.err.println(e.getMessage());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Imports all pages in the Wikipedia XML dump from the given file to Scalaris.
     * 
     * @param args
     * @param reader
     * @param filename
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doImport(String filename, String[] args) throws RuntimeException, IOException,
            SAXException, FileNotFoundException {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        
        int maxRevisions = -1;
        if (args.length >= 1) {
            try {
                maxRevisions = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("no number: " + args[0]);
                System.exit(-1);
            }
        }
        
        // a timestamp in ISO8601 format
        Calendar maxTime = null;
        if (args.length >= 2 && !args[1].isEmpty()) {
            try {
                maxTime = Revision.stringToCalendar(args[1]);
            } catch (IllegalArgumentException e) {
                System.err.println("no date in ISO8601: " + args[1]);
                System.exit(-1);
            }
        }
        
        Set<String> whitelist = null;
        if (args.length >= 3 && !args[2].isEmpty()) {
            FileReader inFile = new FileReader(args[2]);
            BufferedReader br = new BufferedReader(inFile);
            whitelist = new HashSet<String>();
            String line;
            while ((line = br.readLine()) != null) {
                if (!line.isEmpty()) {
                    whitelist.add(line);
                }
            }
            if (whitelist.isEmpty()) {
                whitelist = null;
            }
        }
        
        // add a pre-process step first?
        boolean preprocess = true;
        if (args.length >= 4 && !args[3].isEmpty()) {
            preprocess = Boolean.parseBoolean(args[3]);
        }

        WikiDumpHandler handler;
        if (preprocess) {
//            handler = new WikiDumpPrepareScalarisFilesHandler(blacklist, whitelist, maxRevisions, maxTime, filename + "-tmp");
            handler = new WikiDumpPreparedToScalarisWithSQLiteHandler(blacklist, whitelist, maxRevisions, maxTime, filename + "-tmp");
        } else {
            handler = new WikiDumpToScalarisHandler(blacklist, whitelist, maxRevisions, maxTime);
        }
        handler.setUp();
        Runtime.getRuntime().addShutdownHook(handler.new ReportAtShutDown());
        reader.setContentHandler(handler);
        reader.parse(getFileReader(filename));
        handler.tearDown();
    }

    /**
     * Filters all pages in the Wikipedia XML dump from the given file and
     * creates a list of page names belonging to certain categories.
     * 
     * @param args
     * @param reader
     * @param filename
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doFilter(String filename, String[] args) throws RuntimeException, IOException,
            SAXException, FileNotFoundException {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        
        int recursionLvl = 1;
        if (args.length >= 1) {
            try {
                recursionLvl = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("no number: " + args[0]);
                System.exit(-1);
            }
        }
        
        // a timestamp in ISO8601 format
        Calendar maxTime = null;
        if (args.length >= 2 && !args[1].isEmpty()) {
            try {
                maxTime = Revision.stringToCalendar(args[1]);
            } catch (IllegalArgumentException e) {
                System.err.println("no date in ISO8601: " + args[1]);
                System.exit(-1);
            }
        }
        
        Set<String> categories = null;
        if (args.length >= 3) {
            LinkedList<String> rootCategories = new LinkedList<String>(Arrays.asList(args).subList(2, args.length));
            System.out.println("building category tree...");
            
            // need to get all subcategories recursively, as they must be included as well 
            WikiDumpGetCategoryTreeHandler handler = new WikiDumpGetCategoryTreeHandler(blacklist, maxTime);
            handler.setUp();
            Runtime.getRuntime().addShutdownHook(handler.new ReportAtShutDown());
            reader.setContentHandler(handler);
            reader.parse(getFileReader(filename));
            handler.tearDown();
            
            Map<String, Set<String>> categoryTree = handler.getCategories();
            categories = new HashSet<String>();
            while (!rootCategories.isEmpty()) {
                String curCat = rootCategories.removeFirst();
                Set<String> subcats = categoryTree.get(curCat);
                if (subcats != null) {
                    categories.addAll(subcats);
                    rootCategories.addAll(subcats);
                }
            }
        }

        Set<String> pages = new HashSet<String>(categories);
        pages.add("Main Page");
        System.out.println("creating list of pages to import (recursion level: " + recursionLvl + ") ...");
        while (recursionLvl >= 1) {
            // need to get all subcategories recursively, as they must be included as well 
            WikiDumpGetPagesInCategoriesHandler handler = new WikiDumpGetPagesInCategoriesHandler(blacklist, maxTime, categories, pages);
            handler.setUp();
            Runtime.getRuntime().addShutdownHook(handler.new ReportAtShutDown());
            reader.setContentHandler(handler);
            reader.parse(getFileReader(filename));
            handler.tearDown();
            pages.addAll(handler.getPages());
            pages.addAll(handler.getLinksOnPages());
            
            --recursionLvl;
        }
        
        FileWriter outFile = new FileWriter(filename + "-filtered_pagelist.txt");
        PrintWriter out = new PrintWriter(outFile);
        for (String page : pages) {
            out.println(page);
        }
        out.close();
    }
    
    /**
     * Gets an appropriate file reader for the given file.
     * 
     * @param filename
     *            the name of the file
     * 
     * @return a file reader
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static InputSource getFileReader(String filename) throws FileNotFoundException, IOException {
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
