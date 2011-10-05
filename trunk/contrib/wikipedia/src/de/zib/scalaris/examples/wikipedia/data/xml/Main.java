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
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler.ReportAtShutDown;

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
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length), false);
                } else if (args[1].equals("prepare")) {
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length), true);
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
     * @param filename
     * @param args
     * @param prepare
     * 
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doImport(String filename, String[] args, boolean prepare) throws RuntimeException, IOException,
            SAXException, FileNotFoundException {
        
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

        if (prepare) {
            // only prepare the import to Scalaris, i.e. pre-process K/V pairs?
            String dbFileName = "";
            if (args.length >= 4 && !args[3].isEmpty()) {
                dbFileName = args[3];
            } else {
                System.err.println("need a DB file name for prepare; arguments given: " + Arrays.toString(args));
                System.exit(-1);
            }
            WikiDumpHandler handler =
                    new WikiDumpPrepareSQLiteForScalarisHandler(blacklist, whitelist, maxRevisions, maxTime, dbFileName);
            InputSource file = getFileReader(filename);
            runXmlHandler(handler, file);
        } else {
            if (filename.endsWith(".db")) {
                WikiDumpPreparedSQLiteToScalaris handler =
                        new WikiDumpPreparedSQLiteToScalaris(filename);
                handler.setUp();
                WikiDumpPreparedSQLiteToScalaris.ReportAtShutDown shutdownHook = handler.new ReportAtShutDown();
                Runtime.getRuntime().addShutdownHook(shutdownHook);
                handler.writeToScalaris();
                handler.tearDown();
                shutdownHook.run();
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } else {
                WikiDumpHandler handler =
                        new WikiDumpToScalarisHandler(blacklist, whitelist, maxRevisions, maxTime);
                InputSource file = getFileReader(filename);
                runXmlHandler(handler, file);
            }
        }
    }

    /**
     * @param handler
     * @param file
     * @throws SAXException
     * @throws IOException
     */
    static void runXmlHandler(WikiDumpHandler handler, InputSource file)
            throws SAXException, IOException {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        handler.setUp();
        ReportAtShutDown shutdownHook = handler.new ReportAtShutDown();
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        reader.setContentHandler(handler);
        reader.parse(file);
        handler.tearDown();
        shutdownHook.run();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    /**
     * Filters all pages in the Wikipedia XML dump from the given file and
     * creates a list of page names belonging to certain categories.
     * 
     * @param filename
     * @param args
     * 
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doFilter(String filename, String[] args) throws RuntimeException, IOException,
            SAXException, FileNotFoundException {
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
        
        LinkedList<String> rootCategories = new LinkedList<String>();
        if (args.length >= 3) {
            rootCategories = new LinkedList<String>(Arrays.asList(args).subList(2, args.length));
            rootCategories.removeAll(Arrays.asList(""));
        }
        System.out.println("filtering by categories " + rootCategories.toString() + " ...");

        Map<String, Set<String>> categoryTree = new HashMap<String, Set<String>>();
        Map<String, Set<String>> templateTree = new HashMap<String, Set<String>>();
        getCategoryTemplateTrees(filename, maxTime, categoryTree, templateTree);
        Set<String> categories = new HashSet<String>();
        categories.addAll(WikiDumpGetCategoryTreeHandler.getAllChildren(categoryTree, rootCategories));
        
        do {
            FileWriter outFile = new FileWriter(filename + "-allowed_cats.txt");
            PrintWriter out = new PrintWriter(outFile);
            for (String category : categories) {
                out.println(category);
            }
            out.close();
        } while(false);

        Set<String> pages = new HashSet<String>(categories);
        pages.add("Main Page");
        System.out.println("creating list of pages to import (recursion level: " + recursionLvl + ") ...");
        while (recursionLvl >= 1) {
            // note: need to create a new file for each pass because it is being
            // closed at the end of a pass
            InputSource file = getFileReader(filename);
            // need to get all subcategories recursively, as they must be included as well 
            WikiDumpGetPagesInCategoriesHandler handler =
                    new WikiDumpGetPagesInCategoriesHandler(blacklist, maxTime, categoryTree, templateTree, categories, pages);
            runXmlHandler(handler, file);
            pages.addAll(handler.getPages());
            pages.addAll(handler.getLinksOnPages());
            
            --recursionLvl;
        }

        do {
            FileWriter outFile = new FileWriter(filename + "-filtered_pagelist.txt");
            PrintWriter out = new PrintWriter(outFile);
            for (String page : pages) {
                out.println(page);
            }
            out.close();
        } while(false);
    }

    /**
     * Gets the category and template trees from a file, i.e.
     * <tt>filename + "-trees.bin"</tt>, or if this does not exist, builds the
     * trees and stores them to this file.
     * 
     * @param filename
     *            the name of the xml wiki dump file
     * @param maxTime
     *            the maximum time of a revision to use for category parsing
     * @param categoryTree
     *            the (preferably empty) category tree object
     * @param templateTree
     *            the (preferably empty) template tree object
     * 
     * @throws RuntimeException
     * @throws FileNotFoundException
     * @throws IOException
     * @throws SAXException
     */
    protected static void getCategoryTemplateTrees(String filename,
            Calendar maxTime,
            Map<String, Set<String>> categoryTree,
            Map<String, Set<String>> templateTree) throws RuntimeException,
            FileNotFoundException, IOException, SAXException {
        File trees = new File(filename + "-trees.bin.gz");
        if (trees.exists()) {
            // read trees from tree file
            System.out.println("reading category tree from " + trees.getName() + " ...");
            ObjectInputStream ois = new ObjectInputStream(new GZIPInputStream(
                    new FileInputStream(trees)));
            try {
                @SuppressWarnings("unchecked")
                Map<String, Set<String>> catTree =
                        (Map<String, Set<String>>) ois.readObject();
                categoryTree.putAll(catTree);
                @SuppressWarnings("unchecked")
                Map<String, Set<String>> tplTree =
                        (Map<String, Set<String>>) ois.readObject();
                templateTree.putAll(tplTree);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            } finally {
                ois.close();
            }
        } else {
            // build trees from xml file
            // need to get all subcategories recursively, as they must be included as well 
            System.out.println("building category tree from " + filename + " ...");
            WikiDumpGetCategoryTreeHandler handler = new WikiDumpGetCategoryTreeHandler(blacklist, maxTime);
            InputSource file = getFileReader(filename);
            runXmlHandler(handler, file);
            categoryTree.putAll(handler.getCategories());
            templateTree.putAll(handler.getTemplates());
            
            // save trees to tree file
            System.out.println("saving category tree to " + trees.getName() + " ...");
            ObjectOutputStream oos = new ObjectOutputStream(
                    new GZIPOutputStream(new FileOutputStream(trees)));
            try {
                oos.writeObject(categoryTree);
                oos.writeObject(templateTree);
            } finally {
                oos.close();
            }
        }
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
