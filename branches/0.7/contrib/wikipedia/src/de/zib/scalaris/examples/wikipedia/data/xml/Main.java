/**
 *  Copyright 2007-2013 Zuse Institute Berlin
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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import java.util.zip.GZIPInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import de.zib.scalaris.examples.wikipedia.data.xml.util.SevenZInputStream;

import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.zib.scalaris.examples.wikipedia.Options;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
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
    
    private static enum ImportType {
        IMPORT_XML,
        IMPORT_DB,
        PREPARE_DB,
        XML_2_DB
    }
    
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
                } else if (args[1].equals("import-xml")) {
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length), ImportType.IMPORT_XML);
                } else if (args[1].equals("import-db")) {
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length), ImportType.IMPORT_DB);
                } else if (args[1].equals("prepare")) {
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length), ImportType.PREPARE_DB);
                } else if (args[1].equals("xml2db")) {
                    doImport(filename, Arrays.copyOfRange(args, 2, args.length), ImportType.XML_2_DB);
                } else if (args[1].equals("convert")) {
                    doConvert(filename, Arrays.copyOfRange(args, 2, args.length));
                } else if (args[1].equals("dumpdb-addlinks")) {
                    doDumpdbAddlinks(filename, Arrays.copyOfRange(args, 2, args.length));
                } else if (args[1].equals("dumpdb-filter")) {
                    doDumpdbFilter(filename, Arrays.copyOfRange(args, 2, args.length));
                }
            }
        } catch (SAXException e) {
            System.err.println(e.getMessage());
            System.exit(-1);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            System.exit(-1);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    /**
     * Imports all pages in the Wikipedia XML dump from the given file to Scalaris.
     * 
     * @param filename
     * @param args
     * @param type
     * 
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doImport(String filename, String[] args, ImportType type) throws RuntimeException, IOException,
            SAXException, FileNotFoundException {
        int i = 0;
        
        if (type == ImportType.IMPORT_DB) {
            int numberOfImporters = 1;
            if (args.length > i) {
                try {
                    numberOfImporters = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("no number: " + args[i]);
                    System.exit(-1);
                }
            }
            ++i;
            
            int myNumber = 1;
            if (args.length > i) {
                try {
                    myNumber = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("no number: " + args[i]);
                    System.exit(-1);
                }
            }
            ++i;
            
            Options options = null;
            if (args.length > i) {
                if (args[i].length() > 0) {
                    options = Options.getInstance();
                    Options.parseOptions(options, args[i]);
                }
            }
            ++i;
            
            if (filename.endsWith(".db") && numberOfImporters > 0 && myNumber > 0) {
                WikiDumpHandler.println(System.out, "wiki import from " + filename);
                WikiDumpHandler.println(System.out, " importers   : " + numberOfImporters);
                WikiDumpHandler.println(System.out, " my import nr: " + myNumber);
                WikiDumpPreparedSQLiteToScalaris handler =
                        new WikiDumpPreparedSQLiteToScalaris(filename, options, numberOfImporters, myNumber);
                handler.setUp();
                WikiDumpPreparedSQLiteToScalaris.ReportAtShutDown shutdownHook = handler.new ReportAtShutDown();
                Runtime.getRuntime().addShutdownHook(shutdownHook);
                handler.writeToScalaris();
                handler.tearDown();
                shutdownHook.reportAtEnd();
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
                exitCheckHandler(handler);
            } else {
                System.err.println("incorrect command line parameters");
                System.exit(-1);
            }
            return;
        }
        
        int maxRevisions = -1;
        if (args.length > i) {
            try {
                maxRevisions = Integer.parseInt(args[i]);
            } catch (NumberFormatException e) {
                System.err.println("no number: " + args[i]);
                System.exit(-1);
            }
        }
        ++i;
        
        // a timestamp in ISO8601 format
        Calendar minTime = null;
        if (args.length > i && !args[i].isEmpty()) {
            try {
                minTime = Revision.stringToCalendar(args[i]);
            } catch (IllegalArgumentException e) {
                System.err.println("no date in ISO8601: " + args[i]);
                System.exit(-1);
            }
        }
        ++i;
        
        // a timestamp in ISO8601 format
        Calendar maxTime = null;
        if (args.length > i && !args[i].isEmpty()) {
            try {
                maxTime = Revision.stringToCalendar(args[i]);
            } catch (IllegalArgumentException e) {
                System.err.println("no date in ISO8601: " + args[i]);
                System.exit(-1);
            }
        }
        ++i;
        
        Set<String> whitelist = null;
        String whitelistFile = "";
        if (args.length > i && !args[i].isEmpty()) {
            whitelistFile = args[i];
            whitelist = new HashSet<String>();
            addFromFile(whitelist, whitelistFile);
            if (whitelist.isEmpty()) {
                whitelist = null;
            }
        }
        ++i;

        if (type == ImportType.PREPARE_DB || type == ImportType.XML_2_DB) {
            // only prepare the import to Scalaris, i.e. pre-process K/V pairs?
            String dbFileName = "";
            if (args.length > i && !args[i].isEmpty()) {
                dbFileName = args[i];
            } else {
                System.err.println("need a DB file name for prepare; arguments given: " + Arrays.toString(args));
                System.exit(-1);
            }
            ++i;
            WikiDumpHandler.println(System.out, "wiki prepare file " + dbFileName);
            WikiDumpHandler.println(System.out, " wiki dump     : " + filename);
            WikiDumpHandler.println(System.out, " white list    : " + whitelistFile);
            WikiDumpHandler.println(System.out, " max revisions : " + maxRevisions);
            WikiDumpHandler.println(System.out, " min time      : " + (minTime == null ? "null" : Revision.calendarToString(minTime)));
            WikiDumpHandler.println(System.out, " max time      : " + (maxTime == null ? "null" : Revision.calendarToString(maxTime)));
            WikiDumpHandler handler = null;
            switch (type) {
                case PREPARE_DB:
                    handler = new WikiDumpPrepareSQLiteForScalarisHandler(
                            blacklist, whitelist, maxRevisions, minTime,
                            maxTime, dbFileName);
                    break;
                case XML_2_DB:
                    handler = new WikiDumpXml2SQLite(blacklist, whitelist,
                            maxRevisions, minTime, maxTime, dbFileName);
                    break;
                default:
                    throw new RuntimeException();
            }
            runXmlHandler(handler, getFileReader(filename));
        } else if (type == ImportType.IMPORT_XML) {
            WikiDumpHandler.println(System.out, "wiki import from " + filename);
            WikiDumpHandler.println(System.out, " white list    : " + whitelistFile);
            WikiDumpHandler.println(System.out, " max revisions : " + maxRevisions);
            WikiDumpHandler.println(System.out, " min time      : " + (minTime == null ? "null" : Revision.calendarToString(minTime)));
            WikiDumpHandler.println(System.out, " max time      : " + (maxTime == null ? "null" : Revision.calendarToString(maxTime)));
            WikiDumpHandler handler = new WikiDumpToScalarisHandler(
                    blacklist, whitelist, maxRevisions, minTime, maxTime);
            runXmlHandler(handler, getFileReader(filename));
        }
    }

    /**
     * Exits from the VM if the handler had an error in the previous import
     * step.
     * 
     * @param handler the handler used for the import
     */
    protected static void exitCheckHandler(WikiDump handler) {
        if (handler.isErrorDuringImport()) {
            System.exit(-1);
        }
    }

    /**
     * Converts the default prepared SQLite dump to a different optimisation scheme.
     * 
     * @param dbReadFileName
     * @param args 
     * 
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doConvert(String dbReadFileName, String[] args)
            throws RuntimeException, FileNotFoundException {
        
        int i = 0;
        String dbWriteFileName;
        if (args.length > i) {
            dbWriteFileName = args[i];
        } else {
            System.err.println("need an new DB file name for convert; arguments given: " + Arrays.toString(args));
            System.exit(-1);
            return;
        }
        ++i;
        
        String dbWriteOptionsStr;
        Options dbWriteOptions = new Options();
        if (args.length > i) {
            dbWriteOptionsStr = args[i];
            Options.parseOptions(dbWriteOptions, null, null, null, null, null, null, null, null, dbWriteOptionsStr, null, null);
        } else {
            System.err.println("need a new optimisation scheme for convert; arguments given: " + Arrays.toString(args));
            System.exit(-1);
            return;
        }
        ++i;

        WikiDumpHandler.println(System.out, "converting");
        WikiDumpHandler.println(System.out, " from    : " + dbReadFileName);
        WikiDumpHandler.println(System.out, " to      : " + dbWriteFileName);
        WikiDumpHandler.println(System.out, " options : " + dbWriteOptionsStr);
        
        WikiDumpConvertPreparedSQLite handler = new WikiDumpConvertPreparedSQLite(dbReadFileName, dbWriteFileName, dbWriteOptions);
        handler.setUp();
        WikiDumpConvertPreparedSQLite.ReportAtShutDown shutdownHook = handler.new ReportAtShutDown();
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        handler.convertObjects();
        handler.tearDown();
        shutdownHook.reportAtEnd();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        exitCheckHandler(handler);
    }

    /**
     * processed a SQLite DB created from xml2db and parses all links, i.e.
     * interconnections between pages of the wiki.
     * 
     * @param dbReadFileName
     * @param args 
     * 
     * @throws RuntimeException
     * @throws IOException
     * @throws SAXException
     * @throws FileNotFoundException
     */
    private static void doDumpdbAddlinks(String dbReadFileName, String[] args)
            throws RuntimeException {
        WikiDumpHandler.println(System.out, "adding links to: " + dbReadFileName);
        
        WikiDumpSQLiteLinkTables handler = new WikiDumpSQLiteLinkTables(dbReadFileName);
        handler.setUp();
        WikiDumpSQLiteLinkTables.ReportAtShutDown shutdownHook = handler.new ReportAtShutDown();
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        handler.processLinks();
        handler.tearDown();
        shutdownHook.reportAtEnd();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        exitCheckHandler(handler);
    }

    /**
     * Filters all pages in the Wikipedia XML2DB dump from the given file and
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
    private static void doDumpdbFilter(String filename, String[] args) throws RuntimeException, IOException,
            SAXException, FileNotFoundException {
        int i = 0;
        int recursionLvl = 1;
        if (args.length > i) {
            try {
                recursionLvl = Integer.parseInt(args[i]);
            } catch (NumberFormatException e) {
                System.err.println("no number: " + args[i]);
                System.exit(-1);
            }
        }
        ++i;

        String pageListFileName = "";
        if (args.length > i && !args[i].isEmpty()) {
            pageListFileName = args[i];
        } else {
            System.err.println("need a pagelist file name for filter; arguments given: " + Arrays.toString(args));
            System.exit(-1);
        }
        ++i;
        
        Set<String> allowedPages0 = new HashSet<String>();
        allowedPages0.add("Main Page");
        String allowedPagesFileName = "";
        if (args.length > i && !args[i].isEmpty()) {
            allowedPagesFileName = args[i];
            addFromFile(allowedPages0, allowedPagesFileName);
        }
        ++i;
        
        LinkedList<String> rootCategories = new LinkedList<String>();
        if (args.length > i) {
            for (String rCat : Arrays.asList(args).subList(i, args.length)) {
                if (!rCat.isEmpty()) {
                    rootCategories.add(rCat);
                }
            }
        }
        WikiDumpHandler.println(System.out, "filtering by categories " + rootCategories.toString() + " ...");
        WikiDumpHandler.println(System.out, " wiki dump     : " + filename);
        WikiDumpHandler.println(System.out, " allowed pages : " + allowedPagesFileName);
        WikiDumpHandler.println(System.out, " recursion lvl : " + recursionLvl);
        
        WikiDumpHandler.println(System.out, "creating list of pages to import (recursion level: " + recursionLvl + ") ...");
        Set<String> allowedCats0 = new HashSet<String>(rootCategories);
        
        WikiDumpSQLiteLinkTables handler = new WikiDumpSQLiteLinkTables(filename);
        handler.setUp();
        SortedSet<String> pages = handler.getPagesInCategories(allowedCats0,
                allowedPages0, recursionLvl, false);
        handler.tearDown();

        do {
            FileWriter outFile = new FileWriter(pageListFileName);
            PrintWriter out = new PrintWriter(outFile);
            for (String page : pages) {
                out.println(page);
            }
            out.close();
        } while(false);
        exitCheckHandler(handler);
    }

    /**
     * @param handler
     * @param file
     * @throws SAXException
     * @throws IOException
     */
    private static void runXmlHandler(WikiDumpHandler handler, InputSource[] files)
            throws SAXException, IOException {
        XMLReader reader = XMLReaderFactory.createXMLReader();
        handler.setUp();
        ReportAtShutDown shutdownHook = handler.new ReportAtShutDown();
        Runtime.getRuntime().addShutdownHook(shutdownHook);
        reader.setContentHandler(handler);
        for (InputSource file : files) {
            reader.parse(file);
        }
        handler.tearDown();
        shutdownHook.reportAtEnd();
        Runtime.getRuntime().removeShutdownHook(shutdownHook);
        exitCheckHandler(handler);
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
        int i = 0;
        int recursionLvl = 1;
        if (args.length > i) {
            try {
                recursionLvl = Integer.parseInt(args[i]);
            } catch (NumberFormatException e) {
                System.err.println("no number: " + args[i]);
                System.exit(-1);
            }
        }
        ++i;
        
        // a timestamp in ISO8601 format
        Calendar maxTime = null;
        if (args.length > i && !args[i].isEmpty()) {
            try {
                maxTime = Revision.stringToCalendar(args[i]);
            } catch (IllegalArgumentException e) {
                System.err.println("no date in ISO8601: " + args[i]);
                System.exit(-1);
            }
        }
        ++i;

        String pageListFileName = "";
        if (args.length > i && !args[i].isEmpty()) {
            pageListFileName = args[i];
        } else {
            System.err.println("need a pagelist file name for filter; arguments given: " + Arrays.toString(args));
            System.exit(-1);
        }
        ++i;
        
        Set<String> allowedPages = new HashSet<String>();
        allowedPages.add("Main Page");
        String allowedPagesFileName = "";
        if (args.length > i && !args[i].isEmpty()) {
            allowedPagesFileName = args[i];
            addFromFile(allowedPages, allowedPagesFileName);
        }
        ++i;
        
        LinkedList<String> rootCategories = new LinkedList<String>();
        if (args.length > i) {
            for (String rCat : Arrays.asList(args).subList(i, args.length)) {
                if (!rCat.isEmpty()) {
                    rootCategories.add(rCat);
                }
            }
        }
        WikiDumpHandler.println(System.out, "filtering by categories " + rootCategories.toString() + " ...");
        WikiDumpHandler.println(System.out, " wiki dump     : " + filename);
        WikiDumpHandler.println(System.out, " max time      : " + maxTime);
        WikiDumpHandler.println(System.out, " allowed pages : " + allowedPagesFileName);
        WikiDumpHandler.println(System.out, " recursion lvl : " + recursionLvl);
        SortedSet<String> pages = getPageList(filename, maxTime, allowedPages,
                rootCategories, recursionLvl);

        do {
            FileWriter outFile = new FileWriter(pageListFileName);
            PrintWriter out = new PrintWriter(outFile);
            for (String page : pages) {
                out.println(page);
            }
            out.close();
        } while(false);
    }

    private static void addFromFile(Collection<String> container,
            String fileName) throws FileNotFoundException, IOException {
        FileReader inFile = new FileReader(fileName);
        BufferedReader br = new BufferedReader(inFile);
        String line;
        while ((line = br.readLine()) != null) {
            if (!line.isEmpty()) {
                container.add(line);
            }
        }
        br.close();
    }

    /**
     * Extracts all allowed pages in the given root categories as well as those
     * pages explicitly mentioned in a list of allowed pages.
     * 
     * Gets the category and template trees from a file, i.e.
     * <tt>filename + "-trees.db"</tt>, or if this does not exist, builds the
     * trees and stores them to this file.
     * 
     * @param filename
     *            the name of the xml wiki dump file
     * @param maxTime
     *            the maximum time of a revision to use for category parsing
     * @param allowedPages
     *            all allowed pages (un-normalised page titles)
     * @param rootCategories
     *            all allowed categories (un-normalised page titles)
     * @param recursionLvl
     *            recursion level to include pages linking to
     * 
     * @return a set of (de-normalised) page titles
     * 
     * @throws RuntimeException
     * @throws FileNotFoundException
     * @throws IOException
     * @throws SAXException
     */
    protected static SortedSet<String> getPageList(String filename, Calendar maxTime,
            Set<String> allowedPages, LinkedList<String> rootCategories,
            int recursionLvl) throws RuntimeException, FileNotFoundException,
            IOException, SAXException {
        Map<NormalisedTitle, Set<NormalisedTitle>> templateTree = new HashMap<NormalisedTitle, Set<NormalisedTitle>>();
        Map<NormalisedTitle, Set<NormalisedTitle>> includeTree = new HashMap<NormalisedTitle, Set<NormalisedTitle>>();
        Map<NormalisedTitle, Set<NormalisedTitle>> referenceTree = new HashMap<NormalisedTitle, Set<NormalisedTitle>>();

        File trees = new File(filename + "-trees.db");
        if (trees.exists()) {
            // read trees from tree file
            WikiDumpHandler.println(System.out, "reading category tree from " + trees.getAbsolutePath() + " ...");
            WikiDumpGetCategoryTreeHandler.readTrees(trees.getAbsolutePath(),
                    templateTree, includeTree, referenceTree);
        } else {
            // build trees from xml file
            // need to get all subcategories recursively, as they must be
            // included as well
            WikiDumpHandler.println(System.out, "building category tree from " + filename + " ...");
            WikiDumpGetCategoryTreeHandler handler = new WikiDumpGetCategoryTreeHandler(
                    blacklist, null, maxTime, trees.getPath());
            runXmlHandler(handler, getFileReader(filename));
            WikiDumpGetCategoryTreeHandler.readTrees(trees.getAbsolutePath(),
                    templateTree, includeTree, referenceTree);
        }

        WikiDumpHandler.println(System.out, "creating list of pages to import (recursion level: " + recursionLvl + ") ...");
        Set<String> allowedCats = new HashSet<String>(rootCategories);

        return WikiDumpGetCategoryTreeHandler.getPagesInCategories(
                trees.getAbsolutePath(), allowedCats, allowedPages, recursionLvl,
                templateTree, includeTree, referenceTree, System.out, false);
    }
    
    /**
     * Gets appropriate file reader(s) for the given file(s).
     * 
     * @param filename
     *            the name of the file.
     *            multiple files are separated using a pipe
     * 
     * @return a file reader array
     * 
     * @throws FileNotFoundException
     * @throws IOException
     */
    public static InputSource[] getFileReader(String filename) throws FileNotFoundException, IOException {
        String[] files = filename.split("\\|");
        InputSource[] sources = new InputSource[files.length];

        for (int i = 0; i < files.length; i++) {

            String file = files[i];
            InputStream is;

            if (file.endsWith(".xml.gz")) {
                is = new GZIPInputStream(new FileInputStream(file));
            } else if (file.endsWith(".xml.bz2")) {
                is = new BZip2CompressorInputStream(new FileInputStream(file));
            } else if (file.endsWith(".xml.7z")) {
                is = new SevenZInputStream(new File(file));
            } else if (file.endsWith(".xml")) {
                is = new FileInputStream(file);
            } else {
                System.err.println("Unsupported file: " + file
                        + ". Supported: *.xml.gz, *.xml.bz2, *.xml.7z, *.xml");
                System.exit(-1);
                return null; // will never be reached but is necessary to keep javac happy
            }

            BufferedReader br = new BufferedReader(new InputStreamReader(is, "UTF-8"));
            sources[i] = new InputSource(br);
        }

        return sources;
    }
}
