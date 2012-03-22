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
package de.zib.scalaris.examples.wikipedia.bliki;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.TreeSet;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.xml.sax.InputSource;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.XMLReaderFactory;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.ConnectionPool;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.examples.wikipedia.CircularByteArrayOutputStream;
import de.zib.scalaris.examples.wikipedia.PageHistoryResult;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SavePageResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.examples.wikipedia.data.xml.SAXParsingInterruptedException;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDump;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpHandler;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpPreparedSQLiteToScalaris;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDumpToScalarisHandler;

/**
 * Wiki servlet connecting to Scalaris.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiServletScalaris extends WikiServlet<Connection> {

    private static final long serialVersionUID = 1L;
    private static final int CONNECTION_POOL_SIZE = 100;
    private static final int MAX_WAIT_FOR_CONNECTION = 10000; // 10s
    
    private ConnectionPool cPool;

    /**
     * Default constructor creating the servlet.
     */
    public WikiServletScalaris() {
        super();
    }

    /**
     * Servlet initialisation: creates the connection to the erlang node and
     * imports site information.
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        Properties properties = new Properties();
        try {
            InputStream fis = config.getServletContext().getResourceAsStream("/WEB-INF/scalaris.properties");
            if (fis != null) {
                properties.load(fis);
                properties.setProperty("PropertyLoader.loadedfile", "/WEB-INF/scalaris.properties");
                fis.close();
            } else {
                properties = null;
            }
        } catch (IOException e) {
//            e.printStackTrace();
            properties = null;
        }
        
        ConnectionFactory cFactory;
        if (properties != null) {
            cFactory = new ConnectionFactory(properties);
        } else {
            cFactory = new ConnectionFactory();
            cFactory.setClientName("wiki");
        }
        Random random = new Random();
        String clientName = new BigInteger(128, random).toString(16);
        cFactory.setClientName(cFactory.getClientName() + '_' + clientName);
        cFactory.setClientNameAppendUUID(true);
//        cFactory.setConnectionPolicy(new RoundRobinConnectionPolicy(cFactory.getNodes()));

        cPool = new ConnectionPool(cFactory, CONNECTION_POOL_SIZE);
        loadSiteInfo();
        loadPlugins();
        startExistingPagesUpdate();
    }
    
    /**
     * Loads the siteinfo object from Scalaris.
     * 
     * @return <tt>true</tt> on success,
     *         <tt>false</tt> if not found or no connection available
     */
    @Override
    protected synchronized boolean loadSiteInfo() {
        TransactionSingleOp scalaris_single;
        try {
            Connection conn = cPool.getConnection(MAX_WAIT_FOR_CONNECTION);
            if (conn == null) {
                System.err.println("Could not get a connection to Scalaris for siteinfo, waited " + MAX_WAIT_FOR_CONNECTION + "ms");
                return false;
            }
            scalaris_single = new TransactionSingleOp(conn);
            try {
                siteinfo = scalaris_single.read("siteinfo").jsonValue(SiteInfo.class);
                // TODO: fix siteinfo's base url
                namespace = new MyNamespace(siteinfo);
                initialized = true;
            } catch (Exception e) {
                // no warning here - this probably is an empty wiki
                return false;
            }
        } catch (Exception e) {
            System.out.println(e);
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Sets up the connection to the Scalaris erlang node once on the server.
     * 
     * @param request
     *            request to the servlet or <tt>null</tt> if there is none
     */
    @Override
    protected Connection getConnection(HttpServletRequest request) {
        try {
            Connection conn = cPool.getConnection(MAX_WAIT_FOR_CONNECTION);
            if (conn == null) {
                System.err.println("Could not get a connection to Scalaris, waited " + MAX_WAIT_FOR_CONNECTION + "ms");
                if (request != null) {
                    setParam_error(request, "ERROR: DB unavailable");
                    addToParam_notice(request, "error: <pre>Could not get a connection to Scalaris, waited " + MAX_WAIT_FOR_CONNECTION + "ms</pre>");
                }
                return null;
            }
            return conn;
        } catch (Exception e) {
            if (request != null) {
                setParam_error(request, "ERROR: DB unavailable");
                addToParam_notice(request, "error: <pre>" + e.getMessage() + "</pre>");
            } else {
                System.out.println(e);
                e.printStackTrace();
            }
            return null;
        }
    }

    /**
     * Releases the connection back into the Scalaris connection pool.
     * 
     * @param request
     *            the request to the servlet
     * @param conn
     *            the connection to release
     */
    @Override
    protected void releaseConnection(HttpServletRequest request, Connection conn) {
        cPool.releaseConnection(conn);
    }
    
    /**
     * Shows a page for importing a DB dump.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    @Override
    protected synchronized void showImportPage(HttpServletRequest request,
            HttpServletResponse response, Connection connection)
            throws ServletException, IOException {
        WikiPageBean page = new WikiPageBean();
        page.setNotAvailable(true);
        request.setAttribute("pageBean", page);
        
        StringBuilder content = new StringBuilder();
        String dumpsPath = getServletContext().getRealPath("/WEB-INF/dumps");
        
        if (currentImport.isEmpty() && importHandler == null) {
            TreeSet<String> availableDumps = new TreeSet<String>();
            File dumpsDir = new File(dumpsPath);
            if (dumpsDir.isDirectory()) {
                availableDumps.addAll(Arrays.asList(dumpsDir.list(new FilenameFilter() {
                    @Override
                    public boolean accept(File dir, String name) {
                        return MATCH_WIKI_IMPORT_FILE.matcher(name).matches();
                    }
                })));
            }

            // get parameters:
            String req_import = request.getParameter("import");
            if (req_import == null || !availableDumps.contains(req_import)) {
                content.append("<h2>Please select a wiki dump to import</h2>\n");
                
                content.append("<form method=\"get\" action=\"wiki\">\n");
                content.append("<p>\n");
                content.append("  <select name=\"import\" size=\"10\" style=\"width:500px;\">\n");
                for (String dump: availableDumps) {
                    content.append("   <option>" + dump + "</option>\n");
                }
                content.append("  </select>\n");
                content.append(" </p>\n");
                content.append(" <p>Maximum number of revisions per page: <input name=\"max_revisions\" size=\"2\" value=\"2\" /></br><span style=\"font-size:80%\">(<tt>-1</tt> to import everything)</span></p>\n");
                content.append(" <p>No entry newer than: <input name=\"max_time\" size=\"20\" value=\"\" /></br><span style=\"font-size:80%\">(ISO8601 format, e.g. <tt>2004-01-07T08:09:29Z</tt> - leave empty to import everything)</span></p>\n");
                content.append(" <input type=\"submit\" value=\"Import\" />\n");
                content.append("</form>\n");
                content.append("<p>Note: You will be re-directed to the main page when the import finishes.</p>");
            } else {
                content.append("<h2>Importing \"" + req_import + "\"...</h2>\n");
                try {
                    currentImport = req_import;
                    int maxRevisions = parseInt(request.getParameter("max_revisions"), 2);
                    Calendar maxTime = parseDate(request.getParameter("max_time"), null);
                    importLog = new CircularByteArrayOutputStream(1024 * 1024);
                    PrintStream ps = new PrintStream(importLog);
                    ps.println("starting import...");
                    String fileName = dumpsPath + File.separator + req_import;
                    if (fileName.endsWith(".db")) {
                        importHandler = new WikiDumpPreparedSQLiteToScalaris(fileName, cPool.getConnectionFactory());
                    } else {
                        importHandler = new WikiDumpToScalarisHandler(
                                de.zib.scalaris.examples.wikipedia.data.xml.Main.blacklist,
                                null, maxRevisions, null, maxTime, cPool.getConnectionFactory());
                    }
                    importHandler.setMsgOut(ps);
                    this.new ImportThread(importHandler, fileName, ps).start();
                    response.setHeader("Refresh", "2; url = wiki?import=" + currentImport);
                    content.append("<p>Current log file (refreshed automatically every " + IMPORT_REDIRECT_EVERY + " seconds):</p>\n");
                    content.append("<pre>");
                    content.append("starting import...\n");
                    content.append("</pre>");
                    content.append("<p><a href=\"wiki?import=" + currentImport + "\">refresh</a></p>");
                    content.append("<p><a href=\"wiki?stop_import=" + currentImport + "\">stop</a> (WARNING: pages may be incomplete due to missing templates)</p>");
                } catch (Exception e) {
                    setParam_error(request, "ERROR: import failed");
                    addToParam_notice(request, "error: <pre>" + e.getMessage() + "</pre>");
                    currentImport = "";
                }
            }
        } else {
            content.append("<h2>Importing \"" + currentImport + "\"...</h2>\n");
            
            String req_stop_import = request.getParameter("stop_import");
            boolean stopImport;
            if (req_stop_import == null || req_stop_import.isEmpty()) {
                stopImport = false;
                response.setHeader("Refresh", IMPORT_REDIRECT_EVERY + "; url = wiki?import=" + currentImport);
                content.append("<p>Current log file (refreshed automatically every " + IMPORT_REDIRECT_EVERY + " seconds):</p>\n");
            } else {
                stopImport = true;
                importHandler.stopParsing();
                content.append("<p>Current log file:</p>\n");
            }
            content.append("<pre>");
            String log = importLog.toString();
            int start = log.indexOf("\n");
            if (start != -1) { 
                content.append(log.substring(start));
            }
            content.append("</pre>");
            if (!stopImport) {
                content.append("<p><a href=\"wiki?import=" + currentImport + "\">refresh</a></p>");
                content.append("<p><a href=\"wiki?stop_import=" + currentImport + "\">stop</a> (WARNING: pages may be incomplete due to missing templates)</p>");
            } else {
                content.append("<p>Import has been stopped by the user. Return to <a href=\"wiki?title=" + MAIN_PAGE + "\">" + MAIN_PAGE + "</a>.</p>");
            }
        }

        page.setNotice(WikiServlet.getParam_notice(request));
        page.setError(getParam_error(request));
        page.setTitle("Import Wiki dump");
        page.setPage(content.toString());
        
        RequestDispatcher dispatcher = request.getRequestDispatcher("page.jsp");
        dispatcher.forward(request, response);
    }
    
    private class ImportThread extends Thread {
        private WikiDump handler;
        private String fileName;
        private PrintStream ps;
        
        public ImportThread(WikiDump handler, String fileName, PrintStream ps) {
            this.handler = handler;
            this.fileName = fileName;
            this.ps = ps;
        }
        /* (non-Javadoc)
         * @see java.lang.Thread#run()
         */
        @Override
        public void run() {
            InputSource is = null;
            try {
                handler.setUp();
                if (handler instanceof WikiDumpHandler) {
                    WikiDumpHandler xmlHandler = (WikiDumpHandler) handler;
                    XMLReader reader = XMLReaderFactory.createXMLReader();
                    reader.setContentHandler(xmlHandler);
                    is = de.zib.scalaris.examples.wikipedia.data.xml.Main.getFileReader(fileName);
                    reader.parse(is);
                    xmlHandler.new ReportAtShutDown().run();
                    ps.println("import finished");
                } else if (handler instanceof WikiDumpPreparedSQLiteToScalaris) {
                    WikiDumpPreparedSQLiteToScalaris sqlHandler =
                            (WikiDumpPreparedSQLiteToScalaris) handler;
                    sqlHandler.writeToScalaris();
                    sqlHandler.new ReportAtShutDown().run();
                    
                }
            } catch (Exception e) {
                if (e instanceof SAXParsingInterruptedException) {
                    // this is ok - we told the parser to stop
                } else {
                    e.printStackTrace(ps);
                }
            } finally {
                handler.tearDown();
                if (is != null) {
                    try {
                        is.getCharacterStream().close();
                    } catch (IOException e) {
                        // don't care
                    }
                }
            }
            synchronized (WikiServletScalaris.this) {
                WikiServletScalaris.this.currentImport = "";
                WikiServletScalaris.this.importHandler = null;
            }
        }
    }
    
    @Override
    protected MyScalarisWikiModel getWikiModel(Connection connection) {
        final MyScalarisWikiModel model = new MyScalarisWikiModel(WikiServlet.imageBaseURL,
                WikiServlet.linkBaseURL, connection, namespace);
        model.setExistingPages(existingPages);
        return model;
    }

    @Override
    public String getSiteInfoKey() {
        return ScalarisDataHandler.getSiteInfoKey();
    }

    @Override
    public String getPageListKey() {
        return ScalarisDataHandler.getPageListKey();
    }

    @Override
    public String getPageCountKey() {
        return ScalarisDataHandler.getPageCountKey();
    }

    @Override
    public String getArticleListKey() {
        return ScalarisDataHandler.getArticleListKey();
    }

    @Override
    public String getArticleCountKey() {
        return ScalarisDataHandler.getArticleCountKey();
    }

    @Override
    public String getRevKey(String title, int id, final MyNamespace nsObject) {
        return ScalarisDataHandler.getRevKey(title, id, nsObject);
    }

    @Override
    public String getPageKey(String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getPageKey(title, nsObject);
    }

    @Override
    public String getRevListKey(String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getRevListKey(title, nsObject);
    }

    @Override
    public String getCatPageListKey(String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getCatPageListKey(title, nsObject);
    }

    @Override
    public String getCatPageCountKey(String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getCatPageCountKey(title, nsObject);
    }

    @Override
    public String getTplPageListKey(String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getTplPageListKey(title, nsObject);
    }

    @Override
    public String getBackLinksPageListKey(String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getBackLinksPageListKey(title, nsObject);
    }

    @Override
    public String getStatsPageEditsKey() {
        return ScalarisDataHandler.getStatsPageEditsKey();
    }

    @Override
    public ValueResult<String> getDbVersion(Connection connection) {
        return ScalarisDataHandler.getDbVersion(connection);
    }

    @Override
    public PageHistoryResult getPageHistory(Connection connection, String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getPageHistory(connection, title, nsObject);
    }

    @Override
    public RevisionResult getRevision(Connection connection, String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getRevision(connection, title, nsObject);
    }

    @Override
    public RevisionResult getRevision(Connection connection, String title, int id, final MyNamespace nsObject) {
        return ScalarisDataHandler.getRevision(connection, title, id, nsObject);
    }

    @Override
    public ValueResult<List<String>> getPageList(Connection connection) {
        return ScalarisDataHandler.getPageList(connection);
    }

    @Override
    public ValueResult<List<String>> getArticleList(Connection connection) {
        return ScalarisDataHandler.getArticleList(connection);
    }

    @Override
    public ValueResult<List<String>> getPagesInCategory(Connection connection, String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getPagesInCategory(connection, title, nsObject);
    }

    @Override
    public ValueResult<List<String>> getPagesInTemplate(Connection connection, String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getPagesInTemplate(connection, title, nsObject);
    }

    @Override
    public ValueResult<List<String>> getPagesLinkingTo(Connection connection, String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getPagesLinkingTo(connection, title, nsObject);
    }

    @Override
    public ValueResult<BigInteger> getPageCount(Connection connection) {
        return ScalarisDataHandler.getPageCount(connection);
    }

    @Override
    public ValueResult<BigInteger> getArticleCount(Connection connection) {
        return ScalarisDataHandler.getArticleCount(connection);
    }

    @Override
    public ValueResult<BigInteger> getPagesInCategoryCount(Connection connection, String title, final MyNamespace nsObject) {
        return ScalarisDataHandler.getPagesInCategoryCount(connection, title, nsObject);
    }

    @Override
    public ValueResult<BigInteger> getStatsPageEdits(Connection connection) {
        return ScalarisDataHandler.getStatsPageEdits(connection);
    }

    @Override
    public ValueResult<String> getRandomArticle(Connection connection, Random random) {
        return ScalarisDataHandler.getRandomArticle(connection, random);
    }

    @Override
    public SavePageResult savePage(Connection connection, String title,
            Revision newRev, int prevRevId, Map<String, String> restrictions,
            SiteInfo siteinfo, String username, final MyNamespace nsObject) {
        return ScalarisDataHandler.savePage(connection, title, newRev,
                prevRevId, restrictions, siteinfo, username, nsObject);
    }
}
