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
package de.zib.scalaris.examples.wikipedia.bliki;

import info.bliki.api.Connector;
import info.bliki.api.User;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;

import de.zib.scalaris.examples.wikipedia.CircularByteArrayOutputStream;
import de.zib.scalaris.examples.wikipedia.NamespaceUtils;
import de.zib.scalaris.examples.wikipedia.Options;
import de.zib.scalaris.examples.wikipedia.PageHistoryResult;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SavePageResult;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.WikiServletContext;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageListBean.FormType;
import de.zib.scalaris.examples.wikipedia.data.Contributor;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDump;
import de.zib.scalaris.examples.wikipedia.plugin.PluginClassLoader;
import de.zib.scalaris.examples.wikipedia.plugin.WikiEventHandler;
import de.zib.scalaris.examples.wikipedia.plugin.WikiPlugin;

/**
 * Servlet for handling wiki page display and editing.
 * 
 * @param <Connection> connection to a DB
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public abstract class WikiServlet<Connection> extends HttpServlet implements
        Servlet, WikiServletContext, WikiServletDataHandler<Connection> {
    protected static final String MAIN_PAGE = "Main Page";
    protected static final int IMPORT_REDIRECT_EVERY = 5; // seconds

    private static final long serialVersionUID = 1L;
    
    /**
     * Version of the "Wikipedia on Scalaris" example implementation.
     */
    public static final String version = "0.4.0+svn";

    protected SiteInfo siteinfo = null;
    protected MyNamespace namespace = null;
    
    protected boolean initialized = false;

    /**
     * Name under which the servlet is available.
     */
    public static final String wikiBaseURL = "wiki";
    /**
     * URL for page links
     */
    public static final String linkBaseURL = wikiBaseURL + "?title=${title}";
    /**
     * URL for image links
     */
    public static final String imageBaseURL = wikiBaseURL + "?get_image=${image}";

    protected static final Pattern MATCH_WIKI_IMPORT_FILE = Pattern.compile(".*((\\.xml(\\.gz|\\.bz2)?)|\\.db)$");
    protected static final Pattern MATCH_WIKI_IMAGE_PX = Pattern.compile("^[0-9]*px-");
    protected static final Pattern MATCH_WIKI_IMAGE_SVG_PNG = Pattern.compile("\\.svg\\.png$");
    /*
     * http://simple.wiktionary.org/wiki/Main_Page
     * http://bar.wikipedia.org/wiki/Hauptseitn
     * https://secure.wikimedia.org/wikipedia/en/wiki/Main_Page
     */
    protected static final Pattern MATCH_WIKI_SITE_BASE = Pattern.compile("^(http[s]?://.+)(/wiki/.*)$");
    
    protected String currentImport = "";

    protected static CircularByteArrayOutputStream importLog = null;
    protected WikiDump importHandler = null;
    
    protected List<WikiEventHandler> eventHandlers = new LinkedList<WikiEventHandler>();

    /**
     * Creates the servlet. 
     */
    public WikiServlet() {
        super();
    }

    /**
     * Servlet initialisation: creates the connection to the erlang node and
     * imports site information.
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
        super.init(config);
        final String WIKI_USE_NEW_SCALARIS_OPS = config.getInitParameter("WIKI_USE_NEW_SCALARIS_OPS");
        if (WIKI_USE_NEW_SCALARIS_OPS != null) {
            Options.WIKI_USE_NEW_SCALARIS_OPS = Boolean.parseBoolean(WIKI_USE_NEW_SCALARIS_OPS);
        }
        final String WIKI_USE_BACKLINKS = config.getInitParameter("WIKI_USE_BACKLINKS");
        if (WIKI_USE_BACKLINKS != null) {
            Options.WIKI_USE_BACKLINKS = Boolean.parseBoolean(WIKI_USE_BACKLINKS);
        }
        final String WIKI_SAVEPAGE_RETRIES = config.getInitParameter("WIKI_SAVEPAGE_RETRIES");
        if (WIKI_SAVEPAGE_RETRIES != null) {
            Options.WIKI_SAVEPAGE_RETRIES = parseInt(WIKI_SAVEPAGE_RETRIES, Options.WIKI_SAVEPAGE_RETRIES);
        }
        final String WIKI_SAVEPAGE_RETRY_DELAY = config.getInitParameter("WIKI_SAVEPAGE_RETRY_DELAY");
        if (WIKI_SAVEPAGE_RETRY_DELAY != null) {
            Options.WIKI_SAVEPAGE_RETRY_DELAY = parseInt(WIKI_SAVEPAGE_RETRY_DELAY, Options.WIKI_SAVEPAGE_RETRY_DELAY);
        }
    }
    
    /**
     * Loads the siteinfo object.
     * 
     * @return <tt>true</tt> on success,
     *         <tt>false</tt> if not found or no connection available
     */
    abstract protected boolean loadSiteInfo();
    
    /**
     * Load all plugins from the plugin directory
     * <tt>&lt;ServletContextDir&gt;/WEB-INF/plugins</tt>.
     * 
     * @return <tt>true</tt> on success, <tt>false</tt> otherwise
     */
    @SuppressWarnings("unchecked")
    protected synchronized boolean loadPlugins() {
        final String pluginDir = getServletContext().getRealPath("/WEB-INF/plugins");
        try {
            PluginClassLoader pcl = new PluginClassLoader(pluginDir, new Class[] {WikiPlugin.class});
            List<Class<?>> plugins = pcl.getClasses(WikiPlugin.class);
            if (plugins != null) {
                for (Class<?> clazz: plugins) {
                    WikiPlugin plugin;
                    try {
                        plugin = ((Class<WikiPlugin>) clazz).newInstance();
                        plugin.init(this);
                    } catch (Exception e) {
                        System.err.println("failed to load plugin " + clazz.getCanonicalName());
                        e.printStackTrace();
                        continue;
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("failed to load plugins");
            e.printStackTrace();
            return false;
        }
        return true;
    }

    /**
     * Sets up the connection to the DB server.
     * 
     * @param request
     *            the request to the servlet
     */
    abstract protected Connection getConnection(HttpServletRequest request);

    /**
     * Releases the connection to the DB server, e.g. closes it.
     * 
     * @param request
     *            the request to the servlet
     * @param conn
     *            the connection to release
     */
    abstract protected void releaseConnection(HttpServletRequest request, Connection conn);

    @Override
    public void destroy() {
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.servlet.http.HttpServlet#doGet(HttpServletRequest request,
     *      HttpServletResponse response)
     */
    @Override
    protected void doGet(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        String image = request.getParameter("get_image");
        if (image != null) {
            showImage(request, response, image);
            return;
        }

        Connection connection = getConnection(request);
        if (connection == null) {
            showEmptyPage(request, response, new WikiPageBean()); // should forward to another page
            return; // return just in case
        }
        try {
            if (!initialized && !loadSiteInfo() || !currentImport.isEmpty()) {
                showImportPage(request, response, connection); // should forward to another page
                return; // return just in case
            }
            request.setCharacterEncoding("UTF-8");
            response.setCharacterEncoding("UTF-8");

            // show empty page for testing purposes if a parameter called "test" exists:
            if (request.getParameter("test") != null) {
                showEmptyPage(request, response, new WikiPageBean());
                return;
            }

            // get parameters:
            String req_title = request.getParameter("title");
            if (req_title == null) {
                req_title = MAIN_PAGE;
            }
            
            // if the "search" parameter exists, show the search
            String req_search = request.getParameter("search");
            if (req_search != null) {
                handleSearch(request, response, req_title, req_search, connection, new WikiPageListBean());
                return;
            }

            String req_action = request.getParameter("action");

            if (req_title.equals("Special:Random")) {
                handleViewRandomPage(request, response, req_title, connection, new WikiPageBean());
            } else if (req_title.startsWith("Special:AllPages") || req_title.startsWith("Special:Allpages")) {
                handleSpecialAllPages(request, response, req_title, connection, new WikiPageListBean());
            } else if (req_title.startsWith("Special:PrefixIndex")) {
                handleSpecialPrefix(request, response, req_title, connection, new WikiPageListBean());
            } else if (req_title.startsWith("Special:Search")) {
                handleSearch(request, response, req_title, req_search, connection, new WikiPageListBean());
            } else if (req_title.startsWith("Special:WhatLinksHere")) {
                handleSpecialWhatLinksHere(request, response, req_title, connection, new WikiPageListBean());
            } else if (req_title.equals("Special:SpecialPages")) {
                handleViewSpecialPages(request, response, connection, new WikiPageListBean());
            } else if (req_title.equals("Special:Statistics")) {
                handleViewSpecialStatistics(request, response, connection, new WikiPageListBean());
            } else if (req_title.equals("Special:Version")) {
                handleViewSpecialVersion(request, response, connection, new WikiPageListBean());
            } else if (req_action == null || req_action.equals("view")) {
                handleViewPage(request, response, req_title, connection, new WikiPageBean());
            } else if (req_action.equals("history")) {
                handleViewPageHistory(request, response, req_title, connection, new WikiPageBean());
            } else if (req_action.equals("edit")) {
                handleEditPage(request, response, req_title, connection, new WikiPageEditBean());
            } else {
                // default: show page
                handleViewPage(request, response, req_title, connection, new WikiPageBean());
            }

            // if the request has not been forwarded, print a general error
            response.setContentType("text/html");
            PrintWriter out = response.getWriter();
            out.write("An unknown error occured, please contact your administrator. A server restart may be required.");
            out.close();
        } finally {
            releaseConnection(request, connection);
        }
    }

    /**
     * Shows the page search for the given search string.
     * 
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:Search"
     * @param req_search
     *            search string
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     * 
     * @throws ServletException
     * @throws IOException
     */
    private void handleSearch(HttpServletRequest request,
            HttpServletResponse response, String title, String req_search,
            Connection connection, WikiPageListBean page)
            throws ServletException, IOException {
        // note: req_search can only be null if Special:Search is shown
        if (req_search == null) {
            int slashIndex = title.indexOf('/');
            if (slashIndex != (-1)) {
                req_search = title.substring(slashIndex + 1);
            } else {
                req_search = "";
            }
        }
        // use default namespace (id 0) for invalid values
        int nsId = parseInt(request.getParameter("namespace"), 0);
        page.setPageHeading("Search");
        page.setFormTitle("Search results");
        page.setFormType(FormType.PageSearchForm);
        ValueResult<List<String>> result;
        page.setSearch(req_search);
        page.setTitle("Special:Search&search=" + req_search + "&namespace=" + nsId);
        if (req_search.isEmpty()) {
            result = new ValueResult<List<String>>(new ArrayList<String>(0));
        } else {
            if (nsId == 0) {
                result = getArticleList(connection);
            } else {
                result = getPageList(connection);
            }
        }
        page.setNamespaceId(nsId);
        page.addStats(result.stats);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:Search"
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     *
     * @throws ServletException
     * @throws IOException
     */
    private void handleSpecialAllPages(HttpServletRequest request,
            HttpServletResponse response, String title,
            Connection connection, WikiPageListBean page) throws ServletException, IOException {
        String req_from = request.getParameter("from");
        if (req_from == null) {
            int slashIndex = title.indexOf('/');
            if (slashIndex != (-1)) {
                req_from = title.substring(slashIndex + 1);
            }
        }
        String req_to = request.getParameter("to");
        // use default namespace (id 0) for invalid values
        int nsId = parseInt(request.getParameter("namespace"), 0);
        page.setPageHeading("All pages");
        page.setFormTitle("All pages");
        page.setFormType(FormType.FromToForm);
        ValueResult<List<String>> result;
        if (req_from == null && req_to == null) {
            page.setTitle("Special:AllPages&namespace=" + nsId);
            page.setFromPage("");
            page.setToPage("");
            result = new ValueResult<List<String>>(new ArrayList<String>(0));
        } else {
            if (req_from == null) {
                req_from = ""; // start with first page
            }
            if (req_to == null) {
                req_to = ""; // stop at last page
            }
            page.setTitle("Special:AllPages&from=" + req_from + "&to=" + req_to + "&namespace=" + nsId);
            page.setFromPage(req_from);
            page.setToPage(req_to);
            if (nsId == 0) {
                result = getArticleList(connection);
            } else {
                result = getPageList(connection);
            }
        }
        page.addStats(result.stats);
        page.setNamespaceId(nsId);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:Search"
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     *
     * @throws ServletException
     * @throws IOException
     */
    private void handleSpecialPrefix(HttpServletRequest request,
            HttpServletResponse response, String title,
            Connection connection, WikiPageListBean page) throws ServletException, IOException {
        String req_prefix = request.getParameter("prefix");
        if (req_prefix == null) {
            int slashIndex = title.indexOf('/');
            if (slashIndex != (-1)) {
                req_prefix = title.substring(slashIndex + 1);
            }
        }
        // use default namespace (id 0) for invalid values
        int nsId = parseInt(request.getParameter("namespace"), 0);
        page.setPageHeading("All pages");
        page.setFormTitle("All pages");
        page.setFormType(FormType.PagePrefixForm);
        ValueResult<List<String>> result;
        if (req_prefix == null) {
            page.setTitle("Special:PrefixIndex&namespace=" + nsId);
            page.setPrefix("");
            result = new ValueResult<List<String>>(new ArrayList<String>(0));
        } else {
            page.setTitle("Special:PrefixIndex&prefix=" + req_prefix + "&namespace=" + nsId);
            page.setPrefix(req_prefix);
            if (nsId == 0) {
                result = getArticleList(connection);
            } else {
                result = getPageList(connection);
            }
        }
        page.addStats(result.stats);
        page.setNamespaceId(nsId);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:Search"
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     *
     * @throws ServletException
     * @throws IOException
     */
    private void handleSpecialWhatLinksHere(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageListBean page) throws ServletException, IOException {
        String req_target = request.getParameter("target");
        if (req_target == null) {
            // maybe we got the name separated with a '/' in the title:
            int slashIndex = title.indexOf('/');
            if (slashIndex != (-1)) {
                req_target = title.substring(slashIndex + 1);
            }
        }
        page.setFormTitle("What links here");
        page.setFormType(FormType.TargetPageForm);
        ValueResult<List<String>> result;
        if (req_target == null) {
            page.setPageHeading("Pages that link to a selected page");
            page.setTitle("Special:WhatLinksHere");
            page.setTarget("");
            result = new ValueResult<List<String>>(new ArrayList<String>(0));
        } else {
            page.setPageHeading("Pages that link to \"" + req_target + "\"");
            page.setTitle("Special:WhatLinksHere&target=" + req_target);
            page.setTarget(req_target);
            result = getPagesLinkingTo(connection, req_target, namespace);
        }
        page.addStats(result.stats);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /*
     * (non-Javadoc)
     * 
     * @see javax.servlet.http.HttpServlet#doPost(HttpServletRequest request,
     *      HttpServletResponse response)
     */
    @Override
    protected void doPost(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        Connection connection = getConnection(request);
        if (connection == null) {
            showEmptyPage(request, response, new WikiPageBean()); // should forward to another page
            return; // return just in case
        }
        try {
            if (!initialized && !loadSiteInfo() || !currentImport.isEmpty()) {
                showImportPage(request, response, connection); // should forward to another page
                return; // return just in case
            }
            request.setCharacterEncoding("UTF-8");
            response.setCharacterEncoding("UTF-8");

            handleEditPageSubmitted(request, response,
                    request.getParameter("title"), connection,
                    new WikiPageEditBean());

            // if the request has not been forwarded, print a general error
            response.setContentType("text/html");
            PrintWriter out = response.getWriter();
            out.write("An unknown error occured, please contact your administrator. A server restart may be required.");
            out.close();
        } finally {
            releaseConnection(request, connection);
        }
    }

    /**
     * Gets a random page and forwards the user to the site with this page.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the requested title (mostly "Special:Random")
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     * 
     * @throws IOException
     *             if the forward fails
     * @throws ServletException
     */
    private void handleViewRandomPage(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageBean page) throws IOException, ServletException {
        ValueResult<String> result = getRandomArticle(connection, new Random());
        page.addStats(result.stats);
        if (result.success) {
            ArrayList<Long> times = new ArrayList<Long>();
            for (List<Long> time : result.stats.values()) {
                times.addAll(time);
            }
            StringBuilder redirectUrl = new StringBuilder(256);
            redirectUrl.append("?title=");
            redirectUrl.append(URLEncoder.encode(MyWikiModel.denormalisePageTitle(result.value, namespace), "UTF-8"));
            redirectUrl.append("&random_times=" + StringUtils.join(times, "%2C"));
            response.sendRedirect(response.encodeRedirectURL(redirectUrl.toString()));
        } else if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, page);
        } else {
            response.sendRedirect(response.encodeRedirectURL("?title=Main Page&notice=error: can not view random page: <pre>" + result.message + "</pre>"));
        }
    }

    /**
     * Shows the contents of the page page with the given <tt>title</tt>.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the title of the page to show
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     * 
     * @throws IOException
     * @throws ServletException
     */
    private void handleViewPage(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageBean page) throws ServletException, IOException {
        // get renderer
        int render = getParam_renderer(request);
        // get revision id to load:
        int req_oldid = getParam_oldid(request);

        RevisionResult result = getRevision(connection, title, req_oldid, namespace);
        page.addStats(result.stats);
        
        if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, page);
            return;
        } else if (result.page_not_existing) {
            handleViewPageNotExisting(request, response, title, connection, page);
            return;
        } else if (result.rev_not_existing) {
            result = getRevision(connection, title, namespace);
            page.addStats(result.stats);
            addToParam_notice(request, "revision " + req_oldid + " not found - loaded current revision instead");
        }
        
        if (result.success) {
            renderRevision(result.page.getTitle(), result.revision, render, request, connection, page);
            
            if (!result.page.checkEditAllowed("")) {
                page.setEditRestricted(true);
            }

            // forward the request and the bean to the jsp:
            request.setAttribute("pageBean", page);
            RequestDispatcher dispatcher = request.getRequestDispatcher("page.jsp");
            dispatcher.forward(request, response);
        } else {
            setParam_error(request, "ERROR: revision unavailable");
            addToParam_notice(request, "error: unknown error getting page " + title + ":" + req_oldid + ": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response, page);
        }
    }
    
    /**
     * Extracts the full base URL from a requested URL.
     * 
     * @param requestUrl
     *            the URL from the request
     * 
     * @return the first part of the URL preceding '/' + {@link #wikiBaseURL}
     */
    private static String extractFullUrl(String requestUrl) {
        int colonIndex = requestUrl.indexOf('/' + wikiBaseURL);
        if (colonIndex != (-1)) {
            return requestUrl.substring(0, colonIndex + 1) + linkBaseURL;
        }
        return null;
        
    }
    
    /**
     * Creates a {@link WikiPageBean} object with the rendered content of a
     * given revision.
     * 
     * @param title
     *            the title of the article to render
     * @param revision
     *            the revision to render
     * @param renderer
     *            the renderer to use (0=plain text, 1=Bliki)
     * @param request
     *            the request object
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page (the rendered content will be added to
     *            this object)
     */
    private void renderRevision(String title, Revision revision,
            int renderer, HttpServletRequest request, Connection connection,
            WikiPageBean page) {
        // set the page's contents according to the renderer used
        // (categories are included in the content string, so they only
        // need special handling the wiki renderer is used)
        MyWikiModel wikiModel = getWikiModel(connection);
        String fullUrl = extractFullUrl(request.getRequestURL().toString());
        if (fullUrl != null) {
            wikiModel.setLinkBaseFullURL(fullUrl);
        }
        wikiModel.setPageName(title);
        if (renderer > 0) {
            String mainText = wikiModel.render(revision.unpackedText());
            if (wikiModel.isCategoryNamespace(wikiModel.getNamespace(title))) {
                ValueResult<List<String>> catPagesResult = getPagesInCategory(connection, title, namespace);
                page.addStats(catPagesResult.stats);
                if (catPagesResult.success) {
                    TreeSet<String> subCategories = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                    TreeSet<String> categoryPages = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                    final List<String> catPageList = new ArrayList<String>(catPagesResult.value.size());
                    wikiModel.denormalisePageTitles(catPagesResult.value, catPageList);
                    for (String pageInCat: catPageList) {
                        String pageNamespace = wikiModel.getNamespace(pageInCat);
                        if (wikiModel.isCategoryNamespace(pageNamespace)) {
                            subCategories.add(wikiModel.getTitleName(pageInCat));
                        } else if (wikiModel.isTemplateNamespace(pageNamespace)) {
                            // all pages using a template are in the category, too
                            ValueResult<List<String>> tplResult = getPagesInTemplate(connection, pageInCat, namespace);
                            page.addStats(tplResult.stats);
                            if (tplResult.success) {
                                final List<String> tplPageList = new ArrayList<String>(tplResult.value.size());
                                wikiModel.denormalisePageTitles(tplResult.value, tplPageList);
                                categoryPages.addAll(tplPageList);
                            } else {
                                if (tplResult.connect_failed) {
                                    setParam_error(request, "ERROR: DB connection failed");
                                } else {
                                    setParam_error(request, "ERROR: template page list unavailable");
                                }
                                addToParam_notice(request, "error getting pages using template: " + tplResult.message);
                            }
                        } else {
                            categoryPages.add(pageInCat);
                        }
                    }
                    page.setSubCategories(subCategories);
                    page.setCategoryPages(categoryPages);
                } else {
                    if (catPagesResult.connect_failed) {
                        setParam_error(request, "ERROR: DB connection failed");
                    } else {
                        setParam_error(request, "ERROR: category page list unavailable");
                    }
                    addToParam_notice(request, "error getting category pages: " + catPagesResult.message);
                }
            }
            String redirectedPageName = wikiModel.getRedirectLink();
            if (redirectedPageName != null) {
                page.setRedirectedTo(redirectedPageName);
                // add the content from the page directed to:
                wikiModel.tearDown();
                wikiModel.setUp();
                mainText = wikiModel.render(wikiModel.getRedirectContent(redirectedPageName));
            }
            page.setPage(mainText);
            page.setCategories(wikiModel.getCategories().keySet());
            page.addStats(wikiModel.getStats());
        } else if (renderer == 0) {
            // for debugging, show all parameters:
            StringBuilder sb = new StringBuilder();
            for (Enumeration<?> req_pars = request.getParameterNames(); req_pars.hasMoreElements();) {
                String element = (String) req_pars.nextElement();
                sb.append(element + " = ");
                sb.append(request.getParameter(element) + "\n");
            }
            sb.append("\n\n");
            for (Enumeration<?> headers = request.getHeaderNames(); headers.hasMoreElements();) {
                String element = (String) headers.nextElement();
                sb.append(element + " = ");
                sb.append(request.getHeader(element) + "\n");
            }
            page.setPage("<p>WikiText:<pre>"
                    + StringEscapeUtils.escapeHtml(revision.unpackedText()) + "</pre></p>" +
                    "<p>Version:<pre>"
                    + StringEscapeUtils.escapeHtml(String.valueOf(revision.getId())) + "</pre></p>" +
                    "<p>Last change:<pre>"
                    + StringEscapeUtils.escapeHtml(revision.getTimestamp()) + "</pre></p>" +
                    "<p>Request Parameters:<pre>"
                    + StringEscapeUtils.escapeHtml(sb.toString()) + "</pre></p>");
        }

        page.setNotice(getParam_notice(request));
        final String[] pageSaveTimes = getParam(request, "save_times").split(",");
        for (String pageSaveTime : pageSaveTimes) {
            final int pageSaveTimeInt = parseInt(pageSaveTime, -1);
            if (pageSaveTimeInt >= 0) {
                page.addStat("saving " + title, pageSaveTimeInt);
            }
        }
        final String[] pageRandomTimes = getParam(request, "random_times").split(",");
        for (String pageRandomTime : pageRandomTimes) {
            final int pageRandomTimeInt = parseInt(pageRandomTime, -1);
            if (pageRandomTimeInt >= 0) {
                page.addStat("random page", pageRandomTimeInt);
            }
        }
        page.setError(getParam_error(request));
        page.setTitle(title);
        page.setVersion(revision.getId());
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        // extract the date:
        page.setDate(Revision.stringToCalendar(revision.getTimestamp()));
    }
    
    /**
     * Shows the "Page not available" message the wiki returns in case a page
     * does not exist.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the original title of the page that does not exist
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewPageNotExisting(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageBean page) throws ServletException, IOException {
        // get renderer
        int render = getParam_renderer(request);
        String notExistingTitle = "MediaWiki:Noarticletext";

        RevisionResult result = getRevision(connection, notExistingTitle, namespace);
        page.addStats(result.stats);
        
        if (result.success) {
            renderRevision(title, result.revision, render, request, connection, page);
        } else if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, page);
            return;
        } else {
//            addToParam_notice(request, "error: unknown error getting page " + notExistingTitle + ": <pre>" + result.message + "</pre>");
            page.setPage("Page not available.");
            page.setError(getParam_error(request));
            page.setTitle(title);
        }
        // re-set version (we are only showing this page due to a non-existing page)
        page.setVersion(-1);
        page.setNotAvailable(true);
        page.setNotice(getParam_notice(request));
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", page);
        RequestDispatcher dispatcher = request.getRequestDispatcher("page.jsp");
        dispatcher.forward(request, response);
    }

    /**
     * Shows the page containing the history information of an article with the
     * given <tt>title</tt>.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the title of the page
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewPageHistory(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageBean page) throws ServletException, IOException {
        PageHistoryResult result = getPageHistory(connection, title, namespace);
        page.addStats(result.stats);
        if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, page);
            return;
        } else if (result.not_existing) {
            handleViewPageNotExisting(request, response, title, connection, page);
            return;
        }
        
        if (result.success) {
            page.setNotice(getParam_notice(request));
            page.setRevisions(result.revisions);
            if (!result.page.checkEditAllowed("")) {
                page.setEditRestricted(true);
            }

            page.setError(getParam_error(request));
            page.setTitle(title);
            if (!result.revisions.isEmpty()) { 
                page.setVersion(result.revisions.get(0).getId());
            }
            page.setWikiTitle(siteinfo.getSitename());
            page.setWikiNamespace(namespace);
            
            // forward the request and the bean to the jsp:
            request.setAttribute("pageBean", page);
            RequestDispatcher dispatcher = request
                    .getRequestDispatcher("pageHistory.jsp");
            dispatcher.forward(request, response);
        } else {
            setParam_error(request, "ERROR: revision list unavailable");
            addToParam_notice(request, "error: unknown error getting revision list for page " + title + ": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response, page);
        }
    }

    /**
     * Shows a page containing a list of article names.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param result
     *            result from reading the page list
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     * 
     * @throws IOException
     * @throws ServletException
     */
    private void handleViewSpecialPageList(HttpServletRequest request,
            HttpServletResponse response, ValueResult<List<String>> result,
            Connection connection, WikiPageListBean page)
            throws ServletException, IOException {
        if (result.success) {
            final TreeSet<String> pageList = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
            MyWikiModel.denormalisePageTitles(result.value, namespace, pageList);
            page.setNotice(getParam_notice(request));
            String nsPrefix = namespace.getNamespaceByNumber(page.getNamespaceId());
            if (!nsPrefix.isEmpty()) {
                nsPrefix += ":";
            }
            String prefix = nsPrefix + page.getPrefix();
            String from = page.getFromPage();
            String fullFrom = nsPrefix + page.getFromPage();
            String to = page.getToPage();
            String fullTo = nsPrefix + page.getToPage();
            String search = page.getSearch().toLowerCase();
            if (!prefix.isEmpty() || !from.isEmpty() || !to.isEmpty() || !search.isEmpty()) {
                // only show pages with this prefix:
                for (Iterator<String> it = pageList.iterator(); it.hasNext(); ) {
                    String cur = it.next();
                    // case-insensitive "startsWith" check:
                    if (!cur.regionMatches(true, 0, prefix, 0, prefix.length())) {
                        it.remove();
                    } else if (!from.isEmpty() && cur.compareToIgnoreCase(fullFrom) <= 0) {
                        it.remove();
                    } else if (!to.isEmpty() && cur.compareToIgnoreCase(fullTo) > 0) {
                        it.remove();
                    } else if (!search.isEmpty() && !cur.toLowerCase().contains(search)) {
                        it.remove();
                    }
                }
            }
            page.setPages(pageList);
            
            page.setWikiTitle(siteinfo.getSitename());
            page.setWikiNamespace(namespace);
            
            // forward the request and the bean to the jsp:
            request.setAttribute("pageBean", page);
            RequestDispatcher dispatcher = request
                    .getRequestDispatcher("pageSpecial_pagelist.jsp");
            dispatcher.forward(request, response);
        } else {
            if (result.connect_failed) {
                setParam_error(request, "ERROR: DB connection failed");
            } else {
                setParam_error(request, "ERROR: page list unavailable");
                addToParam_notice(request, "error: unknown error getting page list for " + page.getTitle() + ": <pre>" + result.message + "</pre>");
            }
            showEmptyPage(request, response, page);
            return;
        }
        page.setError(getParam_error(request));
        page.setTitle(page.getTitle());
    }

    /**
     * Shows the overview of all available special pages.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     *
     * @throws ServletException
     * @throws IOException
     */
    private void handleViewSpecialPages(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageListBean page) throws ServletException, IOException {
        final String title = "Special:SpecialPages";
        page.setPageHeading("Special pages");
        page.setTitle(title);
        
        Map<String /*group*/, Map<String /*title*/, String /*description*/>> specialPages = new LinkedHashMap<String, Map<String, String>>();
        Map<String, String> curSpecialPages;
        // Lists of pages
        curSpecialPages = new LinkedHashMap<String, String>();
        curSpecialPages.put("Special:AllPages", "All pages");
        curSpecialPages.put("Special:PrefixIndex", "All pages with prefix");
        specialPages.put("Lists of pages", curSpecialPages);
        // Wiki data and tools
        curSpecialPages = new LinkedHashMap<String, String>();
        curSpecialPages.put("Special:Statistics", "Statistics");
        curSpecialPages.put("Special:Version", "Version");
        specialPages.put("Wiki data and tools", curSpecialPages);
        // Redirecting special pages
        curSpecialPages = new LinkedHashMap<String, String>();
        curSpecialPages.put("Special:Search", "Search");
        curSpecialPages.put("Special:Random", "Show any page");
        specialPages.put("Redirecting special pages", curSpecialPages);
        // Page tools
        curSpecialPages = new LinkedHashMap<String, String>();
        if (Options.WIKI_USE_BACKLINKS) {
            curSpecialPages.put("Special:WhatLinksHere", "What links here");
        }
        specialPages.put("Page tools", curSpecialPages);

        StringBuilder content = new StringBuilder();
        for (Entry<String, Map<String, String>> specialPagesInGroup: specialPages.entrySet()) {
            String groupName = specialPagesInGroup.getKey();
            int i = 0;
            Iterator<Entry<String, String>> it = specialPagesInGroup.getValue().entrySet().iterator();
            
            content.append("<h4 class=\"mw-specialpagesgroup\"> <span class=\"mw-headline\">" + groupName + "</span></h4>\n");
            content.append("<table style=\"width: 100%;\" class=\"mw-specialpages-table\">\n");
            content.append(" <tbody>\n");
            content.append("  <tr>\n");
            content.append("   <td style=\"width: 30%; vertical-align: top;\">\n");
            content.append("    <ul>\n");
            int pagesInFirst = specialPagesInGroup.getValue().size() / 2 + specialPagesInGroup.getValue().size() % 2;
            for (; i < pagesInFirst; ++i) {
                Entry<String, String> pageInFirst = it.next();
                content.append("<li><a href=\"wiki?title=" + pageInFirst.getKey() + "\" title=\"" + pageInFirst.getKey() + "\">" + pageInFirst.getValue() + "</a></li>\n");
            }
            content.append("    </ul>\n");
            content.append("   </td>\n");
            content.append("   <td style=\"width: 10%;\"></td>\n");
            content.append("   <td style=\"width: 30%;\">\n");
            content.append("    <ul>\n");
            while(it.hasNext()) {
                Entry<String, String> pageInSecond = it.next();
                content.append("<li><a href=\"wiki?title=" + pageInSecond.getKey() + "\" title=\"" + pageInSecond.getKey() + "\">" + pageInSecond.getValue() + "</a></li>\n");
            }
            content.append("    </ul>\n");
            content.append("   </td>\n");
            content.append("   <td style=\"width: 30%;\"></td>\n");
            content.append("  </tr>\n");
            content.append(" </tbody>\n");
            content.append("</table>\n");
        }
        
        page.setPage(content.toString());
        // abuse #handleViewSpecialPageList here:
        ValueResult<List<String>> result = new ValueResult<List<String>>(new LinkedList<String>());
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * Shows several statistics about the running Wiki instance.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     *
     * @throws ServletException
     * @throws IOException
     */
    private void handleViewSpecialStatistics(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageListBean page) throws ServletException, IOException {
        MyWikiModel wikiModel = getWikiModel(connection);
        final String title = "Special:Statistics";
        page.setPageHeading("Statistics");
        page.setTitle(title);

        String articleCountStr;
        do {
            ValueResult<BigInteger> articleCount = getArticleCount(connection);
            page.addStats(articleCount.stats);
            if (articleCount.success) {
                articleCountStr = wikiModel.formatStatisticNumber(false, articleCount.value);
            } else {
                articleCountStr = "n/a";
            }
        } while(false);
        
        String pageCountStr;
        String pageEditsStr;
        String pageEditsPerPageStr;
        do {
            ValueResult<BigInteger> pageCount = getPageCount(connection);
            page.addStats(pageCount.stats);
            if (pageCount.success) {
                pageCountStr = wikiModel.formatStatisticNumber(false, pageCount.value);
                
                ValueResult<BigInteger> pageEdits = getStatsPageEdits(connection);
                page.addStats(pageEdits.stats);
                if (pageEdits.success) {
                    pageEditsStr = wikiModel.formatStatisticNumber(false, pageEdits.value);
                    if (pageCount.value.equals(BigInteger.valueOf(0))) {
                        pageEditsPerPageStr = wikiModel.formatStatisticNumber(false, 0.0);
                    } else {
                        pageEditsPerPageStr = wikiModel.formatStatisticNumber(false, pageEdits.value.doubleValue() / pageCount.value.doubleValue());
                    }
                } else {
                    pageEditsStr = "n/a";
                    pageEditsPerPageStr = "n/a";
                }
            } else {
                pageCountStr = "n/a";
                pageEditsStr = "n/a";
                pageEditsPerPageStr = "n/a";
            }
        } while (false);
        BigInteger uploadedFiles = BigInteger.valueOf(0); // currently not supported       
        
        Map<String /*group*/, Map<String /*name*/, String /*value*/>> specialPages = new LinkedHashMap<String, Map<String, String>>();
        Map<String, String> curStats;
        // Page statistics
        curStats = new LinkedHashMap<String, String>();
        curStats.put("Content pages", articleCountStr);
        curStats.put("Pages<br><small class=\"mw-statistic-desc\"> (All pages in the wiki, including talk pages, redirects, etc.)</small>",
                pageCountStr);
        curStats.put("Uploaded files", wikiModel.formatStatisticNumber(false, uploadedFiles));
        specialPages.put("Page statistics", curStats);
        // Edit statistics
        curStats = new LinkedHashMap<String, String>();
        curStats.put("Page edits since Wikipedia was set up", pageEditsStr);
        curStats.put("Average changes per page", pageEditsPerPageStr);
        specialPages.put("Edit statistics", curStats);
        // User statistics
        curStats = new LinkedHashMap<String, String>();
        specialPages.put("User statistics", curStats);

        StringBuilder content = new StringBuilder();
        content.append("<table class=\"wikitable mw-statistics-table\">\n");
        content.append(" <tbody>\n");
        for (Entry<String, Map<String, String>> specialPagesInGroup: specialPages.entrySet()) {
            String groupName = specialPagesInGroup.getKey();
            Iterator<Entry<String, String>> it = specialPagesInGroup.getValue().entrySet().iterator();
            
            content.append("  <tr>\n");
            content.append("   <th colspan=\"2\">" + groupName + "</th>\n");
            content.append("  </tr>\n");
            
            while(it.hasNext()) {
                Entry<String, String> stat = it.next();
                content.append("  <tr class=\"mw-statistics\">\n");
                content.append("   <td>" + stat.getKey() + "</td>\n");
                content.append("   <td class=\"mw-statistics-numbers\">" + stat.getValue() + "</td>\n");
                content.append("  </tr>\n");
            }
        }
        content.append(" </tbody>\n");
        content.append("</table>\n");
        
        page.setPage(content.toString());
        page.addStats(wikiModel.getStats());
        // abuse #handleViewSpecialPageList here:
        ValueResult<List<String>> result = new ValueResult<List<String>>(new LinkedList<String>());
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * Shows version information about the running Wiki instance.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page
     *
     * @throws ServletException
     * @throws IOException
     */
    private void handleViewSpecialVersion(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageListBean page) throws ServletException, IOException {
        final String title = "Special:Version";
        page.setPageHeading("Version");
        page.setTitle(title);
        String dbVersionStr;
        do {
            ValueResult<String> dbVersion = getDbVersion(connection);
            if (dbVersion.success) {
                dbVersionStr = dbVersion.value;
            } else {
                dbVersionStr = "n/a";
            }
        } while(false);

        StringBuilder content = new StringBuilder();
        content.append("<h2 id=\"mw-version-license\"> <span class=\"mw-headline\" id=\"License\">License</span></h2>\n");
        content.append("<div>\n");
        content.append("<p>This wiki is powered by <b><a href=\"http://code.google.com/p/scalaris/\" class=\"external text\" rel=\"nofollow\">Scalaris</a></b>, copyright © 2011 Zuse Institute Berlin</p>\n");
        content.append("<p>\n");
        content.append(" Licensed under the Apache License, Version 2.0 (the \"License\");</br>\n");
        content.append(" you may not use this software except in compliance with the License.</br>\n");
        content.append(" &nbsp;</br>\n");
        content.append(" You may obtain a copy of the License at</br>\n");
        content.append(" <a href=\"http://www.apache.org/licenses/LICENSE-2.0\" class=\"external text\" rel=\"nofollow\">http://www.apache.org/licenses/LICENSE-2.0</a>\n");
        content.append("</p>\n");
        content.append("<p>\n");
        content.append(" Unless required by applicable law or agreed to in writing, software</br>\n");
        content.append(" distributed under the License is distributed on an \"AS IS\" BASIS,</br>\n");
        content.append(" WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.</br>\n");
        content.append(" See the License for the specific language governing permissions and</br>\n");
        content.append(" limitations under the License.\n");
        content.append("</p>\n");
        content.append("</div>");
        
        content.append("<h2 id=\"mw-version-software\"> <span class=\"mw-headline\" id=\"Installed_software\">Installed software</span></h2>\n");
        content.append("<table class=\"wikitable\" id=\"sv-software\">\n");
        content.append(" <tbody>\n");
        content.append("  <tr>\n");
        content.append("   <th>Product</th>\n");
        content.append("   <th>Version</th>\n");
        content.append("  </tr>\n");
        content.append("  <tr>\n");
        content.append("   <td><a href=\"http://code.google.com/p/scalaris/\" class=\"external text\" rel=\"nofollow\">Scalaris Wiki Example</a></td>\n");
        content.append("   <td>" + version + "</td>\n");
        content.append("  </tr>\n");
        content.append("  <tr>\n");
        content.append("   <td><a href=\"http://code.google.com/p/scalaris/\" class=\"external text\" rel=\"nofollow\">Scalaris</a></td>\n");
        content.append("   <td>" + dbVersionStr + "</td>\n");
        content.append("  </tr>\n");
        content.append("  <tr>\n");
        content.append("   <td>Server</td>\n");
        content.append("   <td>" + getServletContext().getServerInfo() + "</td>\n");
        content.append("  </tr>\n");
        content.append(" </tbody>\n");
        content.append("</table>\n");
        
        page.setPage(content.toString());
        // abuse #handleViewSpecialPageList here:
        ValueResult<List<String>> result = new ValueResult<List<String>>(new LinkedList<String>());
        handleViewSpecialPageList(request, response, result, connection, page);
    }
    
    /**
     * Shows an empty page for testing purposes.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param page
     *            the bean for the page
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void showEmptyPage(HttpServletRequest request,
            HttpServletResponse response, WikiPageBeanBase page)
            throws ServletException, IOException {
        page.setNotice(getParam_notice(request));
        page.setError(getParam_error(request));
        request.setAttribute("pageBean", new WikiPageBean(page));
        RequestDispatcher dispatcher = request.getRequestDispatcher("page.jsp");
        dispatcher.forward(request, response);
    }
    
    /**
     * Shows a page for importing a DB dump (if implemented by the sub-class).
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    protected synchronized void showImportPage(HttpServletRequest request,
            HttpServletResponse response, Connection connection)
            throws ServletException, IOException {
    }

    /**
     * Shows the edit page form for an article with the given <tt>title</tt>.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the title of the article to show
     * @param page
     *            the bean for the page
     *            
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleEditPage(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageEditBean page) throws ServletException, IOException {
        // get revision id to load:
        int req_oldid = getParam_oldid(request);

        RevisionResult result = getRevision(connection, title, req_oldid, namespace);
        page.addStats(result.stats);
        if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, page);
            return;
        } else if (result.rev_not_existing) {
            result = getRevision(connection, title, namespace);
            page.addStats(result.stats);
            addToParam_notice(request, "revision " + req_oldid + " not found - loaded current revision instead");
        }

        if (result.page_not_existing) {
            page.setVersion(-1);
            page.setNewPage(true);
        } else if (result.rev_not_existing) {
            // DB corrupt
            setParam_error(request, "ERROR: revision unavailable");
            addToParam_notice(request, "error: unknown error getting current revision of page \"" + title + "\": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response, page);
            return;
        }
        if (result.success) {
            if (!result.page.checkEditAllowed("")) {
                page.setEditRestricted(true);
            }
            page.setPage(StringEscapeUtils.escapeHtml(result.revision.unpackedText()));
            page.setVersion(result.revision.getId());
        } else if (!page.isNewPage()) {
            page.setEditRestricted(true);
        }

        // set the textarea's contents:
        page.setNotice(getParam_notice(request));
        page.setError(getParam_error(request));
        page.setTitle(title);
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", page);
        RequestDispatcher dispatcher = request.getRequestDispatcher("pageEdit.jsp");
        dispatcher.forward(request, response);
    }
    
    /**
     * Shows a preview of the edit operation submitted or saves the page with
     * the given <tt>title</tt> depending on what button the user clicked.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the title of the article to show
     * @param page
     *            the bean for the page
     * 
     * @throws IOException 
     * @throws UnsupportedEncodingException 
     * @throws ServletException 
     */
    private void handleEditPageSubmitted(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageEditBean page) throws UnsupportedEncodingException,
            IOException, ServletException {
        String content = request.getParameter("wpTextbox1");
        String summary = request.getParameter("wpSummary");
        int oldVersion = parseInt(request.getParameter("oldVersion"), -1);
        boolean minorChange = Boolean.parseBoolean(request.getParameter("minor"));

        // save page or preview+edit page?
        if (request.getParameter("wpSave") != null) {
            // save page
            Contributor contributor = new Contributor();
            contributor.setIp(request.getRemoteAddr());
            String timestamp = Revision.calendarToString(Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            int newRevId = (oldVersion == -1) ? 1 : oldVersion + 1;
            Revision newRev = new Revision(newRevId, timestamp, minorChange, contributor, summary);
            newRev.setUnpackedText(content);

            SavePageResult result;
            int retries = 0;
            while (true) {
                result = savePage(connection, title, newRev, oldVersion, null, siteinfo, "", namespace);
                page.addStats(result.stats);
                if (!result.success && retries < Options.WIKI_SAVEPAGE_RETRIES) {
                    // check for conflicting edit on same page, do not retry in this case
                    final Page oldPage = result.oldPage;
                    if (oldPage != null && oldPage.getCurRev().getId() != oldVersion) {
                        break;
                    }
                    try {
                        Thread.sleep(Options.WIKI_SAVEPAGE_RETRY_DELAY);
                    } catch (InterruptedException e) {
                    }
                    ++retries;
                } else {
                    break;
                }
            }
            for (WikiEventHandler handler: eventHandlers) {
                handler.onPageSaved(result);
            }
            if (result.success) {
                // successfully saved -> show page with a notice of the successful operation
                ArrayList<Long> times = new ArrayList<Long>();
                for (List<Long> time : page.getStats().values()) {
                    times.addAll(time);
                }
                // do not include the UTF-8-title directly into encodeRedirectURL since that's not 
                // encoding umlauts (maybe other special chars as well) correctly, e.g. ä -> %E4 instead of %C3%A4
                StringBuilder redirectUrl = new StringBuilder(256);
                redirectUrl.append("?title=");
                redirectUrl.append(URLEncoder.encode(title, "UTF-8"));
                redirectUrl.append("&notice=successfully%20saved%20page");
                redirectUrl.append("&save_times=" + StringUtils.join(times, "%2C"));
                response.sendRedirect(response.encodeRedirectURL(redirectUrl.toString()));
                return;
            } else {
                // set error message and show the edit page again (see below)
                if (result.connect_failed) {
                    setParam_error(request, "ERROR: DB connection failed");
                } else {
                    setParam_error(request, "ERROR: conflicting edit");
                }
                addToParam_notice(request, "error: could not save page: <pre>" + result.message + "</pre>");
            }
        }

        // preview+edit page

        page.setNotice(getParam_notice(request));
        // set the textarea's contents:
        page.setPage(StringEscapeUtils.escapeHtml(content));

        MyWikiModel wikiModel = getWikiModel(connection);
        wikiModel.setPageName(title);
        String fullUrl = extractFullUrl(request.getRequestURL().toString());
        if (fullUrl != null) {
            wikiModel.setLinkBaseFullURL(fullUrl);
        }
        page.setPreview(wikiModel.render(content));
        page.addStats(wikiModel.getStats());
        page.setPage(content);
        page.setVersion(oldVersion);
        page.setError(getParam_error(request));
        page.setTitle(title);
        page.setSummary(request.getParameter("wpSummary"));
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", page);
        RequestDispatcher dispatcher = request.getRequestDispatcher("pageEdit.jsp");
        dispatcher.forward(request, response);
    }
    
    /**
     * Shows a preview of the edit operation submitted or saves the page with
     * the given <tt>title</tt> depending on what button the user clicked.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * @param title
     *            the title of the article to show
     * 
     * @throws IOException 
     * @throws UnsupportedEncodingException 
     * @throws ServletException 
     */
    private void showImage(HttpServletRequest request,
            HttpServletResponse response, String image)
            throws UnsupportedEncodingException, IOException, ServletException {
        // we need to fix the image title first, e.g. a prefix with the desired size may exist
        image = MATCH_WIKI_IMAGE_PX.matcher(image).replaceFirst("");
        String realImageUrl = getWikiImageUrl(image);
        if (realImageUrl != null) {
            response.sendRedirect(realImageUrl);
        } else {
            // bliki may have created ".svg.png" from an original ".svg" image:
            String new_image = MATCH_WIKI_IMAGE_SVG_PNG.matcher(image).replaceFirst(".svg");
            if (!image.equals(new_image)
                    && (realImageUrl = getWikiImageUrl(new_image)) != null) {
                response.sendRedirect(realImageUrl);
            } else {
                response.sendRedirect(response.encodeRedirectURL("images/image.png"));
            }
        }
    }

    /**
     * Retrieves the URL of an image from the Wikipedia related to the base URL
     * of this wiki.
     * 
     * @param image
     *            the name of the image as created by the bliki engine
     */
    protected String getWikiImageUrl(String image) {
        // add namespace - "Image" is a default alias for "File" in any language
        image = new String("Image:" + image);
        String fullBaseUrl = siteinfo.getBase();
        String baseUrl = "http://en.wikipedia.org";
        Matcher matcher = MATCH_WIKI_SITE_BASE.matcher(fullBaseUrl);
        if (matcher.matches()) {
            baseUrl = matcher.group(1);
        }
        User user = new User("", "", baseUrl + "/w/api.php");
        Connector connector = new Connector();
        user = connector.login(user);

        // set image width thumb size to 200px
        List<info.bliki.api.Page> pages = user.queryImageinfo(new String[] { image }, 200);
        if (pages.size() == 1) {
            info.bliki.api.Page imagePage = pages.get(0);
//            System.out.println("IMG-THUMB-URL: " + imagePage.getImageThumbUrl());
//            System.out.println("IMG-URL: " + imagePage.getImageUrl());

            if (imagePage.getImageThumbUrl() != null && !imagePage.getImageThumbUrl().isEmpty()) {
                return imagePage.getImageThumbUrl();
            }
        }
        return null;
    }
    
    abstract protected MyWikiModel getWikiModel(Connection connection);

    /**
     * Adds the given notice to the notice attribute.
     * 
     * @param request
     *            the http request
     * @param notice
     *            the notice to add
     */
    public static void addToParam_notice(HttpServletRequest request, String notice) {
        String req_notice = getParam_notice(request);
        String new_notice = req_notice.isEmpty() ? notice : req_notice + "<br />" + notice;
        request.setAttribute("notice", new_notice);
    }

    /**
     * Returns the notice parameter.
     * 
     * @param request
     *            the http request
     * 
     * @return the notice parameter or ""
     * 
     * @see #getParam(HttpServletRequest, String)
     */
    public static String getParam_notice(HttpServletRequest request) {
        return getParam(request, "notice");
    }

    /**
     * Returns the given parameter (or attribute if the parameter does not
     * exist, or an empty string if both are not present).
     * 
     * @param request
     *            the http request
     * @param name
     *            the name of the parameter
     * 
     * @return the notice parameter or ""
     */
    public static String getParam(HttpServletRequest request, String name) {
        String parValue = request.getParameter(name);
        if (parValue == null) {
            Object temp = request.getAttribute(name);
            if (temp instanceof String) {
                parValue = (String) temp; 
            }
        }
        if (parValue == null) {
            return new String("");
        } else {
            return parValue;
        }
    }

    /**
     * Returns the error parameter.
     * 
     * @param request
     *            the http request
     * 
     * @return the error parameter or ""
     * 
     * @see #getParam(HttpServletRequest, String)
     */
    public static String getParam_error(HttpServletRequest request) {
        return getParam(request, "error");
    }

    /**
     * Sets the error attribute. Once set, it cannot be changed with this
     * method.
     * 
     * @param request
     *            the http request
     * @param error
     *            the error to set
     */
    public static void setParam_error(HttpServletRequest request, String error) {
        String req_error = getParam(request, "error");
        if (req_error.isEmpty()) {
            request.setAttribute("error", error + " - ");
        }
    }

    /**
     * Get the revision id to load from the request object
     * 
     * @param request
     *            the http request
     * @return the revision id or -1 on failure to parse
     */
    private static int getParam_oldid(HttpServletRequest request) {
        String req_oldid = request.getParameter("oldid");
        return parseInt(req_oldid, -1);
    }

    /**
     * Determines which renderer should be used by evaluating the render
     * parameter of the request
     * 
     * @param request
     *            the http request
     * @return the renderer id
     */
    private static int getParam_renderer(HttpServletRequest request) {
        int render = 1;
        String req_render = request.getParameter("render");
        if (req_render == null) {
            // already set to 1, so the default renderer is used
        } else if (req_render.equals("0")) {
            render = 0;
        } else if (req_render.equals("-1")) {
            render = -1;
        }
        return render;
    }
    
    protected final static int parseInt(String value, int def) {
        if (value == null) {
            return def;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return def;
        }
    }
    
    protected final static Calendar parseDate(String value, Calendar def) {
        if (value == null) {
            return def;
        }
        try {
            return Revision.stringToCalendar(value);
        } catch (IllegalArgumentException e) {
            return def;
        }
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getNamespace()
     */
    @Override
    public final NamespaceUtils getNamespace() {
        return namespace;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getSiteinfo()
     */
    @Override
    public SiteInfo getSiteinfo() {
        return siteinfo;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getVersion()
     */
    @Override
    public String getVersion() {
        return version;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getWikibaseurl()
     */
    @Override
    public String getWikibaseurl() {
        return wikiBaseURL;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getLinkbaseurl()
     */
    @Override
    public String getLinkbaseurl() {
        return linkBaseURL;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getImagebaseurl()
     */
    @Override
    public String getImagebaseurl() {
        return imageBaseURL;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletInterface#getImagebaseurl()
     */
    @Override
    public synchronized void registerEventHandler(WikiEventHandler handler) {
        eventHandlers.add(handler);
    }
}
