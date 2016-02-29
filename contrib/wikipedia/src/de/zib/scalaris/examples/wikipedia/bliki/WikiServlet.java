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
package de.zib.scalaris.examples.wikipedia.bliki;

import info.bliki.api.Connector;
import info.bliki.api.User;
import info.bliki.wiki.model.Configuration;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.EnumMap;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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

import de.zib.scalaris.examples.wikipedia.InvolvedKey;
import de.zib.scalaris.examples.wikipedia.NamespaceUtils;
import de.zib.scalaris.examples.wikipedia.Options;
import de.zib.scalaris.examples.wikipedia.PageHistoryResult;
import de.zib.scalaris.examples.wikipedia.RevisionResult;
import de.zib.scalaris.examples.wikipedia.SavePageResult;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.WikiServletContext;
import de.zib.scalaris.examples.wikipedia.bliki.MyNamespace.NamespaceEnum;
import de.zib.scalaris.examples.wikipedia.bliki.MyWikiModel.SpecialPage;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageListBean.FormType;
import de.zib.scalaris.examples.wikipedia.data.Contributor;
import de.zib.scalaris.examples.wikipedia.data.Page;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;
import de.zib.scalaris.examples.wikipedia.data.xml.WikiDump;
import de.zib.scalaris.examples.wikipedia.plugin.PluginClassLoader;
import de.zib.scalaris.examples.wikipedia.plugin.WikiEventHandler;
import de.zib.scalaris.examples.wikipedia.plugin.WikiPlugin;
import de.zib.scalaris.examples.wikipedia.tomcat.URLParamEncoder;
import de.zib.tools.CircularByteArrayOutputStream;

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
    public static final String version = "0.9.0+git";

    protected SiteInfo siteinfo = null;
    protected MyNamespace namespace = null;
    
    protected boolean initialized = false;

    protected static final Pattern MATCH_WIKI_AUTOIMPORT_FILE = Pattern.compile(".*\\.db.auto$");
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
    
    protected ExistingPagesCache existingPages = ExistingPagesCache.createCache(100);

    protected static final EnumMap<SpecialPage, String> SPECIAL_SUFFIX_EN = MyWikiModel.SPECIAL_SUFFIX.get("en");
    protected EnumMap<SpecialPage, String> SPECIAL_SUFFIX_LANG;

    /**
     * Full list of normalised special page titles for {@link #existingPages}. 
     */
    protected final List<NormalisedTitle> specialPages;
    
    protected static LinkedList<Map<String, Object>>[] userReqLogs = null;
    protected static int curReqLog = 0;
    protected static int curReqLogStartTime = 0;

    /**
     * Creates the servlet. 
     */
    public WikiServlet() {
        super();
        SPECIAL_SUFFIX_LANG = SPECIAL_SUFFIX_EN;
        specialPages = new ArrayList<NormalisedTitle>(SPECIAL_SUFFIX_EN.size() * 2);
        // add English names here - the localised versions will be added when
        // the siteinfo is loaded
        for (String suffix : SPECIAL_SUFFIX_EN.values()) {
            // note: if non-english namespace, "Special" won't be recognised
            // during normalisation -> leave it unnormalised
            specialPages.add(new NormalisedTitle(
                    MyNamespace.MAIN_NAMESPACE_KEY,
                    MyWikiModel.createFullPageName(
                            MyWikiModel.SPECIAL_PREFIX.get("en"),
                            suffix)));
        }
    }

    /**
     * Servlet initialisation: imports options from the servlet info, and
     * initialises it.
     */
    @SuppressWarnings("unchecked")
    @Override
    public final void init(ServletConfig config) throws ServletException {
        super.init(config);
        readOptionsFromConfig(config);
        
        init2(config);
        
        loadSiteInfo();
        loadPlugins(config);
        startExistingPagesUpdate();
        existingPages.addAll(specialPages);
        if (Options.getInstance().LOG_USER_REQS > 0) {
            userReqLogs = new LinkedList[Options.getInstance().LOG_USER_REQS];
            for (int i = 0; i < userReqLogs.length; ++i) {
                userReqLogs[i] = new LinkedList<Map<String, Object>>();
            }
            // integer resolution should be enough
            curReqLogStartTime = (int) (System.currentTimeMillis() / 1000);
        }
        
        startAutoImport();
    }

    /**
     * Start automatically importing data right after initialisation in
     * {@link #init()} (if implemented in sub-class).
     */
    protected void startAutoImport() {
    }

    /**
     * Extracts servlet parameters from its config into the {@link Options}
     * class.
     * 
     * @param config
     *            servlet config
     */
    protected void readOptionsFromConfig(ServletConfig config) {
        final Options options = Options.getInstance();
        Options.parseOptions(options,
                config.getInitParameter("SERVERNAME"),
                config.getInitParameter("SERVERPATH"),
                config.getInitParameter("WIKI_USE_BACKLINKS"),
                config.getInitParameter("WIKI_SAVEPAGE_RETRIES"),
                config.getInitParameter("WIKI_SAVEPAGE_RETRY_DELAY"),
                config.getInitParameter("WIKI_PAGES_CACHE_IMPL"),
                config.getInitParameter("WIKI_REBUILD_PAGES_CACHE"),
                config.getInitParameter("WIKI_STORE_CONTRIBUTIONS"),
                config.getInitParameter("WIKI_OPTIMISATIONS"),
                config.getInitParameter("LOG_USER_REQS"),
                config.getInitParameter("SCALARIS_NODE_DISCOVERY"));
        System.out.println("Effective optimisations: " + options.OPTIMISATIONS.toString());
    }
    
    /**
     * Servlet initialisation, phase 2: this is executed directly after
     * importing the servlet config in {@link #init(ServletConfig)}. Overwrite
     * in sub-classes if needed, e.g. to setup the DB connection.
     */
    protected void init2(ServletConfig config) throws ServletException {
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
     * @param config
     *            servlet config
     * 
     * @return <tt>true</tt> on success, <tt>false</tt> otherwise
     */
    @SuppressWarnings("unchecked")
    protected synchronized boolean loadPlugins(ServletConfig config) {
        final String pluginDir = getServletContext().getRealPath("/WEB-INF/plugins");
        try {
            PluginClassLoader pcl = new PluginClassLoader(pluginDir, new Class[] {WikiPlugin.class});
            List<Class<?>> plugins = pcl.getClasses(WikiPlugin.class);
            if (plugins != null) {
                for (Class<?> clazz: plugins) {
                    WikiPlugin plugin;
                    try {
                        plugin = ((Class<WikiPlugin>) clazz).newInstance();
                        plugin.init(this, config);
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
     * Starts the service updating the bloom filter for existing pages.
     */
    protected void startExistingPagesUpdate() {
        final int rebuildDelay = Options.getInstance().WIKI_REBUILD_PAGES_CACHE;
        if (rebuildDelay > 0) {
            updateExistingPages();
            ScheduledExecutorService ses = Executors.newScheduledThreadPool(1);
            ses.scheduleWithFixedDelay(new Runnable() {
                @Override
                public void run() {
                    updateExistingPages();
                }
            }, rebuildDelay, rebuildDelay, TimeUnit.SECONDS);
        }
    }
    
    /**
     * Sets localised special page names by using the information provided by
     * the {@link #siteinfo} object.
     * 
     * Call this method inside {@link #loadSiteInfo()} in implementing classes.
     */
    protected void setLocalisedSpecialPageNames() {
        if (initialized) {
            String lang = siteinfo.extractLang();
            final EnumMap<SpecialPage, String> specialSuffixLang = MyWikiModel.SPECIAL_SUFFIX.get(lang);
            if (specialSuffixLang != null) {
                SPECIAL_SUFFIX_LANG = specialSuffixLang;
            }
        }
        // note: we always need to add these suffixes, even if lang == "en"
        // because English suffixes have only been added as un-normalised titles
        // before!
        for (String suffix : SPECIAL_SUFFIX_LANG.values()) {
            specialPages.add(new NormalisedTitle(MyNamespace.SPECIAL_NAMESPACE_KEY, suffix));
        }
        existingPages.addAll(specialPages);
    }

    /**
     * Sets up the connection to the DB server.
     * 
     * In case of errors, the <tt>error</tt> and <tt>notice</tt> attributes of
     * the <tt>request</tt> object are set appropriately if not <tt>null</tt>.
     * 
     * @param request
     *            the request to the servlet (may be <tt>null</tt>)
     * 
     * @return a valid connection of <tt>null</tt> if an error occurred
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
        long startTime = System.currentTimeMillis();
        String image = request.getParameter("get_image");
        if (image != null) {
            showImage(request, response, image);
            return;
        }

        final String serviceUser = getParam(request, "service_user");
        
        Connection connection = getConnection(request);
        if (connection == null) {
            response.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            showEmptyPage(request, response, connection, new WikiPageBean(serviceUser, startTime)); // should forward to another page
            return; // return just in case
        }
        
        for (WikiEventHandler handler: eventHandlers) {
            if (!handler.checkAccess(serviceUser, request, connection)) {
                // access not allowed
                showEmptyPage(request, response, connection, new WikiPageBean(serviceUser, startTime));
                return;
            }
        }
        
        try {
            if (!initialized && !loadSiteInfo() || !currentImport.isEmpty()) {
                showImportPage(request, response, connection, new WikiPageBean(serviceUser, startTime)); // should forward to another page
                return; // return just in case
            }
            request.setCharacterEncoding("UTF-8");
            response.setCharacterEncoding("UTF-8");

            // show empty page for testing purposes if a parameter called "test" exists:
            if (request.getParameter("test") != null) {
                showEmptyPage(request, response, connection, new WikiPageBean(
                        serviceUser, startTime));
                return;
            }
            
            // if the "search" parameter exists, show the search
            String req_search = request.getParameter("search");
            if (req_search != null) {
                handleSearch(request, response, null, req_search, connection,
                        new WikiPageListBean(serviceUser, startTime));
                return;
            }

            // get parameters:
            String req_title = request.getParameter("title");
            if (req_title == null) {
                req_title = MAIN_PAGE;
            }

            String req_action = request.getParameter("action");

            if (!MyWikiModel.isValidTitle(req_title)) {
                handleViewPageBadTitle(request, response, connection, new WikiPageBean(serviceUser, startTime));
            } else if (req_title.startsWith("Special:")) {
                handleViewSpecialPage(request, response, req_title, req_search, "Special:".length(), connection, startTime, serviceUser);
            } else if (req_title.startsWith(namespace.getSpecial() + ":")) {
                handleViewSpecialPage(request, response, req_title, req_search, namespace.getSpecial().length() + 1, connection, startTime, serviceUser);
            } else if (req_action == null || req_action.equals("view")) {
                handleViewPage(request, response, req_title, connection, new WikiPageBean(serviceUser, startTime));
            } else if (req_action.equals("history")) {
                handleViewPageHistory(request, response, req_title, connection, new WikiPageBean(serviceUser, startTime));
            } else if (req_action.equals("edit")) {
                handleEditPage(request, response, req_title, connection, new WikiPageEditBean(serviceUser, startTime));
            } else {
                // default: show page
                handleViewPage(request, response, req_title, connection, new WikiPageBean(serviceUser, startTime));
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
     * Shows a special page.
     * 
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:Search"
     * @param req_search
     *            search string
     * @param prefixLength
     *            length of the (localised) "Special:" prefix string
     * @param connection
     *            connection to the database
     * @param startTime
     *            the time when the request reached the servlet (in ms)
     * 
     * @throws ServletException
     * @throws IOException
     */
    protected void handleViewSpecialPage(HttpServletRequest request,
            HttpServletResponse response, String title, String req_search,
            int prefixLength, Connection connection, long startTime,
            final String serviceUser)
            throws IOException, ServletException {
        final String plainTitle = title.substring(prefixLength);
        // a "/" which separates the special page title from its parameters
        final String plainTitleToSlash = plainTitle.split("/")[0];
        if (plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_RANDOM))
                || plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_RANDOM))) {
            handleViewRandomPage(request, response, title, connection, new WikiPageBean(serviceUser, startTime));
        } else if (plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_ALLPAGES))
                || plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_ALLPAGES))) {
            handleSpecialAllPages(request, response, title, connection, new WikiPageListBean(serviceUser, startTime));
        } else if (plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_PREFIXINDEX))
                || plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_PREFIXINDEX))) {
            handleSpecialPrefix(request, response, title, connection, new WikiPageListBean(serviceUser, startTime));
        } else if (plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_SEARCH))
                || plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_SEARCH))) {
            handleSearch(request, response, title, req_search, connection, new WikiPageListBean(serviceUser, startTime));
        } else if (plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_WHATLINKSHERE))
                || plainTitleToSlash.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_WHATLINKSHERE))) {
            handleSpecialWhatLinksHere(request, response, title, connection, new WikiPageListBean(serviceUser, startTime));
        } else if (plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_SPECIALPAGES))
                || plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_SPECIALPAGES))) {
            handleViewSpecialPages(request, response, connection, new WikiPageListBean(serviceUser, startTime));
        } else if (plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_STATS))
                || plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_STATS))) {
            handleViewSpecialStatistics(request, response, connection, new WikiPageListBean(serviceUser, startTime));
        } else if (plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_EN.get(SpecialPage.SPECIAL_VERSION))
                || plainTitle.equalsIgnoreCase(SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_VERSION))) {
            handleViewSpecialVersion(request, response, connection, new WikiPageListBean(serviceUser, startTime));
        } else {
            // no such special page
            handleViewPageNotExisting(request, response, title, connection, new WikiPageBean(serviceUser, startTime));
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
        ValueResult<List<NormalisedTitle>> result;
        page.setSearch(req_search);
        page.setTitle(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_SEARCH)));
        page.setShowAllPages(false);
        if (req_search.isEmpty()) {
            result = new ValueResult<List<NormalisedTitle>>(new ArrayList<InvolvedKey>(0), new ArrayList<NormalisedTitle>(0));
        } else {
            if (existingPages.hasFullList()) {
                final long timeAtStart = System.currentTimeMillis();
                final String statName = "page list:" + nsId;
                final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
                final Set<NormalisedTitle> pages = existingPages.getList(NamespaceEnum.fromId(nsId));
                result = new ValueResult<List<NormalisedTitle>>(involvedKeys, new ArrayList<NormalisedTitle>(pages),
                        statName, System.currentTimeMillis() - timeAtStart);
            } else {
                result = getPageList(nsId, connection);
            }
        }
        page.setNamespaceId(nsId);
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:AllPages"
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
        page.setTitle(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_ALLPAGES)));
        ValueResult<List<NormalisedTitle>> result;
        if (req_from == null && req_to == null) {
            page.setShowAllPages(false);
            page.setFromPage("");
            page.setToPage("");
            result = new ValueResult<List<NormalisedTitle>>(new ArrayList<InvolvedKey>(0), new ArrayList<NormalisedTitle>(0));
        } else {
            page.setShowAllPages(true);
            if (req_from == null) {
                req_from = ""; // start with first page
            }
            if (req_to == null) {
                req_to = ""; // stop at last page
            }
            page.setFromPage(req_from);
            page.setToPage(req_to);
            if (existingPages.hasFullList()) {
                final long timeAtStart = System.currentTimeMillis();
                final String statName = "page list:" + nsId;
                final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
                final Set<NormalisedTitle> pages = existingPages.getList(NamespaceEnum.fromId(nsId));
                result = new ValueResult<List<NormalisedTitle>>(involvedKeys, new ArrayList<NormalisedTitle>(pages),
                        statName, System.currentTimeMillis() - timeAtStart);
            } else {
                result = getPageList(nsId, connection);
            }
        }
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        page.setNamespaceId(nsId);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:PrefixIndex"
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
        page.setTitle(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_PREFIXINDEX)));
        ValueResult<List<NormalisedTitle>> result;
        if (req_prefix == null) {
            page.setShowAllPages(false);
            page.setPrefix("");
            result = new ValueResult<List<NormalisedTitle>>(new ArrayList<InvolvedKey>(0), new ArrayList<NormalisedTitle>(0));
        } else {
            page.setShowAllPages(true);
            page.setPrefix(req_prefix);
            if (existingPages.hasFullList()) {
                final long timeAtStart = System.currentTimeMillis();
                final String statName = "page list:" + nsId;
                final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
                final Set<NormalisedTitle> pages = existingPages.getList(NamespaceEnum.fromId(nsId));
                result = new ValueResult<List<NormalisedTitle>>(involvedKeys, new ArrayList<NormalisedTitle>(pages),
                        statName, System.currentTimeMillis() - timeAtStart);
            } else {
                result = getPageList(nsId, connection);
            }
        }
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        page.setNamespaceId(nsId);
        handleViewSpecialPageList(request, response, result, connection, page);
    }

    /**
     * @param request
     *            the HTTP request
     * @param response
     *            the response object
     * @param title
     *            the requested title, e.g. "Special:WhatLinksHere"
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
        page.setTitle(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_WHATLINKSHERE)));
        ValueResult<List<NormalisedTitle>> result;
        if (req_target == null) {
            page.setShowAllPages(false);
            page.setPageHeading("Pages that link to a selected page");
            page.setTarget("");
            result = new ValueResult<List<NormalisedTitle>>(new ArrayList<InvolvedKey>(0), new ArrayList<NormalisedTitle>(0));
        } else {
            page.setShowAllPages(true);
            page.setPageHeading("Pages that link to \"" + req_target + "\"");
            page.setTarget(req_target);
            result = getPagesLinkingTo(connection, req_target, namespace);
        }
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
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
        long startTime = System.currentTimeMillis();
        Connection connection = getConnection(request);
        final String serviceUser = getParam(request, "service_user");
        if (connection == null) {
            showEmptyPage(request, response, connection, new WikiPageBean(
                    serviceUser, startTime)); // should forward to another page
            return; // return just in case
        }
        
        for (WikiEventHandler handler: eventHandlers) {
            if (!handler.checkAccess(serviceUser, request, connection)) {
                // access not allowed
                showEmptyPage(request, response, connection, new WikiPageBean(
                        serviceUser, startTime));
                return;
            }
        }
        
        try {
            if (!initialized && !loadSiteInfo() || !currentImport.isEmpty()) {
                showImportPage(request, response, connection, new WikiPageBean(
                        serviceUser, startTime)); // should forward to another page
                return; // return just in case
            }
            request.setCharacterEncoding("UTF-8");
            response.setCharacterEncoding("UTF-8");

            String req_title = request.getParameter("title");
            if (!MyWikiModel.isValidTitle(req_title)) {
                handleViewPageBadTitle(request, response, connection,
                        new WikiPageBean(serviceUser, startTime));
            } else {
                handleEditPageSubmitted(request, response,
                        request.getParameter("title"), connection,
                        new WikiPageEditBean(serviceUser, startTime));
            }

            // if the request has not been forwarded, print a general error
            response.setContentType("text/html");
            PrintWriter out = response.getWriter();
            out.write("An unknown error occured, please contact your administrator. "
                    + "A server restart may be required.");
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
        page.setTitle(title);
        final Random random = new Random();
        ValueResult<NormalisedTitle> result;
        if (existingPages.hasFullList()) {
            final long timeAtStart = System.currentTimeMillis();
            final String statName = "RANDOM_PAGE";
            final List<InvolvedKey> involvedKeys = new ArrayList<InvolvedKey>();
            final Set<NormalisedTitle> pages = existingPages.getList(NamespaceEnum.MAIN_NAMESPACE_KEY);
            final int randIdx = random.nextInt(pages.size());
            // no real other option to get a random element from a set :(
            int i = 0;
            NormalisedTitle randValue = null;
            for (Iterator<NormalisedTitle> iterator = pages.iterator(); iterator.hasNext();) {
                if (i == randIdx) {
                    randValue = iterator.next();
                    break;
                } else {
                    iterator.next();
                }
                ++i;
            }
            result = new ValueResult<NormalisedTitle>(involvedKeys, randValue,
                    statName, System.currentTimeMillis() - timeAtStart);
        } else {
            result = getRandomArticle(connection, random);
        }
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        final String serviceUser = page.getServiceUser().isEmpty() ? "" : "&service_user=" + page.getServiceUser();
        String redirectUrl;
        ArrayList<Long> times = new ArrayList<Long>();
        for (List<Long> time : result.stats.values()) {
            times.addAll(time);
        }
        if (result.success) {
            StringBuilder redirectUrl0 = new StringBuilder(256);
            redirectUrl0.append("?title=");
            redirectUrl0.append(URLEncoder.encode(result.value.denormalise(namespace), "UTF-8"));
            redirectUrl0.append("&random_times=" + StringUtils.join(times, "%2C"));
            redirectUrl0.append("&involved_keys=" + URLEncoder.encode(StringUtils.join(page.getInvolvedKeys(), " # "), "UTF-8"));
            redirectUrl = "http://" + Options.getInstance().SERVERNAME
                    + Options.getInstance().SERVERPATH
                    + response.encodeRedirectURL(redirectUrl0.toString())
                    + serviceUser;
        } else if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, connection, page);
            return;
        } else {
            StringBuilder redirectUrl0 = new StringBuilder(256);
            redirectUrl0.append("?title=");
            redirectUrl0.append(URLEncoder.encode(MAIN_PAGE, "UTF-8"));
            redirectUrl0.append("&random_times=" + StringUtils.join(times, "%2C"));
            redirectUrl0.append("&involved_keys=" + URLEncoder.encode(StringUtils.join(page.getInvolvedKeys(), " # "), "UTF-8"));
            redirectUrl0.append("&notice=" + URLParamEncoder.encode("error: can not view random page: <pre>" + result.message + "</pre>"));
            redirectUrl = "http://" + Options.getInstance().SERVERNAME
                    + Options.getInstance().SERVERPATH
                    + response.encodeRedirectURL(redirectUrl0.toString())
                    + serviceUser;
        }
        for (WikiEventHandler handler: eventHandlers) {
            handler.onViewRandomPage(page, result, connection);
        }
        response.sendRedirect(redirectUrl + "&server_time=" + (System.currentTimeMillis() - page.getStartTime()));
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
        // get revision id to load:
        int req_oldid = getParam_oldid(request);

        RevisionResult result = getRevision(connection, title, req_oldid, namespace);
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        handleViewPage2(request, response, title, connection, page, req_oldid,
                result);
    }

    private void handleViewPage2(HttpServletRequest request,
            HttpServletResponse response, String title, Connection connection,
            WikiPageBean page, int req_oldid, RevisionResult result)
            throws ServletException, IOException {
        if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, connection, page);
            return;
        } else if (result.page_not_existing) {
            handleViewPageNotExisting(request, response, title, connection, page);
            return;
        } else if (result.rev_not_existing) {
            if (result.page != null) {
                result.revision = result.page.getCurRev();
                result.success = true;
            }
            addToParam_notice(request, "revision " + req_oldid + " not found - loaded current revision instead");
        }
        
        if (result.success) {
            // get renderer
            int render = getParam_renderer(request);
            final boolean noRedirect = getParam(request, "redirect").equals("no");
            renderRevision(result.page.getTitle(), result, render, request,
                    connection, page, noRedirect,
                    getWikiModel(connection, page), true);
            
            if (!result.page.checkEditAllowed("")) {
                page.setEditRestricted(true);
            }

            forwardToPageJsp(request, response, connection, page, "page.jsp");
        } else {
            setParam_error(request, "ERROR: revision unavailable");
            addToParam_notice(request, "error: unknown error getting page " + title + ":" + req_oldid + ": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response, connection, page);
        }
    }
    
    /**
     * Creates a {@link WikiPageBean} object with the rendered content of a
     * given revision.
     * 
     * @param title
     *            the title of the article to render
     * @param result
     *            the revision to render (must be successful and contain a
     *            revision)
     * @param renderer
     *            the renderer to use (0=plain text, 1=Bliki)
     * @param request
     *            the request object
     * @param connection
     *            connection to the database
     * @param page
     *            the bean for the page (the rendered content will be added to
     *            this object)
     * @param noRedirect
     *            if <tt>true</tt>, a redirect will be shown as such, otherwise
     *            the content of the redirected page will be show
     * @param wikiModel
     *            the wiki model to use
     * @param topLevel
     *            if this function is called from inside
     *            {@link #renderRevision()}, this will be <tt>false</tt>,
     *            otherwise always use <tt>true</tt>
     */
    private void renderRevision(final String title,
            final RevisionResult result, final int renderer,
            final HttpServletRequest request, final Connection connection,
            final WikiPageBean page, final boolean noRedirect,
            final MyWikiModel wikiModel, final boolean topLevel) {
        // set the page's contents according to the renderer used
        // (categories are included in the content string, so they only
        // need special handling the wiki renderer is used)
        NormalisedTitle titleN = NormalisedTitle.fromUnnormalised(title, namespace);
        wikiModel.setNamespaceName(namespace.getNamespaceByNumber(titleN.namespace));
        wikiModel.setPageName(titleN.title);
        if (renderer > 0) {
            String mainText = wikiModel.renderPageWithCache(result.revision.unpackedText());
            if (titleN.namespace.equals(MyNamespace.CATEGORY_NAMESPACE_KEY)) {
                ValueResult<List<NormalisedTitle>> catPagesResult = getPagesInCategory(connection, titleN);
                page.addStats(catPagesResult.stats);
                page.getInvolvedKeys().addAll(catPagesResult.involvedKeys);
                if (catPagesResult.success) {
                    final TreeSet<String> subCategories = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                    final TreeSet<String> categoryPages = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);
                    final List<NormalisedTitle> tplPages = new ArrayList<NormalisedTitle>(catPagesResult.value.size());

                    for (NormalisedTitle pageInCat: catPagesResult.value) {
                        if (pageInCat.namespace.equals(MyNamespace.CATEGORY_NAMESPACE_KEY)) {
                            subCategories.add(pageInCat.title);
                        } else if (pageInCat.namespace.equals(MyNamespace.TEMPLATE_NAMESPACE_KEY)) {
                            tplPages.add(pageInCat);
                            categoryPages.add(pageInCat.denormalise(namespace));
                        } else {
                            categoryPages.add(pageInCat.denormalise(namespace));
                        }
                    }
                    if (!tplPages.isEmpty()) {
                        // all pages using a template are in the category of the template, too
                        ValueResult<List<NormalisedTitle>> tplResult = getPagesInTemplates(connection, tplPages, title);
                        page.addStats(tplResult.stats);
                        page.getInvolvedKeys().addAll(tplResult.involvedKeys);
                        if (tplResult.success) {
                            for (NormalisedTitle pageInTplOfCat: tplResult.value) {
                                if (pageInTplOfCat.namespace.equals(MyNamespace.CATEGORY_NAMESPACE_KEY)) {
                                    subCategories.add(pageInTplOfCat.title);
                                } else if (pageInTplOfCat.namespace.equals(MyNamespace.TEMPLATE_NAMESPACE_KEY)) {
                                    // TODO: go into recursion?! -> for now, just add the template
//                                  tplPages.add(pageInTplOfCat);
                                    categoryPages.add(pageInTplOfCat.denormalise(namespace));
                                } else {
                                    categoryPages.add(pageInTplOfCat.denormalise(namespace));
                                }
                            }
                        } else {
                            if (tplResult.connect_failed) {
                                setParam_error(request, "ERROR: DB connection failed");
                            } else {
                                setParam_error(request, "ERROR: template page lists unavailable");
                            }
                            addToParam_notice(request, "error getting pages using templates: " + tplResult.message);
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
            page.setTitle(title);
            page.setVersion(result.revision.getId());
            String redirectedPageName = wikiModel.getRedirectLink();
            if (redirectedPageName != null) {
                if (noRedirect) {
                    if (topLevel) {
                        page.setContentSub("Redirect page");
                    }
                    mainText = wikiModel.renderRedirectPage(redirectedPageName);
                    page.setDate(Revision.stringToCalendar(result.revision.getTimestamp()));
                } else {
                    final String safeTitle = StringEscapeUtils.escapeHtml(title);
                    final String redirectUrl = wikiModel.getWikiBaseURL().replace("${title}", title);
                    page.setContentSub("(Redirected from <a href=\"" + redirectUrl + "&redirect=no\" title=\"" + safeTitle + "\">" + title + "</a>)");
                    // add the content from the page directed to:
                    wikiModel.tearDown();
                    wikiModel.setUp();
                    
                    RevisionResult redirectResult = getRevision(connection,
                            redirectedPageName, namespace);
                    page.addStats(redirectResult.stats);
                    page.getInvolvedKeys().addAll(redirectResult.involvedKeys);
                    if (redirectResult.success) {
                        renderRevision(redirectedPageName, redirectResult,
                                renderer, request, connection, page, true,
                                wikiModel, false);
                        return;
                    } else {
                        // non-existing/non-successful page is like redirect=no
                        mainText = wikiModel.renderRedirectPage(redirectedPageName);
                        page.setDate(Revision.stringToCalendar(result.revision.getTimestamp()));
                    }
                }
            } else {
                setSubPageNav(title, page, wikiModel);
            }
            page.setPage(mainText);
            page.setCategories(wikiModel.getCategories().keySet());
            page.addStats(wikiModel.getStats());
            page.getInvolvedKeys().addAll(wikiModel.getInvolvedKeys());
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
                    + StringEscapeUtils.escapeHtml(result.revision.unpackedText()) + "</pre></p>" +
                    "<p>Version:<pre>"
                    + StringEscapeUtils.escapeHtml(String.valueOf(result.revision.getId())) + "</pre></p>" +
                    "<p>Last change:<pre>"
                    + StringEscapeUtils.escapeHtml(result.revision.getTimestamp()) + "</pre></p>" +
                    "<p>Request Parameters:<pre>"
                    + StringEscapeUtils.escapeHtml(sb.toString()) + "</pre></p>");
            page.setTitle(title);
            page.setVersion(result.revision.getId());
            page.setDate(Revision.stringToCalendar(result.revision.getTimestamp()));
        }
        page.setNotice(getParam_notice(request));
        page.setError(getParam_error(request));
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);
    }

    /**
     * For sub-pages set a navigation to higher-level pages via
     * {@link WikiPageBean#setContentSub(String)} into the page bean.
     * 
     * @param title
     *            the title of the article to render
     * @param page
     *            the bean for the page
     * @param wikiModel
     *            the wiki model to get the base URL from
     */
    protected void setSubPageNav(String title, WikiPageBean page,
            MyWikiModel wikiModel) {
        final String wikiBaseURL = wikiModel.getWikiBaseURL();
        setSubPageNav(title, page, wikiBaseURL);
    }

    /**
     * For sub-pages set a navigation to higher-level pages via
     * {@link WikiPageBean#setContentSub(String)} into the page bean.
     * 
     * @param title
     *            the title of the article to render
     * @param page
     *            the bean for the page
     * @param wikiBaseURL
     *            base url for links
     */
    protected void setSubPageNav(String title, WikiPageBean page,
            final String wikiBaseURL) {
        String[] parts = title.split("/");
        if (parts.length > 1) {
            String fullPart = null;
            StringBuilder contentSub = new StringBuilder();
            for (int i = 0; i < parts.length - 1; ++i) {
                String part = parts[i];
                if (i == 0) {
                    // first?
                    fullPart = part;
                } else {
                    contentSub.append(" · ");
                    fullPart += "/" + part;
                }
                contentSub.append("<a href=\"");
                contentSub.append(wikiBaseURL.replace("${title}", fullPart));
                contentSub.append("\" title=\"");
                contentSub.append(fullPart);
                contentSub.append("\">");
                contentSub.append(fullPart);
                contentSub.append("</a>‎");
            }
            if (contentSub.length() > 0) {
                page.setContentSub("<span class=\"subpages\">&lt; " + contentSub.toString() + "</span>");
            }
        }
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
        page.getInvolvedKeys().addAll(result.involvedKeys);
        
        if (result.success) {
            renderRevision(title, result, render, request, connection, page,
                    false, getWikiModel(connection, page), true);
        } else if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, connection, page);
            return;
        } else {
//            addToParam_notice(request, "error: unknown error getting page " + notExistingTitle + ": <pre>" + result.message + "</pre>");
            page.setPage("Page not available.");
            page.setError(getParam_error(request));
            page.setTitle(title);
        }
        setSubPageNav(title, page, getLinkbaseurl(page));
        // re-set version (we are only showing this page due to a non-existing page)
        page.setVersion(-1);
        page.setNotAvailable(true);
        page.setNotice(getParam_notice(request));
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        forwardToPageJsp(request, response, connection, page, "page.jsp");
    }

    /**
     * Forwards a request to the <tt>page.jsp</tt> with the given page bean.
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
    protected void forwardToPageJsp(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageBeanBase page, String jsp) throws ServletException, IOException {
        final String[] pageSaveTimes = getParam(request, "save_times").split(",");
        for (String pageSaveTime : pageSaveTimes) {
            final int pageSaveTimeInt = parseInt(pageSaveTime, -1);
            if (pageSaveTimeInt >= 0) {
                final String statName = "SAVE:" + page.getTitle();
                page.addStat(statName, pageSaveTimeInt);
            }
        }
        final int pageSaveServerTime = parseInt(getParam(request, "server_time"), -1);
        if (pageSaveServerTime >= 0) {
            page.addStat("server_time (last op)", pageSaveServerTime);
        }
        page.setSaveAttempts(parseInt(getParam(request, "save_attempts"), 0));
        for (int i = 1; i <= Options.getInstance().WIKI_SAVEPAGE_RETRIES; ++i) {
            final String failedKeysPar = getParam(request, "failed_keys" + i);
            if (!failedKeysPar.isEmpty()) {
                final List<String> pageSaveFailedKeys = Arrays.asList(failedKeysPar.split(" # "));
                page.getFailedKeys().put(i, pageSaveFailedKeys);
            }
        }
        final String involvedKeysPar = getParam(request, "involved_keys");
        if (!involvedKeysPar.isEmpty()) {
            page.getInvolvedKeys().add(new InvolvedKey());
            InvolvedKey.addInvolvedKeys(page.getInvolvedKeys(), Arrays.asList(involvedKeysPar.split(" # ")), true);
        }
        final String[] pageRandomTimes = getParam(request, "random_times").split(",");
        for (String pageRandomTime : pageRandomTimes) {
            final int pageRandomTimeInt = parseInt(pageRandomTime, -1);
            if (pageRandomTimeInt >= 0) {
                final String statName = "RANDOM_PAGE (last op)";
                page.addStat(statName, pageRandomTimeInt);
            }
        }
        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", page);
        request.setAttribute("servlet", this);
        RequestDispatcher dispatcher = request.getRequestDispatcher(jsp);
        dispatcher.forward(request, response);
    }
    
    /**
     * Shows the "Bad Title" message the wiki returns in case a page title is
     * invalid.
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
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewPageBadTitle(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageBean page) throws ServletException, IOException {
        final String title = "Bad title";
        // get renderer
        final int render = getParam_renderer(request);
        final String badTitleKey = "MediaWiki:Badtitletext";

        RevisionResult result = getRevision(connection, badTitleKey, namespace);
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        
        if (result.success) {
            renderRevision(title, result, render, request, connection, page,
                    false, getWikiModel(connection, page), true);
        } else if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, connection, page);
            return;
        } else {
//            addToParam_notice(request, "error: unknown error getting page " + badTitleTitle + ": <pre>" + result.message + "</pre>");
            page.setPage("The requested page title is invalid. It may be empty, contain unsupported characters, or include a non-local or incorrectly linked interwiki prefix. You may be able to locate the desired page by searching for its name (with interwiki prefix, if any) in the search box.");
            page.setError(getParam_error(request));
            page.setTitle(title);
        }
        // re-set version (we are only showing this page due to a non-existing page)
        page.setVersion(-1);
        page.setEditRestricted(true);
        page.setNotAvailable(true);
        page.setNotice(getParam_notice(request));
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        forwardToPageJsp(request, response, connection, page, "page.jsp");
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
        page.getInvolvedKeys().addAll(result.involvedKeys);
        if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, connection, page);
            return;
        } else if (result.not_existing) {
            handleViewPageNotExisting(request, response, title, connection, page);
            return;
        }

        page.setTitle(title);
        if (result.success) {
            page.setNotice(getParam_notice(request));
            page.setRevisions(result.revisions);
            if (!result.page.checkEditAllowed("")) {
                page.setEditRestricted(true);
            }

            page.setError(getParam_error(request));
            if (!result.revisions.isEmpty()) { 
                page.setVersion(result.revisions.get(0).getId());
            }
            page.setWikiTitle(siteinfo.getSitename());
            page.setWikiNamespace(namespace);

            forwardToPageJsp(request, response, connection, page, "pageHistory.jsp");
        } else {
            setParam_error(request, "ERROR: revision list unavailable");
            addToParam_notice(request, "error: unknown error getting revision list for page " + title + ": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response, connection, page);
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
            HttpServletResponse response, ValueResult<List<NormalisedTitle>> result,
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
            final String prefix = nsPrefix + page.getPrefix();
            final String from = page.getFromPage();
            final String fullFrom = nsPrefix + page.getFromPage();
            final String to = page.getToPage();
            final String fullTo = nsPrefix + page.getToPage();
            final String search = page.getSearch().toLowerCase();
            final String searchTitle = MyWikiModel.normaliseName(page.getSearch());
            boolean foundMatch = false;
            if (!prefix.isEmpty() || !from.isEmpty() || !to.isEmpty() || !search.isEmpty()) {
                // only show pages with this prefix:
                for (Iterator<String> it = pageList.iterator(); it.hasNext(); ) {
                    final String cur = it.next();
                    // case-insensitive "startsWith" check:
                    if (!cur.regionMatches(true, 0, prefix, 0, prefix.length())) {
                        it.remove();
                    } else if (!from.isEmpty() && cur.compareToIgnoreCase(fullFrom) <= 0) {
                        it.remove();
                    } else if (!to.isEmpty() && cur.compareToIgnoreCase(fullTo) > 0) {
                        it.remove();
                    } else if (!search.isEmpty() && !cur.toLowerCase().contains(search)) {
                        it.remove();
                    } else if (!search.isEmpty() && cur.equals(searchTitle)) {
                        foundMatch = true;
                    }
                }
            }
            page.setPages(pageList);
            page.setFoundFullMatch(foundMatch);
            page.setWikiTitle(siteinfo.getSitename());
            page.setWikiNamespace(namespace);
            
            forwardToPageJsp(request, response, connection, page, "pageSpecial_pagelist.jsp");
        } else {
            if (result.connect_failed) {
                setParam_error(request, "ERROR: DB connection failed");
            } else {
                setParam_error(request, "ERROR: page list unavailable");
                addToParam_notice(request, "error: unknown error getting page list for " + page.getTitle() + ": <pre>" + result.message + "</pre>");
            }
            showEmptyPage(request, response, connection, page);
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
        final String title = MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_SPECIALPAGES));
        page.setPageHeading("Special pages");
        page.setTitle(title);
        
        Map<String /*group*/, Map<String /*title*/, String /*description*/>> specialPages = new LinkedHashMap<String, Map<String, String>>();
        Map<String, String> curSpecialPages;
        // Lists of pages
        curSpecialPages = new LinkedHashMap<String, String>();
        curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_ALLPAGES)), "All pages");
        curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_PREFIXINDEX)), "All pages with prefix");
        specialPages.put("Lists of pages", curSpecialPages);
        // Wiki data and tools
        curSpecialPages = new LinkedHashMap<String, String>();
        curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_STATS)), "Statistics");
        curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_VERSION)), "Version");
        specialPages.put("Wiki data and tools", curSpecialPages);
        // Redirecting special pages
        curSpecialPages = new LinkedHashMap<String, String>();
        curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_SEARCH)), "Search");
        curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_RANDOM)), "Show any page");
        specialPages.put("Redirecting special pages", curSpecialPages);
        // Page tools
        curSpecialPages = new LinkedHashMap<String, String>();
        if (Options.getInstance().WIKI_USE_BACKLINKS) {
            curSpecialPages.put(MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_WHATLINKSHERE)), "What links here");
        }
        specialPages.put("Page tools", curSpecialPages);

        StringBuilder content = new StringBuilder();
        final String serviceUser = page.getServiceUser().isEmpty() ? "" : "&service_user=" + page.getServiceUser();
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
                content.append("<li><a href=\"wiki?title=" + pageInFirst.getKey() + serviceUser + "\" title=\"" + pageInFirst.getKey() + "\">" + pageInFirst.getValue() + "</a></li>\n");
            }
            content.append("    </ul>\n");
            content.append("   </td>\n");
            content.append("   <td style=\"width: 10%;\"></td>\n");
            content.append("   <td style=\"width: 30%;\">\n");
            content.append("    <ul>\n");
            while(it.hasNext()) {
                Entry<String, String> pageInSecond = it.next();
                content.append("<li><a href=\"wiki?title=" + pageInSecond.getKey() + serviceUser + "\" title=\"" + pageInSecond.getKey() + "\">" + pageInSecond.getValue() + "</a></li>\n");
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
        ValueResult<List<NormalisedTitle>> result = new ValueResult<List<NormalisedTitle>>(
                new ArrayList<InvolvedKey>(0),
                new ArrayList<NormalisedTitle>(0));
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
        MyWikiModel wikiModel = getWikiModel(connection, page);
        final String title = MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_STATS));
        page.setPageHeading("Statistics");
        page.setTitle(title);

        String articleCountStr;
        do {
            ValueResult<BigInteger> articleCount = getArticleCount(connection);
            page.addStats(articleCount.stats);
            page.getInvolvedKeys().addAll(articleCount.involvedKeys);
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
            page.getInvolvedKeys().addAll(pageCount.involvedKeys);
            if (pageCount.success) {
                pageCountStr = wikiModel.formatStatisticNumber(false, pageCount.value);
                
                ValueResult<BigInteger> pageEdits = getStatsPageEdits(connection);
                page.addStats(pageEdits.stats);
                page.getInvolvedKeys().addAll(pageEdits.involvedKeys);
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
        page.getInvolvedKeys().addAll(wikiModel.getInvolvedKeys());
        // abuse #handleViewSpecialPageList here:
        ValueResult<List<NormalisedTitle>> result = new ValueResult<List<NormalisedTitle>>(
                new ArrayList<InvolvedKey>(0),
                new ArrayList<NormalisedTitle>(0));
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
        final String title = MyWikiModel.createFullPageName(namespace.getSpecial(), SPECIAL_SUFFIX_LANG.get(SpecialPage.SPECIAL_VERSION));
        page.setPageHeading("Version");
        page.setTitle(title);

        StringBuilder content = new StringBuilder();
        content.append("<h2 id=\"mw-version-license\"> <span class=\"mw-headline\" id=\"License\">License</span></h2>\n");
        content.append("<div>\n");
        content.append("<p>This wiki is powered by <b><a href=\"http://code.google.com/p/scalaris/\" class=\"external text\" rel=\"nofollow\">Scalaris</a></b>, copyright © 2013 Zuse Institute Berlin</p>\n");
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
        content.append("   <td><a href=\"https://code.google.com/p/scalaris/\" class=\"external text\" rel=\"nofollow\">Scalaris Wiki Example</a></td>\n");
        content.append("   <td>" + version + "</td>\n");
        content.append("  </tr>\n");
        content.append("  <tr>\n");
        content.append("   <td><a href=\"https://code.google.com/p/scalaris/\" class=\"external text\" rel=\"nofollow\">Scalaris</a></td>\n");
        content.append("   <td>" + getDbVersionStr(connection) + "</td>\n");
        content.append("  </tr>\n");
        content.append("  <tr>\n");
        content.append("   <td>Server</td>\n");
        content.append("   <td>" + getServerVersion() + "</td>\n");
        content.append("  </tr>\n");
        content.append("  <tr>\n");
        content.append("   <td><a href=\"https://code.google.com/p/gwtwiki/\" class=\"external text\" rel=\"nofollow\">bliki renderer</a></td>\n");
        content.append("   <td>" + getBlikiVersion() + "</td>\n");
        content.append("  </tr>\n");
        content.append(" </tbody>\n");
        content.append("</table>\n");
        
        content.append("<h2 id=\"mw-version-ext\"> <span class=\"mw-headline\" id=\"Installed_extensions\">Installed extensions</span></h2>\n");
        content.append("<table class=\"wikitable\" id=\"sv-ext\">\n");
        content.append(" <tbody>\n");
        content.append("  <tr>\n");
        content.append("   <th colspan=\"4\" id=\"sv-credits-specialpage\">Event handlers</th>\n");
        content.append("  </tr>\n");
        if (eventHandlers.isEmpty()) {
            content.append("  <tr>\n");
            content.append("   <td>-</td>\n");
            content.append("   <td>-</td>\n");
            content.append("   <td>-</td>\n");
            content.append("   <td>-</td>\n");
            content.append("  </tr>\n");
        }
        for (WikiEventHandler eventHandler : eventHandlers) {
            content.append("  <tr>\n");
            content.append("   <td><em><a class=\"external text\" href=\"" + eventHandler.getURL() + "\">" + eventHandler.getName() + "</a> (Version " + eventHandler.getVersion() + ")</em></td>\n");
            content.append("   <td><code title=\" " + eventHandler.getClass().getCanonicalName() + "\">" + eventHandler.getClass().getSimpleName() + "</code></td>\n");
            content.append("   <td>" + eventHandler.getDescription() + "</td>\n");
            content.append("   <td>" + eventHandler.getAuthor() + "</td>\n");
            content.append("  </tr>\n");
        }
        content.append(" </tbody>\n");
        content.append("</table>\n");
        
        page.setPage(content.toString());
        // abuse #handleViewSpecialPageList here:
        ValueResult<List<NormalisedTitle>> result = new ValueResult<List<NormalisedTitle>>(
                new ArrayList<InvolvedKey>(0),
                new ArrayList<NormalisedTitle>(0));
        handleViewSpecialPageList(request, response, result, connection, page);
    }
    
    /**
     * Shows an empty page for testing purposes.
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
     * @throws IOException 
     * @throws ServletException 
     */
    private void showEmptyPage(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageBeanBase page) throws ServletException, IOException {
        page.setNotice(getParam_notice(request));
        page.setError(getParam_error(request));
        forwardToPageJsp(request, response, connection, new WikiPageBean(page), "page.jsp");
    }
    
    /**
     * Shows a page for importing a DB dump (if implemented by the sub-class).
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
    protected synchronized void showImportPage(HttpServletRequest request,
            HttpServletResponse response, Connection connection,
            WikiPageBean page) throws ServletException, IOException {
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
        
        if (getParam_redlink(request) && !result.page_not_existing) {
            handleViewPage2(request, response, title, connection,
                    new WikiPageBean(page), req_oldid, result);
            return;
        }
        
        page.addStats(result.stats);
        page.getInvolvedKeys().addAll(result.involvedKeys);
        if (result.connect_failed) {
            setParam_error(request, "ERROR: DB connection failed");
            showEmptyPage(request, response, connection, page);
            return;
        } else if (result.rev_not_existing) {
            result = getRevision(connection, title, namespace);
            page.addStats(result.stats);
            page.getInvolvedKeys().addAll(result.involvedKeys);
            addToParam_notice(request, "revision " + req_oldid + " not found - loaded current revision instead");
        }

        if (result.page_not_existing) {
            page.setVersion(-1);
            page.setNewPage(true);
        } else if (result.rev_not_existing) {
            // DB corrupt
            setParam_error(request, "ERROR: revision unavailable");
            addToParam_notice(request, "error: unknown error getting current revision of page \"" + title + "\": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response, connection, page);
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

        forwardToPageJsp(request, response, connection, page, "pageEdit.jsp");
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
            Revision newRev = new Revision(-1, timestamp, minorChange, contributor, summary);
            newRev.setUnpackedText(content);

            SavePageResult result;
            int retries = 0;
            while (true) {
                result = savePage(connection, title, newRev, oldVersion, null, siteinfo, "", namespace);
                page.addStats(result.stats);
                page.getInvolvedKeys().addAll(result.involvedKeys);
                if (!result.failedKeys.isEmpty()) {
                    page.getFailedKeys().put(retries + 1, result.failedKeys);
                }
                if (!result.success && retries < Options.getInstance().WIKI_SAVEPAGE_RETRIES) {
                    // check for conflicting edit on same page, do not retry in this case
                    final Page oldPage = result.oldPage;
                    if (oldPage != null && oldPage.getCurRev().getId() != oldVersion) {
                        break;
                    }
                    try {
                        Thread.sleep(Options.getInstance().WIKI_SAVEPAGE_RETRY_DELAY);
                    } catch (InterruptedException e) {
                    }
                    ++retries;
                } else {
                    break;
                }
            }
            page.setSaveAttempts(retries + 1);
            for (WikiEventHandler handler: eventHandlers) {
                handler.onPageSaved(page, result, connection);
            }
            if (result.success) {
                // successfully saved -> show page with a notice of the successful operation
                // also actively update the bloom filter of existing pages
                existingPages.add(NormalisedTitle.fromUnnormalised(title, namespace));
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
                redirectUrl.append("&save_attempts=" + page.getSaveAttempts());
                for (Entry<Integer, List<String>> failedKeys : page.getFailedKeys().entrySet()) {
                    redirectUrl.append("&failed_keys" + failedKeys.getKey() + "=" + URLEncoder.encode(StringUtils.join(failedKeys.getValue(), " # "), "UTF-8"));
                }
                redirectUrl.append("&involved_keys=" + URLEncoder.encode(StringUtils.join(page.getInvolvedKeys(), " # "), "UTF-8"));
                redirectUrl.append("&server_time=" + (System.currentTimeMillis() - page.getStartTime()));
                if (result.newPage.isRedirect()) {
                    redirectUrl.append("&redirect=no");
                }
                final String serviceUser = page.getServiceUser().isEmpty() ? "" : "&service_user=" + page.getServiceUser();
                response.sendRedirect("http://"
                        + Options.getInstance().SERVERNAME
                        + Options.getInstance().SERVERPATH
                        + response.encodeRedirectURL(redirectUrl.toString())
                        + serviceUser);
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

        MyWikiModel wikiModel = getWikiModel(connection, page);
        String[] titleParts = wikiModel.splitNsTitle(title);
        wikiModel.setNamespaceName(titleParts[0]);
        wikiModel.setPageName(titleParts[1]);
        page.setPreview(wikiModel.renderPageWithCache(content));
        page.setIncludes(wikiModel.getIncludes());
        page.setTemplates(wikiModel.getTemplatesNoMagicWords());
        page.addStats(wikiModel.getStats());
        page.getInvolvedKeys().addAll(wikiModel.getInvolvedKeys());
        page.setPage(content);
        page.setVersion(oldVersion);
        page.setError(getParam_error(request));
        page.setTitle(title);
        page.setSummary(request.getParameter("wpSummary"));
        page.setWikiTitle(siteinfo.getSitename());
        page.setWikiNamespace(namespace);

        forwardToPageJsp(request, response, connection, page, "pageEdit.jsp");
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
    private void showImage(final HttpServletRequest request,
            final HttpServletResponse response, final String image)
            throws UnsupportedEncodingException, IOException, ServletException {
        // we need to fix the image title first, e.g. a prefix with the desired size may exist
        final String imageNoSize = MATCH_WIKI_IMAGE_PX.matcher(image).replaceFirst("");
        String realImageUrl = getWikiImageUrl(imageNoSize);
        if (realImageUrl == null) {
            // bliki may have created ".svg.png" from an original ".svg" image:
            String new_image = MATCH_WIKI_IMAGE_SVG_PNG.matcher(imageNoSize).replaceFirst(".svg");
            realImageUrl = getWikiImageUrl(new_image);
            if (imageNoSize.equals(new_image) || realImageUrl == null) {
                realImageUrl = response.encodeRedirectURL("images/image.png");
            }
        }

        for (WikiEventHandler handler: eventHandlers) {
            handler.onImageRedirect(image, realImageUrl);
        }
        
        response.sendRedirect(realImageUrl);
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

        // set image width thumb size to 400px
        List<info.bliki.api.Page> pages = user.queryImageinfo(new String[] { image }, 400);
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
    
    abstract protected MyWikiModel getWikiModel(Connection connection, WikiPageBeanBase page);

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
     * @return the requested parameter or ""
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

    /**
     * Check whether the page was reached by a "redlink" URL (in this case, if
     * the edit page was requested but the page exists, show the page instead)
     * 
     * @param request
     *            the http request
     * @return <tt>true</tt> if it was a redlink, <tt>false</tt> otherwise
     */
    private static boolean getParam_redlink(HttpServletRequest request) {
        String req_redlink = request.getParameter("redlink");
        return parseInt(req_redlink, 0) == 1;
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
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletContext#getNamespace()
     */
    @Override
    public final NamespaceUtils getNamespace() {
        return namespace;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletContext#getSiteinfo()
     */
    @Override
    public SiteInfo getSiteinfo() {
        return siteinfo;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletContext#getVersion()
     */
    @Override
    public String getVersion() {
        return version;
    }

    @Override
    public String getDbVersion() {
        Connection connection = getConnection(null);
        if (connection == null) {
            return "n/a"; // return just in case
        }
        return getDbVersionStr(connection);
    }

    protected String getDbVersionStr(Connection connection) {
        ValueResult<String> dbVersion = getDbVersion(connection);
        if (dbVersion.success) {
            return dbVersion.value;
        } else {
            return "n/a";
        }
    }

    @Override
    public String getServerVersion() {
        return getServletContext().getServerInfo();
    }

    @Override
    public String getBlikiVersion() {
        return Configuration.BLIKI_VERSION;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletContext#getLinkbaseurl(WikiPageBeanBase)
     */
    @Override
    public String getLinkbaseurl(WikiPageBeanBase page) {
        final String serviceUser = page.getServiceUser().isEmpty() ? "" : "&service_user=" + page.getServiceUser();
        return Options.getInstance().SERVERPATH + "?title=${title}" + serviceUser;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletContext#getImagebaseurl(WikiPageBeanBase)
     */
    @Override
    public String getImagebaseurl(WikiPageBeanBase page) {
        final String serviceUser = page.getServiceUser().isEmpty() ? "" : "&service_user=" + page.getServiceUser();
        return Options.getInstance().SERVERPATH + "?get_image=${image}" + serviceUser;
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.bliki.WikiServletContext#getImagebaseurl()
     */
    @Override
    public synchronized void registerEventHandler(WikiEventHandler handler) {
        eventHandlers.add(handler);
    }

    /**
     * Updates the bloom filter of existing pages for quick checks.
     */
    protected void updateExistingPages() {
        if (initialized) {
            Connection connection = getConnection(null);
            if (connection != null) {
                try {
                    ValueResult<List<NormalisedTitle>> result = getPageList(connection);
                    if (result.success) {
                        List<NormalisedTitle> pages = result.value;
                        pages.addAll(specialPages);
                        ExistingPagesCache filter = ExistingPagesCache.createCache(pages);
                        existingPages = filter;
                    }
                } finally {
                    releaseConnection(null, connection);
                }
            }
        }
    }
    
    @Override
    public void storeUserReq(WikiPageBeanBase page, long servertime) {
        long timestamp = page.getStartTime();
        String serviceUser = page.getServiceUser();
        if (!eventHandlers.isEmpty()) {
            Connection connection = getConnection(null);
            for (WikiEventHandler handler: eventHandlers) {
                try {
                    handler.onPageView(page, connection);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            releaseConnection(null, connection);
        }
        if (userReqLogs != null && userReqLogs.length > 0) {
            int timestamp_s = (int) (timestamp / 1000);
            // build log entry:
            Map<String, Object> entry = new HashMap<String, Object>();
            entry.put("timestamp", timestamp_s);
            entry.put("serviceuser", serviceUser == null ? "" : serviceUser);
            entry.put("servertime", servertime);
            
            // get the correct bucket:
            LinkedList<Map<String, Object>> curReqLogList;
            synchronized (userReqLogs) {
                curReqLogList = userReqLogs[curReqLog];
                int diffInMin = (timestamp_s - curReqLogStartTime) / 60;
                if (diffInMin > 0) {
                    if (diffInMin > userReqLogs.length) {
                        for (LinkedList<Map<String, Object>> log : userReqLogs) {
                            log.clear();
                        }
                        curReqLog = 0;
                        curReqLogList = userReqLogs[curReqLog];
                    } else  {
                        while ((curReqLogStartTime + 60) < timestamp_s) {
                            curReqLogList = userReqLogs[curReqLog];
                            curReqLogList.clear();
                            curReqLog = (++curReqLog) % userReqLogs.length;
                        }
                    }
                    curReqLogStartTime += 60 * diffInMin;
                }
            }
            curReqLogList.add(entry);
        }
    }
}
