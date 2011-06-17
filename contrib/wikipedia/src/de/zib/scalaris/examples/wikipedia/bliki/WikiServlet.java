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

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Calendar;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Properties;
import java.util.Random;
import java.util.TimeZone;

import javax.servlet.RequestDispatcher;
import javax.servlet.Servlet;
import javax.servlet.ServletConfig;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang.StringEscapeUtils;

import de.zib.scalaris.Connection;
import de.zib.scalaris.ConnectionFactory;
import de.zib.scalaris.TransactionSingleOp;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.PageHistoryResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.PageListResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.RandomTitleResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.RevisionResult;
import de.zib.scalaris.examples.wikipedia.ScalarisDataHandler.SaveResult;
import de.zib.scalaris.examples.wikipedia.data.Contributor;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.data.SiteInfo;

/**
 * Servlet for handling wiki page display and editing.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class WikiServlet extends HttpServlet implements Servlet {
    private static final long serialVersionUID = 1L;

    private Connection connection = null; 
    private SiteInfo siteinfo = null;
    private MyNamespace namespace = null;
    private Random random = null;
    
    private boolean initialized = false;

    /**
     * Name under which the servlet is available.
     */
    public static final String wikiBaseURL = "wiki";
    /**
     * URL for page links
     */
    public static final String linkBaseURL = WikiServlet.wikiBaseURL + "?title=${title}";
    /**
     * URL for image links
     */
    public static final String imageBaseURL = "images/image.png";
    
    private ConnectionFactory cFactory;

    /**
     * Creates the servlet. 
     */
    public WikiServlet() {
        super();
    }

    /**
     * Servlet initialisation: creates the connection to the erlang node.
     * 
     * @see #setupChordSharpConnection(HttpServletRequest)
     */
    @Override
    public void init(ServletConfig config) throws ServletException {
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
        
        if (properties != null) {
            cFactory = new ConnectionFactory(properties);
            cFactory.setClientNameAppendUUID(true);
        } else {
            cFactory = new ConnectionFactory();
            cFactory.setClientName("wiki_renderer");
            cFactory.setClientNameAppendUUID(true);
        }
        
        setupChordSharpConnection(null);
        random = new Random();
    }

        /**
     * Sets up the connection to the Scalaris erlang node once on the server.
     * 
     * @param config
     *            the servlet's configuration
     */
    private boolean setupChordSharpConnection(HttpServletRequest request) {
        try {
            connection = cFactory.createConnection();
            TransactionSingleOp scalaris_single = new TransactionSingleOp(connection);
            siteinfo = scalaris_single.read("siteinfo").jsonValue(SiteInfo.class);
            namespace = new MyNamespace(siteinfo);
            initialized = true;
            return true;
        } catch (Exception e) {
            if (request != null) {
                addToParam_notice(request, "error: <pre>" + e.getMessage() + "</pre>");
            } else {
                System.out.println(e);
                e.printStackTrace();
            }
            return false;
        }
    }

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
        if (!initialized && !setupChordSharpConnection(request)) {
            showEmptyPage(request, response);
            return;
        }
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        
        // show empty page for testing purposes if a parameter called "test" exists:
        if (request.getParameter("test") != null) {
            showEmptyPage(request, response);
            return;
        }
        
        // get parameters:
        String req_title = request.getParameter("title");
        if (req_title == null) {
            req_title = "Main Page";
        }

        
        String req_action = request.getParameter("action");

        if (req_title.equals("Special:Random")) {
            handleViewRandomPage(request, response);
        } else if (req_title.startsWith("Special:AllPages") || req_title.startsWith("Special:Allpages")) {
            String prefix = "";
            int slashIndex = req_title.indexOf('/');
            if (slashIndex != (-1)) {
                prefix = req_title.substring(slashIndex + 1);
            } 
            handleViewSpecialPageList(request, response, prefix);
        } else if (req_action == null || req_action.equals("view")) {
            handleViewPage(request, response, req_title);
        } else if (req_action.equals("history")) {
            handleViewPageHistory(request, response, req_title);
        } else if (req_action.equals("edit")) {
            handleEditPage(request, response, req_title);
        } else {
            // default: show page
            handleViewPage(request, response, req_title);
        }

        // if the request has not been forwarded, print a general error
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.write("An unknown error occured, please contact your administrator. A server restart may be required.");
        out.close();
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
        if (!initialized && !setupChordSharpConnection(request)) {
            showEmptyPage(request, response);
        }
        request.setCharacterEncoding("UTF-8");
        response.setCharacterEncoding("UTF-8");
        
        // for debugging, show all parameters:
//        Enumeration test = request.getParameterNames();
//        while (test.hasMoreElements()) {
//            String element = (String) test.nextElement();
//            System.out.println(element);
//            System.out.println(request.getParameter(element));
//        }
        handleEditPageSubmitted(request, response, request.getParameter("title"));
        
        // if the request has not been forwarded, print a general error
        response.setContentType("text/html");
        PrintWriter out = response.getWriter();
        out.write("An unknown error occured, please contact your administrator. A server restart may be required.");
        out.close();
    }

    /**
     * Gets a random page and forwards the user to the site with this page.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * 
     * @throws IOException
     *            if the forward fails
     */
    private void handleViewRandomPage(HttpServletRequest request,
            HttpServletResponse response) throws IOException {
        // TODO: filter out template and category pages
        RandomTitleResult result = ScalarisDataHandler.getRandomTitle(connection, random);
        if (result.success) {
            response.sendRedirect(response.encodeRedirectURL("?title=" + URLEncoder.encode(result.title, "UTF-8")));
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
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewPage(HttpServletRequest request,
            HttpServletResponse response, String title) throws ServletException, IOException {
        // get renderer
        int render = WikiServlet.getParam_renderer(request);
        // get revision id to load:
        int req_oldid = WikiServlet.getParam_oldid(request);

        RevisionResult result = ScalarisDataHandler.getRevision(connection, title, req_oldid);
        if (result.page_not_existing) {
            handleViewPageNotExisting(request, response, title);
            return;
        } else if (result.rev_not_existing) {
            result = ScalarisDataHandler.getRevision(connection, title);
            addToParam_notice(request, "revision " + req_oldid + " not found - loaded current revision instead");
        }
        
        if (result.success) {
            WikiPageBean value = renderRevision(title, result.revision, render, request);
            
            if (!result.page.checkEditAllowed("")) {
                value.setEditRestricted(true);
            }

            // forward the request and the bean to the jsp:
            request.setAttribute("pageBean", value);
            RequestDispatcher dispatcher = request
                    .getRequestDispatcher("page.jsp");
            dispatcher.forward(request, response);
        } else {
            addToParam_notice(request, "error: unknown error getting page " + title + ":" + req_oldid + ": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response);
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
     * 
     * @return a bean with the rendered content for the jsp
     */
    private WikiPageBean renderRevision(String title, Revision revision, int renderer, HttpServletRequest request) {
        String notice = WikiServlet.getParam_notice(request);
        // set the page's contents according to the renderer used
        // (categories are included in the content string, so they only
        // need special handling the wiki renderer is used)
        WikiPageBean value = new WikiPageBean();
        value.setNotice(notice);
        MyWikiModel wikiModel = getWikiModel();
        String fullUrl = extractFullUrl(request.getRequestURL().toString());
        if (fullUrl != null) {
            wikiModel.setLinkBaseFullURL(fullUrl);
        }
        wikiModel.setPageName(title);
        if (renderer > 0) {
            String mainText = wikiModel.render(revision.getText());
            if (wikiModel.isCategoryNamespace(MyWikiModel.getNamespace(title))) {
                PageListResult catPagesResult = ScalarisDataHandler.getPagesInCategory(connection, title);
                if (catPagesResult.success) {
                    LinkedList<String> subCategories = new LinkedList<String>();
                    LinkedList<String> categoryPages = new LinkedList<String>();
                    for (String page: catPagesResult.pages) {
                        String pageNamespace = MyWikiModel.getNamespace(page);
                        if (wikiModel.isCategoryNamespace(pageNamespace)) {
                            subCategories.add(MyWikiModel.getTitleName(page));
                        } else if (wikiModel.isTemplateNamespace(pageNamespace)) {
                            // all pages using a template are in the category, too
                            PageListResult tplResult = ScalarisDataHandler.getPagesInTemplate(connection, page);
                            if (tplResult.success) {
                                categoryPages.addAll(tplResult.pages);
                            }
                        } else {
                            categoryPages.add(page);
                        }
                    }
                    Collections.sort(subCategories);
                    Collections.sort(categoryPages);
                    value.setSubCategories(subCategories);
                    value.setCategoryPages(categoryPages);
                } else {
                    addToParam_notice(request, "error getting category pages: " + catPagesResult.message);
                }
            }
            if (wikiModel.getRedirectLink() != null) {
                String redirectedPageName = wikiModel.getRedirectLink();
                value.setRedirectedTo(redirectedPageName);
                // add the content from the page directed to:
                mainText = wikiModel.render(wikiModel.getRedirectContent(redirectedPageName));
            }
            value.setPage(mainText);
            value.setCategories(wikiModel.getCategories().keySet());
        } else if (renderer == 0) {
            value.setPage("<p>WikiText:<pre>"
                    + StringEscapeUtils.escapeHtml(revision.getText()) + "</pre></p>" +
                    "<p>Version:<pre>"
                    + StringEscapeUtils.escapeHtml(String.valueOf(revision.getId())) + "</pre></p>" +
                    "<p>Last change:<pre>"
                    + StringEscapeUtils.escapeHtml(revision.getTimestamp()) + "</pre></p>" +
                    "<p>Categories:<pre>"
                    + StringEscapeUtils.escapeHtml(revision.parseCategories(wikiModel).toString()) + "</pre></p>");
        }

        value.setTitle(title);
        value.setVersion(revision.getId());
        value.setWikiTitle(siteinfo.getSitename());
        value.setWikiNamespace(namespace);

        // extract the date:
        value.setDate(Revision.stringToCalendar(revision.getTimestamp()));
        return value;
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
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewPageNotExisting(HttpServletRequest request,
            HttpServletResponse response, String title) throws ServletException, IOException {
        // get renderer
        int render = WikiServlet.getParam_renderer(request);
        String notExistingTitle = "MediaWiki:Noarticletext";

        RevisionResult result = ScalarisDataHandler.getRevision(connection, notExistingTitle);
        
        WikiPageBean value;
        if (result.success) {
            value = renderRevision(title, result.revision, render, request);
        } else {
            addToParam_notice(request, "error: unknown error getting page " + notExistingTitle + ": <pre>" + result.message + "</pre>");
            value = new WikiPageBean();
            value.setPage("Page not available.");
            value.setTitle(title);
        }
        // re-set version (we are only showing this page due to a non-existing page)
        value.setVersion(-1);
        value.setNotAvailable(true);
        value.setNotice(WikiServlet.getParam_notice(request));
        value.setWikiTitle(siteinfo.getSitename());
        value.setWikiNamespace(namespace);

        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", value);
        RequestDispatcher dispatcher = request
                .getRequestDispatcher("page.jsp");
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
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewPageHistory(HttpServletRequest request,
            HttpServletResponse response, String title) throws ServletException, IOException {
        PageHistoryResult result = ScalarisDataHandler.getPageHistory(connection, title);
        if (result.not_existing) {
            handleViewPageNotExisting(request, response, title);
            return;
        }
        
        if (result.success) {
            WikiPageBean value = new WikiPageBean();
            value.setNotice(WikiServlet.getParam_notice(request));
            value.setRevisions(result.revisions);
            if (!result.page.checkEditAllowed("")) {
                value.setEditRestricted(true);
            }
            
            value.setTitle(title);
            if (!result.revisions.isEmpty()) { 
                value.setVersion(result.revisions.get(0).getId());
            }
            value.setWikiTitle(siteinfo.getSitename());
            value.setWikiNamespace(namespace);
            
            // forward the request and the bean to the jsp:
            request.setAttribute("pageBean", value);
            RequestDispatcher dispatcher = request
                    .getRequestDispatcher("pageHistory.jsp");
            dispatcher.forward(request, response);
        } else {
            addToParam_notice(request, "error: unknown error getting revision list for page " + title + ": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response);
        }
    }

    /**
     * Shows the page containing the list of pages.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleViewSpecialPageList(HttpServletRequest request,
            HttpServletResponse response, String prefix) throws ServletException, IOException {
        PageListResult result = ScalarisDataHandler.getPageList(connection);
        
        if (result.success) {
            WikiPageListBean value = new WikiPageListBean();
            value.setNotice(WikiServlet.getParam_notice(request));
            Collections.sort(result.pages);
            String last = prefix;
            if (!prefix.equals("")) {
                // only show pages with this prefix:
                for (Iterator<String> it = result.pages.iterator(); it.hasNext(); ) {
                    String cur = it.next();
                    if (!cur.startsWith(prefix)) {
                        it.remove();
                    } else {
                        last = cur;
                    }
                }
            }
            String first = prefix;
            if (!result.pages.isEmpty()) {
                first = result.pages.get(0);
            }
            value.setPages(result.pages);
            value.setFromPage(first);
            value.setToPage(last);
            
            value.setTitle("Special:AllPages");
            value.setWikiTitle(siteinfo.getSitename());
            value.setWikiNamespace(namespace);
            
            // forward the request and the bean to the jsp:
            request.setAttribute("pageBean", value);
            RequestDispatcher dispatcher = request
                    .getRequestDispatcher("pageSpecial_pagelist.jsp");
            dispatcher.forward(request, response);
        } else {
            addToParam_notice(request, "error: unknown error getting page list: <pre>" + result.message + "</pre>");
            showEmptyPage(request, response);
        }
    }
    
    /**
     * Shows an empty page for testing purposes.
     * 
     * @param request
     *            the request of the current operation
     * @param response
     *            the response of the current operation
     * 
     * @throws IOException 
     * @throws ServletException 
     */
    private void showEmptyPage(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        WikiPageBean value = new WikiPageBean();
        value.setNotice(WikiServlet.getParam_notice(request));
        request.setAttribute("pageBean", value);
        RequestDispatcher dispatcher = request.getRequestDispatcher("page.jsp");
        dispatcher.forward(request, response);
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
     *            
     * @throws IOException 
     * @throws ServletException 
     */
    private void handleEditPage(HttpServletRequest request,
            HttpServletResponse response, String title) throws ServletException, IOException {
        // get revision id to load:
        int req_oldid = WikiServlet.getParam_oldid(request);

        WikiPageEditBean value = new WikiPageEditBean();
        RevisionResult result = ScalarisDataHandler.getRevision(connection, title, req_oldid);
        if (result.rev_not_existing) {
            result = ScalarisDataHandler.getRevision(connection, title);
            addToParam_notice(request, "revision " + req_oldid + " not found - loaded current revision instead");
        }

        if (result.page_not_existing) {
            value.setVersion(-1);
            value.setNewPage(true);
        } else if (result.rev_not_existing) {
            // DB corrupt
            addToParam_notice(request, "error: unknown error getting current revision of page \"" + title + "\": <pre>" + result.message + "</pre>");
            showEmptyPage(request, response);
            return;
        }
        if (result.success) {
            if (!result.page.checkEditAllowed("")) {
                value.setEditRestricted(true);
            }
            value.setPage(StringEscapeUtils.escapeHtml(result.revision.getText()));
            value.setVersion(result.revision.getId());
        }

        // set the textarea's contents:
        value.setNotice(WikiServlet.getParam_notice(request));
        value.setTitle(title);
        value.setWikiTitle(siteinfo.getSitename());
        value.setWikiNamespace(namespace);

        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", value);
        RequestDispatcher dispatcher = request
                .getRequestDispatcher("pageEdit.jsp");
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
    private void handleEditPageSubmitted(HttpServletRequest request,
            HttpServletResponse response, String title) throws UnsupportedEncodingException, IOException, ServletException {
        String content = request.getParameter("wpTextbox1");
        String summary = request.getParameter("wpSummary");
        int oldVersion = Integer.parseInt(request.getParameter("oldVersion"));
        boolean minorChange = Boolean.parseBoolean(request.getParameter("minor"));

        // save page or preview+edit page?
        if (request.getParameter("wpSave") != null) {
            // save page
            Contributor contributor = new Contributor();
            contributor.setIp(request.getRemoteAddr());
            String timestamp = Revision.calendarToString(Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            int newRevId = (oldVersion == -1) ? 1 : oldVersion + 1;
            Revision newRev = new Revision(newRevId, timestamp, minorChange, contributor, summary, content);

            SaveResult result = ScalarisDataHandler.savePage(connection, title, newRev, oldVersion, null, siteinfo, "");
            if (result.success) {
                // successfully saved -> show page with a notice of the successful operation
                // do not include the UTF-8-title directly into encodeRedirectURL since that's not 
                // encoding umlauts (maybe other special chars as well) correctly, e.g. ä -> %E4 instead of %C3%A4
                response.sendRedirect(response.encodeRedirectURL("?title=" + URLEncoder.encode(title, "UTF-8") + "&notice=successfully saved page"));
                return;
            } else {
                // set error message and show the edit page again (see below)
                addToParam_notice(request, "error: could not save page: <pre>" + result.message + "</pre>");
            }
        }

        // preview+edit page

        WikiPageEditBean value = new WikiPageEditBean();
        value.setNotice(WikiServlet.getParam_notice(request));
        // set the textarea's contents:
        value.setPage(StringEscapeUtils.escapeHtml(content));

        MyWikiModel wikiModel = getWikiModel();
        wikiModel.setPageName(title);
        String fullUrl = extractFullUrl(request.getRequestURL().toString());
        if (fullUrl != null) {
            wikiModel.setLinkBaseFullURL(fullUrl);
        }
        value.setPreview(wikiModel.render(content));
        value.setPage(content);
        value.setVersion(oldVersion);
        value.setTitle(title);
        value.setSummary(request.getParameter("wpSummary"));
        value.setWikiTitle(siteinfo.getSitename());
        value.setWikiNamespace(namespace);

        // forward the request and the bean to the jsp:
        request.setAttribute("pageBean", value);
        RequestDispatcher dispatcher = request
                .getRequestDispatcher("pageEdit.jsp");
        dispatcher.forward(request, response);
    }
    
    private MyWikiModel getWikiModel() {
        return new MyWikiModel(WikiServlet.imageBaseURL,
                WikiServlet.linkBaseURL, connection, namespace);
    }

    /**
     * Adds the given notice to the notice attribute .
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
     * Returns the notice parameter (or attribute if the parameter does not exist) or an empty string if both are not present
     * 
     * @param request
     *            the http request
     * @return the notice parameter or ""
     */
    public static String getParam_notice(HttpServletRequest request) {
        String req_notice = request.getParameter("notice");
        if (req_notice == null) {
            Object temp = request.getAttribute("notice");
            if (temp instanceof String) {
                req_notice = (String) temp; 
            }
        }
        if (req_notice == null) {
            return new String("");
        } else {
            return req_notice;
        }
    }

    /**
     * Get the revision id to load from the request object
     * 
     * @param request
     *            the http request
     * @return the revision id or -1 on failure to parse
     */
    public static int getParam_oldid(HttpServletRequest request) {
        String req_oldid = request.getParameter("oldid");
        int result;
        try {
            result = Integer.parseInt(req_oldid);
        } catch (NumberFormatException e) {
            result = -1;
        }
        return result;
    }

    /**
     * Determines which renderer should be used by evaluating the render
     * parameter of the request
     * 
     * @param request
     *            the http request
     * @return the renderer id
     */
    public static int getParam_renderer(HttpServletRequest request) {
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
}
