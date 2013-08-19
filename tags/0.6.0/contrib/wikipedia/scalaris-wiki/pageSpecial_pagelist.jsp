<?xml version="1.0" encoding="UTF-8" ?>
<%@page import="de.zib.scalaris.examples.wikipedia.bliki.WikiServlet"%>
<%@page import="de.zib.scalaris.examples.wikipedia.InvolvedKey"%>
<%@page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"
 import="java.util.Calendar,java.util.Locale,java.text.DateFormat,java.text.SimpleDateFormat,java.util.TimeZone,java.util.Iterator,de.zib.scalaris.examples.wikipedia.bliki.WikiPageListBean,java.util.Map,java.util.List,org.apache.commons.lang.StringEscapeUtils,java.net.URLEncoder"%>
<% String req_render = request.getParameter("render"); %>
<jsp:useBean id="pageBean" type="de.zib.scalaris.examples.wikipedia.bliki.WikiPageListBean" scope="request" />
<jsp:useBean id="servlet" type="de.zib.scalaris.examples.wikipedia.WikiServletContext" scope="request" />
<% /* created page based on https://secure.wikimedia.org/wiktionary/simple/wiki/relief */ %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html lang="${ pageBean.wikiLang }" dir="${ pageBean.wikiLangDir }" xmlns="http://www.w3.org/1999/xhtml">
<head>
<%
final String safePageTitle = StringEscapeUtils.escapeHtml(URLEncoder.encode(pageBean.getTitle(), "UTF-8"));
final String pageTitleWithPars = pageBean.titleWithParameters();
final String safePageTitleWithPars = StringEscapeUtils.escapeHtml(pageTitleWithPars);

final String safeFromTitle = StringEscapeUtils.escapeHtml(pageBean.getFromPage());
final String safeToTitle = StringEscapeUtils.escapeHtml(pageBean.getToPage());
final String safeTargetTitle = StringEscapeUtils.escapeHtml(pageBean.getTarget());
final String safePrefixTitle = StringEscapeUtils.escapeHtml(pageBean.getPrefix());
final String safeSearchTitle = StringEscapeUtils.escapeHtml(pageBean.getSearch());
final String andServiceUser = pageBean.getServiceUser().isEmpty() ? "" : "&amp;service_user=" + pageBean.getServiceUser();
%>
<title>${ pageBean.pageHeading } - ${ pageBean.wikiTitle }</title>
<!--<% if (!pageBean.getError().isEmpty()) { %>
<error>${ pageBean.error }</error>
<% } %>-->
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta http-equiv="Content-Style-Type" content="text/css" />
<% /* 
<meta name="generator" content="MediaWiki 1.17wmf1" />
<link rel="apple-touch-icon" href="http://simple.wiktionary.org/apple-touch-icon.png">
*/ %>
<link rel="shortcut icon" href="favicon-wikipedia.ico" />
<% /*
<link rel="search" type="application/opensearchdescription+xml" href="https://secure.wikimedia.org/wiktionary/simple/w/opensearch_desc.php" title="Wiktionary (simple)">
<link rel="EditURI" type="application/rsd+xml" href="https://secure.wikimedia.org/wiktionary/simple/w/api.php?action=rsd">
*/ %>
<link rel="copyright" href="http://creativecommons.org/licenses/by-sa/3.0/" />
<% /*
<link rel="alternate" type="application/atom+xml" title="Wiktionary Atom feed" href="https://secure.wikimedia.org/wiktionary/simple/w/index.php?title=Special:RecentChanges&amp;feed=atom">
*/ %>
<link rel="stylesheet" href="skins/load_002.css" type="text/css" media="all" />
<style type="text/css" media="all">.suggestions{overflow:hidden;position:absolute;top:0px;left:0px;width:0px;border:none;z-index:99;padding:0;margin:-1px -1px 0 0} html > body .suggestions{margin:-1px 0 0 0}.suggestions-special{position:relative;background-color:Window;font-size:0.8em;cursor:pointer;border:solid 1px #aaaaaa;padding:0;margin:0;margin-top:-2px;display:none;padding:0.25em 0.25em;line-height:1.25em}.suggestions-results{background-color:white;background-color:Window;font-size:0.8em;cursor:pointer;border:solid 1px #aaaaaa;padding:0;margin:0}.suggestions-result{color:black;color:WindowText;margin:0;line-height:1.5em;padding:0.01em 0.25em;text-align:left}.suggestions-result-current{background-color:#4C59A6;background-color:Highlight;color:white;color:HighlightText}.suggestions-special .special-label{font-size:0.8em;color:gray;text-align:left}.suggestions-special .special-query{color:black;font-style:italic;text-align:left}.suggestions-special .special-hover{background-color:silver}.suggestions-result-current .special-label,.suggestions-result-current .special-query{color:white;color:HighlightText}.autoellipsis-matched,.highlight{font-weight:bold}</style>
<style type="text/css" media="all">#mw-panel.collapsible-nav div.portal{background-image:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAIwAAAABCAMAAAA7MLYKAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAEtQTFRF29vb2tra4ODg6urq5OTk4uLi6+vr7e3t7Ozs8PDw5+fn4+Pj4eHh3d3d39/f6Ojo5eXl6enp8fHx8/Pz8vLy7+/v3Nzc2dnZ2NjYnErj7QAAAD1JREFUeNq0wQUBACAMALDj7hf6JyUFGxzEnYhC9GaNPG1xVffGDErk/iCigLl1XV2xM49lfAxEaSM+AQYA9HMKuv4liFQAAAAASUVORK5CYII=);background-image:url(https://secure.wikimedia.org/w/extensions-1.17/Vector/modules/./images/portal-break.png?2011-02-12T21:25:00Z)!ie;background-position:left top;background-repeat:no-repeat;padding:0.25em 0 !important;margin:-11px 9px 10px 11px}#mw-panel.collapsible-nav div.portal h5{color:#4D4D4D;font-weight:normal;background:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQBAMAAADt3eJSAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAA9QTFRFeXl53d3dmpqasbGx////GU0iEgAAAAV0Uk5T/////wD7tg5TAAAAK0lEQVQI12NwgQIG0hhCDAwMTCJAhqMCA4MiWEoIJABiOCooQhULi5BqMgB2bh4svs8t+QAAAABJRU5ErkJggg==) left center no-repeat;background:url(https://secure.wikimedia.org/w/extensions-1.17/Vector/modules/./images/open.png?2011-02-12T21:25:00Z) left center no-repeat!ie;padding:4px 0 3px 1.5em;margin-bottom:0px}#mw-panel.collapsible-nav div.collapsed h5{color:#0645AD;background:url(data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAABAAAAAQCAMAAAAoLQ9TAAAAGXRFWHRTb2Z0d2FyZQBBZG9iZSBJbWFnZVJlYWR5ccllPAAAAAxQTFRF3d3deXl5////nZ2dQA6SoAAAAAN0Uk5T//8A18oNQQAAADNJREFUeNpiYEIDDMQKMKALMDOgCTDCRWACcBG4AEwEIcDITEAFuhnotmC4g4EEzwEEGAADqgHmQSPJKgAAAABJRU5ErkJggg==) left center no-repeat;background:url(https://secure.wikimedia.org/w/extensions-1.17/Vector/modules/./images/closed-ltr.png?2011-02-12T21:25:00Z) left center no-repeat!ie;margin-bottom:0px}#mw-panel.collapsible-nav div h5:hover{cursor:pointer;text-decoration:none}#mw-panel.collapsible-nav div.collapsed h5:hover{text-decoration:underline}#mw-panel.collapsible-nav div.portal div.body{background:none !important;padding-top:0px;display:none}#mw-panel.collapsible-nav div.persistent div.body{display:block}#mw-panel.collapsible-nav div.first h5{display:none}#mw-panel.collapsible-nav div.persistent h5{background:none !important;padding-left:0.7em;cursor:default}#mw-panel.collapsible-nav div.portal div.body ul li{padding:0.25em 0}#mw-panel.collapsible-nav div.first{background-image:none;margin-top:0px}#mw-panel.collapsible-nav div.persistent div.body{margin-left:0.5em}</style>
<meta name="ResourceLoaderDynamicStyles" content="" />
<link rel="stylesheet" href="skins/load.css" type="text/css" media="all" />
<style type="text/css" media="all">a.new,#quickbar a.new{color:#ba0000}</style>
</head>
<body class="mediawiki ltr ns-0 ns-subject skin-vector">
        <div id="mw-page-base" class="noprint"></div>
        <div id="mw-head-base" class="noprint"></div>
        <!-- content -->
        <div id="content">
            <a id="top"></a>
            <div id="mw-js-message" style="display:none;"></div>
            <!-- sitenotice -->
            <div id="siteNotice"><!-- centralNotice loads here -->${ pageBean.notice }</div>
            <!-- /sitenotice -->
            <!-- firstHeading -->
            <h1 id="firstHeading" class="firstHeading">${ pageBean.error }${ pageBean.pageHeading }</h1>
            <!-- /firstHeading -->
            <!-- bodyContent -->
            <div id="bodyContent">
                <!-- tagline -->
                <div id="siteSub">From <%= pageBean.getWikiNamespace().getMeta() %></div>
                <!-- /tagline -->
                <!-- subtitle -->
                <div id="contentSub">
                </div>
                <!-- /subtitle -->
                <!-- jumpto -->
                <div id="jump-to-nav">
                    Jump to: <a href="#mw-head">navigation</a>,
                    <a href="#p-search">search</a>
                </div>
                <!-- /jumpto -->
                <!-- bodytext -->

${ pageBean.page }

                <% if (pageBean.getFormType() != WikiPageListBean.FormType.NoForm ) { %>
                <table class="mw-allpages-table-form">
                  <tr>
                    <td>
                      <div class="namespaceoptions">
                        <form method="get" action="wiki">
                          <input type="hidden" value="${ pageBean.title }" name="title" />
                          <fieldset>
                            <legend>${ pageBean.formTitle }</legend>
                            <table id="nsselect" class="allpages">
                      <% if (pageBean.getFormType() == WikiPageListBean.FormType.FromToForm ) { %>
                              <tr>
                                <td class='mw-label'><label for="nsfrom">Display pages starting at:</label> </td>
                                <td class='mw-input'><input name="from" size="30" value="<%= safeFromTitle %>" id="nsfrom" />  </td>
                              </tr>
                              <tr>
                                <td class='mw-label'><label for="nsto">Display pages ending at:</label> </td>
                                <td class='mw-input'><input name="to" size="30" value="<%= safeToTitle %>" id="nsto" />      </td>
                              </tr>
                      <% } else if (pageBean.getFormType() == WikiPageListBean.FormType.TargetPageForm) { %>
                              <tr>
                                <td class='mw-label'><label for="target">Page:</label> </td>
                                <td class='mw-input'><input name="target" size="30" value="<%= safeTargetTitle %>" id="nstarget" /> <input type="submit" value="Go" />  </td>
                              </tr>
                      <% } else if (pageBean.getFormType() == WikiPageListBean.FormType.PagePrefixForm) { %>
                              <tr>
                                <td class='mw-label'><label for="prefix">Display pages with prefix:</label> </td>
                                <td class='mw-input'><input name="prefix" size="30" value="<%= safePrefixTitle %>" id="nsprefix" />  </td>
                              </tr>
                      <% } else if (pageBean.getFormType() == WikiPageListBean.FormType.PageSearchForm) { %>
                              <tr>
                                <td class='mw-label'><label for="search">Display pages containing:</label> </td>
                                <td class='mw-input'><input name="search" size="30" value="<%= safeSearchTitle %>" id="nsprefix" />  </td>
                              </tr>
                      <% } %>
                      <% if (pageBean.getFormType() != WikiPageListBean.FormType.TargetPageForm) { %>
                              <tr>
                                <td class='mw-label'><label for="namespace">Namespace:</label>  </td>
                                <td class='mw-input'>
                                  <select id="namespace" name="namespace" class="namespaceselector">
                      <% for (int i = 0; i <= 15; ++i) {
                          String namespace = (i == 0) ? "Article" : pageBean.getWikiNamespace().getNamespaceByNumber(i);
                          String selected = (pageBean.getNamespaceId() == i) ? " selected=\"selected\"" : "";
                      %>
                                    <option value="<%= i %>"<%= selected %>><%= namespace %></option>
                      <% } %>
                                  </select>
                                  <input type="submit" value="Go" />
                                </td>
                              </tr>
                      <% } %>
                            </table>
                          </fieldset>
                        </form>
                      </div>
                    </td>
<% /*               <td class="mw-allpages-nav"><a href="wiki?title=Special:AllPages<%= andServiceUser >" title="Special:AllPages">All pages</a></td> */ %>
                  </tr>
                </table>
                <% } %>

<% if (pageBean.getFormType() == WikiPageListBean.FormType.PageSearchForm) { %>
  <p class="mw-search-createlink">
  <%
      if (pageBean.isFoundFullMatch()) {
  %>
    <b>There is a page named "<a href="wiki?title=<%= safeSearchTitle %><%= andServiceUser %>" title="<%= safeSearchTitle %>"><%= pageBean.getSearch() %></a>" on this wiki.</b>
  <% } else {%>
    <b>Create the page "<a href="wiki?title=<%= safeSearchTitle %>&amp;action=edit<%= andServiceUser %>" class="new" title="<%= safeSearchTitle %>"><%= pageBean.getSearch() %></a>" on this wiki!</b>
  <% } %>
  </p>
<% } %>
                <table class="mw-allpages-table-chunk">
<% if (!pageBean.getPages().isEmpty()) {
    Iterator<String> iter = pageBean.getPages().iterator();
    while (iter.hasNext()) {
        String value = iter.next();
%>
                  <tr>
                    <td style="width:33%">
                      <a href="wiki?title=<%= value %><%= andServiceUser %>"><%= value %></a>
          <% if (de.zib.scalaris.examples.wikipedia.Options.getInstance().WIKI_USE_BACKLINKS) { %> 
                      <span class="mw-whatlinkshere-tools">(<a href="wiki?title=Special:WhatLinksHere&amp;target=<%= value %><%= andServiceUser %>" title="Special:WhatLinksHere">← links</a>)</span>
          <% } %>
                    </td>
                    <td style="width:33%">
<%      if (iter.hasNext()) {
            value = iter.next();
%>
                      <a href="wiki?title=<%= value %><%= andServiceUser %>"><%= value %></a> 
          <% if (de.zib.scalaris.examples.wikipedia.Options.getInstance().WIKI_USE_BACKLINKS) { %>
                      <span class="mw-whatlinkshere-tools">(<a href="wiki?title=Special:WhatLinksHere&amp;target=<%= value %><%= andServiceUser %>" title="Special:WhatLinksHere">← links</a>)</span>
          <% } %>
<%      } %>
                    </td>
                    <td style="width:33%">
<%      if (iter.hasNext()) {
            value = iter.next();
%>
                      <a href="wiki?title=<%= value %><%= andServiceUser %>"><%= value %></a>
          <% if (de.zib.scalaris.examples.wikipedia.Options.getInstance().WIKI_USE_BACKLINKS) { %> 
                      <span class="mw-whatlinkshere-tools">(<a href="wiki?title=Special:WhatLinksHere&amp;target=<%= value %><%= andServiceUser %>" title="Special:WhatLinksHere">← links</a>)</span>
          <% } %>
<%      } %>
                    </td>
                  </tr>
<%
    }
  }
%>
                </table>
                <hr />
<% /*           <p class="mw-allpages-nav"><a href="wiki?title=Special:AllPages<%= andServiceUser >" title="Special:AllPages">All pages</a></p> */ %>
                <div class="printfooter">
                Retrieved from "<a href="wiki?title=<%= pageTitleWithPars %><%= andServiceUser %>">wiki?title=${ pageBean.title }</a>"</div>
                <!-- /bodytext -->
<% if (req_render == null || !req_render.equals("0")) { %>
                <!-- catlinks -->
                <div id="catlinks" class="catlinks catlinks-allhidden"></div>
                <!-- /catlinks -->
<% } %>
                <div class="visualClear"></div>
            </div>
            <!-- /bodyContent -->
        </div>
        <!-- /content -->
        <!-- header -->
        <div id="mw-head" class="noprint">
            
<!-- 0 -->
<div id="p-personal" class="">
    <h5>Personal tools</h5>
    <ul>
                    <li id="pt-login"><a href="wiki?title=Special:UserLogin&amp;returnto=<%= safePageTitleWithPars %><%= andServiceUser %>" title="You are encouraged to log in; however, it is not mandatory [o]" accesskey="o">Log in / create account</a></li>
    </ul>
</div>

<!-- /0 -->
            <div id="left-navigation">
                
<!-- 0 -->
<div id="p-namespaces" class="vectorTabs">
    <h5>Namespaces</h5>
    <ul>
                    <li id="ca-special" class="selected"><span><a href="wiki?title=<%= pageTitleWithPars %><%= andServiceUser %>" >Special page</a></span></li>
    </ul>
</div>

<!-- /0 -->

<!-- 1 -->
<div id="p-variants" class="vectorMenu emptyPortlet">
        <h5><span>Variants</span><a href="#"></a></h5>
    <div class="menu">
        <ul>
        </ul>
    </div>
</div>

<!-- /1 -->
            </div>
            <div id="right-navigation">
                
<!-- 0 -->
<div id="p-views" class="vectorTabs emptyPortlet">
    <h5>Views</h5>
    <ul>
    </ul>
</div>

<!-- /0 -->

<!-- 1 -->
<div id="p-cactions" class="vectorMenu emptyPortlet">
    <h5><span>Actions</span><a href="#"></a></h5>
    <div class="menu">
        <ul>
        </ul>
    </div>
</div>

<!-- /1 -->

<!-- 2 -->
<div id="p-search">
    <h5><label for="searchInput">Search</label></h5>
    <form action="wiki?" id="searchform">
        <input name="title" value="Special:Search" type="hidden" />
        <div id="simpleSearch">
            <input type="hidden" value="${ pageBean.serviceUser }" name="service_user"/>
            <input autocomplete="off" placeholder="Search" tabindex="1" id="searchInput" name="search" title="Search Wikipedia [f]" accesskey="f" type="text" />
            <button id="searchButton" type="submit" name="button" title="Search the pages for this text"><img src="skins/search-ltr.png" alt="Search" /></button>
        </div>
    </form>
</div>

<!-- /2 -->
            </div>
        </div>
        <!-- /header -->
        <!-- panel -->
            <div id="mw-panel" class="noprint collapsible-nav">
                <!-- logo -->
                    <div id="p-logo"><a style="background-image: url(&quot;images/Wikipedia.png&quot;);" href="wiki?title=Main Page<%= andServiceUser %>" title="Visit the main page"></a></div>
                <!-- /logo -->
                
<!-- navigation -->
<div class="portal first persistent" id="p-navigation">
    <h5>Links</h5>
    <div class="body">
                <ul>
                    <li id="n-mainpage"><a href="wiki?title=Main Page<%= andServiceUser %>" title="Visit the main page [z]" accesskey="z">Main Page</a></li>
                    <li id="n-recentchanges"><a href="wiki?title=Special:RecentChanges<%= andServiceUser %>" title="The list of recent changes in the wiki [r]" accesskey="r">New changes</a></li>
                    <li id="n-randompage"><a href="wiki?title=Special:Random<%= andServiceUser %>" title="Load a random page [x]" accesskey="x">Show any entry</a></li>
                    <li id="n-help"><a href="wiki?title=Help:Contents<%= andServiceUser %>" title="The place to find out">Help</a></li>
                </ul>
            </div>
</div>

<!-- /navigation -->

<!-- SEARCH -->

<!-- /SEARCH -->

<!-- TOOLBOX -->
<div class="portal expanded" id="p-tb">
    <h5 tabindex="2">Toolbox</h5>
    <div style="display: block;" class="body">
        <ul>
                    <li id="t-specialpages"><a href="wiki?title=Special:SpecialPages<%= andServiceUser %>" title="List of all special pages [q]" accesskey="q">Special pages</a></li>
        </ul>
    </div>
</div>

<!-- /TOOLBOX -->

<!-- LANGUAGES -->
<!-- /LANGUAGES -->
            </div>
        <!-- /panel -->
        <!-- footer -->
        <div id="footer">
                <ul id="footer-places">
                    <li id="footer-places-privacy"><a href="wiki?title=<%= pageBean.getWikiNamespace().getMeta() %>:Privacy policy<%= andServiceUser %>" title="<%= pageBean.getWikiNamespace().getMeta() %>:Privacy policy">Privacy policy</a></li>
                    <li id="footer-places-about"><a href="wiki?title=<%= pageBean.getWikiNamespace().getMeta() %>:About<%= andServiceUser %>" title="<%= pageBean.getWikiNamespace().getMeta() %>:About">About <%= pageBean.getWikiNamespace().getMeta() %></a></li>
                    <li id="footer-places-disclaimer"><a href="wiki?title=<%= pageBean.getWikiNamespace().getMeta() %>:General disclaimer<%= andServiceUser %>" title="<%= pageBean.getWikiNamespace().getMeta() %>:General disclaimer">Disclaimers</a></li>
                </ul>
                <ul id="footer-icons" class="noprint">
                </ul>
                <div style="clear:both"></div>
                <div style="font-size:0.7em">DB Timings:
                <pre id="db_timings" style="padding:0;line-height:0.7em">
<% for (Map.Entry<String,List<Long>> stats : pageBean.getStats().entrySet()) { %>
<%= stats.getKey() %>: <%= stats.getValue().toString() %>
<% } %>
                </pre></div>
                <div style="font-size:0.7em">Involved DB keys:
                <pre id="involved_keys" style="padding:0;line-height:0.7em">
<% for (InvolvedKey involvedKey : pageBean.getInvolvedKeys()) { %>
<%= involvedKey.toString() %>
<% } %>
                </pre></div>
<% long renderTime = (System.currentTimeMillis() - pageBean.getStartTime()); %>
                <div style="clear:both"></div>
                <div style="font-size:0.7em">More timings:
                <pre id="other_timings" style="padding:0;line-height:0.7em">

server: <%= renderTime %>

                </pre></div>
        </div>
        <!-- /footer -->
<% servlet.storeUserReq(pageBean, renderTime); %>
</body>
</html>
