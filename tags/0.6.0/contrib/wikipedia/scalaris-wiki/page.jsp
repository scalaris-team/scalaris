<?xml version="1.0" encoding="UTF-8" ?>
<%@page import="de.zib.scalaris.examples.wikipedia.bliki.WikiServlet"%>
<%@page import="de.zib.scalaris.examples.wikipedia.InvolvedKey"%>
<%@page import="java.net.URL"%>
<%@page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"
 import="java.util.Calendar,java.util.Locale,java.text.DateFormat,java.text.SimpleDateFormat,java.util.TimeZone,java.util.Iterator,java.util.Map,java.util.List,org.apache.commons.lang.StringUtils,org.apache.commons.lang.StringEscapeUtils,java.net.URLEncoder"%>
<% String req_render = request.getParameter("render"); %>
<jsp:useBean id="pageBean" type="de.zib.scalaris.examples.wikipedia.bliki.WikiPageBean" scope="request" />
<jsp:useBean id="servlet" type="de.zib.scalaris.examples.wikipedia.WikiServletContext" scope="request" />
<% /* created page based on https://secure.wikimedia.org/wiktionary/simple/wiki/relief */ %>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html lang="${ pageBean.wikiLang }" dir="${ pageBean.wikiLangDir }" xmlns="http://www.w3.org/1999/xhtml">
<head>
<% final String safePageTitle = StringEscapeUtils.escapeHtml(URLEncoder.encode(pageBean.getTitle(), "UTF-8")); %>
<% final String andServiceUser = pageBean.getServiceUser().isEmpty() ? "" : "&amp;service_user=" + pageBean.getServiceUser(); %>
<title>${ pageBean.title } - ${ pageBean.wikiTitle }</title>
<!--<% if (!pageBean.getError().isEmpty()) { %>
<error>${ pageBean.error }</error>
<% } %>-->
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8" />
<meta http-equiv="Content-Style-Type" content="text/css" />
<link rel="alternate" type="application/x-wiki" title="change this page" href="wiki?title=<%= safePageTitle %>&amp;action=edit<%= andServiceUser %>">
<link rel="edit" title="change this page" href="wiki?title=<%= safePageTitle %>&amp;action=edit<%= andServiceUser %>">
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
            <h1 id="firstHeading" class="firstHeading">${ pageBean.error }${ pageBean.title }</h1>
            <!-- /firstHeading -->
            <!-- bodyContent -->
            <div id="bodyContent">
                <!-- tagline -->
                <div id="siteSub">From <%= pageBean.getWikiNamespace().getMeta() %></div>
                <!-- /tagline -->
                <!-- subtitle -->
                <div id="contentSub">
                ${ pageBean.contentSub }
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

<% if (!pageBean.getSubCategories().isEmpty()) {
    int rest = pageBean.getSubCategories().size();
%>

<div id="mw-subcategories">
<h2>Subcategories</h2>
<p>This category has the following <%= rest %> subcategories.</p>
<%
int[] columnCount = new int[3];
if (rest > 15) {
    columnCount[0] = rest / 3;
    rest -= columnCount[0];
} else {
    columnCount[0] = rest;
    rest = 0;
}
columnCount[1] = rest / 2;
columnCount[2] = rest - columnCount[1];

Iterator<String> iter = pageBean.getSubCategories().iterator();
%>

<table width="100%"><tbody><tr valign="top">
<% for (int i = 0; i < 3; ++i) { %>
<td>

<h3> </h3>
<ul>
<% for (int j = 0; j < columnCount[i]; ++j) {
    final String subCat = iter.next();
    final String safeFullSubCat = StringEscapeUtils.escapeHtml(pageBean.getWikiNamespace().getCategory() + ":" + subCat);
%>
<li>
  <div class="CategoryTreeSection">
	<div class="CategoryTreeItem">
<% /*
	  <span class="CategoryTreeEmptyBullet">[<b>×</b>] </span>
*/ %>
	  <a class="CategoryTreeLabel  CategoryTreeLabelNs14 CategoryTreeLabelCategory" href="wiki?title=<%=safeFullSubCat%><%= andServiceUser %>"><%=subCat%></a>
<% /* 
	  <span title="contains 0 subcategories, 1745 pages, and 0 files">(0)</span>
*/ %>
	</div>
    <div class="CategoryTreeChildren" style="display: none;"></div>
  </div>
</li>
<% } %>
</ul></td>
<% } %>
</tr>
</table>
</div>
<% } %>

<% if (!pageBean.getCategoryPages().isEmpty()) {
    int rest = pageBean.getCategoryPages().size(); %>

<div id="mw-pages">
<h2>Pages in category "${ pageBean.title }"</h2>
<p>The following <%= rest %> pages are in this category.</p>
<%
int[] columnCount = new int[3];
if (rest > 15) {
    columnCount[0] = rest / 3;
    rest -= columnCount[0];
} else {
    columnCount[0] = rest;
    rest = 0;
}
columnCount[1] = rest / 2;
columnCount[2] = rest - columnCount[1];

Iterator<String> iter = pageBean.getCategoryPages().iterator();
%>

<table width="100%"><tbody><tr valign="top">
<% for (int i = 0; i < 3; ++i) { %>
<td>

<h3> </h3>
<ul>
<% for (int j = 0; j < columnCount[i]; ++j) {
    final String catPage = iter.next();
    final String safeCatPage = StringEscapeUtils.escapeHtml(catPage);
%>
<li><a href="wiki?title=<%=safeCatPage%><%= andServiceUser %>" title="<%=safeCatPage%>"><%=catPage%></a></li>
<% } %>
</ul></td>
<% } %>
</tr>
</table>
</div>
<% } %>

                <!-- /bodytext -->
<% if (req_render == null || !req_render.equals("0")) { %>
                <!-- catlinks -->
<% if (!pageBean.getCategories().isEmpty()) { %>
                <div id="catlinks" class="catlinks">
                <div id="mw-normal-catlinks">
                <a href="wiki?title=Special:Categories<%= andServiceUser %>" title="Special:Categories">Categories</a>:
<%
    for (Iterator<String> iterator = pageBean.getCategories().iterator(); iterator.hasNext();) {
        final String category = iterator.next();
        final String safeCategory = StringEscapeUtils.escapeHtml(category);
        final String safeFullCatName = pageBean.getWikiNamespace().getCategory() + ":" + safeCategory;
        out.print("<span dir=\"" + pageBean.getWikiLangDir() + "\"><a href=\"wiki?title=" + safeFullCatName + andServiceUser + "\" title=\"" + safeFullCatName + "\">" + category + "</a></span>");

        if (iterator.hasNext()) {
            out.print(" | ");
        }
    }
%>
                </div>
                </div>
<% } // if (!pageBean.getCategories().isEmpty()) %>
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
                    <li id="pt-login"><a href="wiki?title=Special:UserLogin&amp;returnto=<%= safePageTitle %><%= andServiceUser %>" title="You are encouraged to log in; however, it is not mandatory [o]" accesskey="o">Log in / create account</a></li>
    </ul>
</div>

<!-- /0 -->
            <div id="left-navigation">
                
<!-- 0 -->
<div id="p-namespaces" class="vectorTabs">
    <h5>Namespaces</h5>
    <ul>
    <%
    final String mainSelected = pageBean.getWikiNamespace().isTalkPage(pageBean.getTitle()) ? "" : " class=\"selected\"";
    final String talkSelected = !pageBean.getWikiNamespace().isTalkPage(pageBean.getTitle()) ? "" : " class=\"selected\"";
    %>
                    <li id="ca-nstab-main"<%= mainSelected %>><span><a href="wiki?title=<%= StringEscapeUtils.escapeHtml(pageBean.getWikiNamespace().getPageNameFromTalkPage(pageBean.getTitle())) %><%= andServiceUser %>" title="View the content page [c]" accesskey="c">Page</a></span></li>
                    <li id="ca-talk"<%= talkSelected %>><span><a href="wiki?title=<%= StringEscapeUtils.escapeHtml(pageBean.getWikiNamespace().getTalkPageFromPageName(pageBean.getTitle())) %><%= andServiceUser %>" title="Discussion about the content page [t]" accesskey="t">Talk</a></span></li>
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
<div id="p-views" class="vectorTabs">
    <h5>Views</h5>
    <ul>
          <% if (!pageBean.isNotAvailable()) { %>
                    <li id="ca-view" class="selected"><span><a href="wiki?title=<%= safePageTitle %><%= andServiceUser %>">Read</a></span></li>
          <% if (!pageBean.isEditRestricted()) { %>
                    <li id="ca-edit"><span><a href="wiki?title=<%= safePageTitle %>&amp;action=edit&amp;oldid=${ pageBean.version }<%= andServiceUser %>" title="You can edit this page. Please use the preview button before saving [e]" accesskey="e">Change</a></span></li>
          <% } else {%>
                    <li id="ca-viewsource"><span><a href="wiki?title=<%= safePageTitle %>&amp;action=edit&amp;oldid=${ pageBean.version }<%= andServiceUser %>" title="This page is protected. You can view its source [e]" accesskey="e">View source</a></span></li>
          <% } %>
                    <li id="ca-history" class="collapsible "><span><a href="wiki?title=<%= safePageTitle %>&amp;action=history<%= andServiceUser %>" title="Past revisions of this page [h]" accesskey="h">View history</a></span></li>
          <% } else {%>
          <% if (!pageBean.isEditRestricted()) { %>
                    <li id="ca-edit"><span><a href="wiki?title=<%= safePageTitle %>&amp;action=edit&amp;oldid=${ pageBean.version }<%= andServiceUser %>" title="You can edit this page. Please use the preview button before saving [e]" accesskey="e">Start</a></span></li>
          <% }%>
          <% } %>
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
            <input autocomplete="off" placeholder="Search" tabindex="1" id="searchInput" name="search" title="Search <%= pageBean.getWikiNamespace().getMeta() %> [f]" accesskey="f" type="text" />
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
          <% if (de.zib.scalaris.examples.wikipedia.Options.getInstance().WIKI_USE_BACKLINKS) { %>
                    <li id="t-whatlinkshere"><a href="wiki?title=Special:WhatLinksHere&target=<%= safePageTitle %><%= andServiceUser %>" title="List of all wiki pages that link here [j]" accesskey="j">What links here</a></li>
          <% } %>
          <% if (!pageBean.isNotAvailable()) { %>
<% /*
                    <li id="t-recentchangeslinked"><a href="wiki?title=Special:RecentChangesLinked&target=safePageTitle<%= andServiceUser >" title="Recent changes in pages linked from this page [k]" accesskey="k">Related changes</a></li>
*/ %>
          <% } %>
                    <li id="t-specialpages"><a href="wiki?title=Special:SpecialPages<%= andServiceUser %>" title="List of all special pages [q]" accesskey="q">Special pages</a></li>
<% /*
                    <li id="t-print"><a href="wiki?title=safePageTitle&amp;printable=yes<%= andServiceUser >" rel="alternate" title="Printable version of this page [p]" accesskey="p">Page for printing</a></li>
*/ %>
          <% if (!pageBean.isNotAvailable()) { %>
                    <li id="t-permalink"><a href="wiki?title=<%= safePageTitle %>&amp;oldid=${ pageBean.version }<%= andServiceUser %>" title="Permanent link to this revision of the page">Permanent link</a></li>
          <% } %>
<% /*
                    <li><span><a href="javascript:adddefinition()">Add definition</a></span></li>
                    <li id="newimagebutton"><span><a href="javascript:addimage()">Add image</a></span></li>
*/ %>
        </ul>
    </div>
</div>

<!-- /TOOLBOX -->

<!-- LANGUAGES -->
<!-- /LANGUAGES -->
<!-- RENDERER -->
<div class="portal expanded" id="p-renderer">
    <h5 tabindex="2">Renderer</h5>
    <div style="display: block;" class="body">
        <ul>
                    <li id="t-renderer-default">
            <% if (req_render == null || req_render.equals("1")) { %>
                        Default
            <% } else { %>
                        <a href="wiki?title=<%= safePageTitle %>&render=1<%= andServiceUser %>" title="Default renderer (gwtwiki)">Default</a></li>
            <% } %>
                    </li>
                    <li id="t-renderer-none">
            <% if (req_render != null && req_render.equals("0")) { %>
                        Plain
            <% } else { %>
                        <a href="wiki?title=<%= safePageTitle %>&render=0<%= andServiceUser %>" title="No renderer (plain wiki text)">Plain</a></li>
            <% } %>
                    </li>
        </ul>
    </div>
</div>

<!-- /RENDERER -->
            </div>
        <!-- /panel -->
        <!-- footer -->
        <div id="footer">
                <ul id="footer-info">
        <% if (!pageBean.isNotAvailable()) {
          final DateFormat dfm = new SimpleDateFormat("d MMMMM yyyy', at 'HH:mm");
          dfm.setTimeZone(TimeZone.getTimeZone("GMT")); // time presented by Wikipedia is UTC/GMT, not the local time of the user viewing the page
          %>
                    <li id="footer-info-lastmod"> This page was last modified on <%= dfm.format(pageBean.getDate().getTime()) %>.</li>
                    <li id="footer-info-copyright">
                        Text is available under the <a href="http://creativecommons.org/licenses/by-sa/3.0/">Creative Commons Attribution/Share-Alike License</a>;
                        additional terms may apply.
                        See <a href="wiki?title=Terms of Use<%= andServiceUser %>">Terms of Use</a> for details.</li>
        <% } %>
                </ul>
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
                <div style="font-size:0.7em">Page save attempts: <span id="save_attempts">${ pageBean.saveAttempts }</span></div>
                <div style="font-size:0.7em">Failed keys when saving page:
                <pre id="save_failed_keys" style="padding:0;line-height:0.7em">
<% for (Map.Entry<Integer,List<String>> failedKeys : pageBean.getFailedKeys().entrySet()) { %>
<%= failedKeys.getKey() %>: <%= StringUtils.join(failedKeys.getValue(), " # ") %>
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
