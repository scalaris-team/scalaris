/**
 *  Copyright 2012-2013 Zuse Institute Berlin
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
package de.zib.scalaris.examples.wikipedia.plugin.fourcaast;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.Map.Entry;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang.StringEscapeUtils;

import de.zib.scalaris.examples.wikipedia.SavePageResult;
import de.zib.scalaris.examples.wikipedia.ValueResult;
import de.zib.scalaris.examples.wikipedia.WikiServletContext;
import de.zib.scalaris.examples.wikipedia.bliki.NormalisedTitle;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageBean;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageBeanBase;
import de.zib.scalaris.examples.wikipedia.bliki.WikiPageEditBean;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.plugin.WikiEventHandler;

/**
 * Monitors page views and aggregates certain attributes.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class FourCaastMonitoring implements WikiEventHandler, FourCaastMonitoringMBean {
    protected final WikiServletContext servlet;
    final static protected Record NULL_RECORD = new Record("", "", "", 0l, 0l, 0l);
    protected Record lastRecord = NULL_RECORD; 

    /**
     * Creates the plugin and stores the wiki servlet context.
     * 
     * @param servlet
     *            servlet context
     */
    public FourCaastMonitoring(WikiServletContext servlet) {
        this.servlet = servlet;
        resetMonitoringStats();
    }

    @Override
    public <Connection> void onPageSaved(WikiPageEditBean page,
            SavePageResult result, Connection connection) {
        // if unsuccessful, #onPageEditPreview is called, too
        if (result.success) {
            extractMonitoringStats(page);
        }
    }

    @Override
    public <Connection> void onPageView(WikiPageBeanBase page,
            Connection connection) {
        extractMonitoringStats(page);
    }

    @Override
    public void onImageRedirect(String image, String realImageUrl) {
    }

    @Override
    public <Connection> void onViewRandomPage(WikiPageBean page,
            ValueResult<NormalisedTitle> result, Connection connection) {
        extractMonitoringStats(page);
    }

    @Override
    public <Connection> boolean checkAccess(String serviceUser,
            HttpServletRequest request, Connection connection) {
        return true;
    }
    
    protected synchronized void extractMonitoringStats(final WikiPageBeanBase page) {
        final Calendar now = GregorianCalendar.getInstance();
        long dbTime = 0l;
        for (Entry<String, List<Long>> stats : page.getStats().entrySet()) {
            String statName = stats.getKey();
            for (Long stat : stats.getValue()) {
                if (statName.endsWith(" (last op)")) {
                    // this is from a previous operation that lead to the
                    // current page view, e.g. during random page view
                    // -> exclude it here (there has already been a monitoring call for it)
                } else {
                    dbTime += stat;
                }
            }
        }
        final long serverTime = System.currentTimeMillis() - page.getStartTime();
        final long renderTime = serverTime - dbTime;
        lastRecord = new Record(page.getServiceUser(), Revision.calendarToString(now),
                StringEscapeUtils.escapeJava(page.getTitle()),
                serverTime, dbTime, renderTime);
    }
    
    @Override
    public
    synchronized void resetMonitoringStats() {
        lastRecord = NULL_RECORD;
    }

    @Override
    public String getName() {
        return "4CaaSt monitoring";
    }
    
    @Override
    public String getURL() {
        return "https://code.google.com/p/scalaris/";
    }

    @Override
    public String getVersion() {
        return "0.1";
    }

    @Override
    public String getDescription() {
        return "Exposes several timing values via JMX.";
    }

    @Override
    public String getAuthor() {
        return "Nico Kruber";
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.plugin.fourcaast.FourCaastMonitoringMBean#getServletVersion()
     */
    @Override
    public String getServletVersion() {
        return servlet.getVersion();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.plugin.fourcaast.FourCaastMonitoringMBean#getDbVersion()
     */
    @Override
    public String getDbVersion() {
        return servlet.getDbVersion();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.plugin.fourcaast.FourCaastMonitoringMBean#getServerVersion()
     */
    @Override
    public String getServerVersion() {
        return servlet.getServerVersion();
    }

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.plugin.fourcaast.FourCaastMonitoringMBean#getBlikiVersion()
     */
    @Override
    public String getBlikiVersion() {
        return servlet.getBlikiVersion();
    }

    /**
     * @return the lastServiceUser
     */
    @Override
    public String getLastServiceUser() {
        return lastRecord.serviceUser;
    }

    /**
     * @return the lastTimestamp
     */
    @Override
    public String getLastTimestamp() {
        return lastRecord.timestamp;
    }

    /**
     * @return the lastTitle
     */
    @Override
    public String getLastTitle() {
        return lastRecord.title;
    }

    /**
     * @return the lastDbTime
     */
    @Override
    public long getLastDbTime() {
        return lastRecord.dbTime;
    }

    /**
     * @return the lastServerTime
     */
    @Override
    public long getLastServerTime() {
        return lastRecord.serverTime;
    }

    /**
     * @return the lastRenderTime
     */
    @Override
    public long getLastRenderTime() {
        return lastRecord.renderTime;
    }
}
