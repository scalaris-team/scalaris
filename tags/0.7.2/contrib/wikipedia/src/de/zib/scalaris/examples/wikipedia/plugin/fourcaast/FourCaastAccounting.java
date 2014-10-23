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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.ProtocolException;
import java.net.URL;
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
import de.zib.scalaris.examples.wikipedia.bliki.WikiServlet;
import de.zib.scalaris.examples.wikipedia.data.Revision;
import de.zib.scalaris.examples.wikipedia.plugin.WikiEventHandler;

/**
 * Registers each page view with the accounting server.
 * 
 * @author Nico Kruber, kruber@zib.de
 */
public class FourCaastAccounting implements WikiEventHandler {
    protected final WikiServletContext servlet;
    protected final String meteringInstructionId = "de.zib.scalaris.examples.wikipedia.pages_viewed";
    protected final String eventName = "Pages viewed";
    protected final URL accountingServer;

    /**
     * Creates the plugin and stores the wiki servlet context.
     * 
     * @param servlet
     *            servlet context
     * @param accountingServer
     *            the URL pointing to the accounting server
     */
    public FourCaastAccounting(WikiServletContext servlet, URL accountingServer) {
        this.servlet = servlet;
        this.accountingServer = accountingServer;
    }

    @Override
    public <Connection> void onPageSaved(WikiPageEditBean page,
            SavePageResult result, Connection connection) {
        // if unsuccessful, #onPageEditPreview is called, too
        if (result.success) {
            pushSdrToServer(page);
        }
    }

    @Override
    public <Connection> void onPageView(WikiPageBeanBase page,
            Connection connection) {
        pushSdrToServer(page);
    }

    @Override
    public void onImageRedirect(String image, String realImageUrl) {
    }

    @Override
    public <Connection> void onViewRandomPage(WikiPageBean page,
            ValueResult<NormalisedTitle> result, Connection connection) {
        pushSdrToServer(page);
    }

    @Override
    public <Connection> boolean checkAccess(String serviceUser,
            HttpServletRequest request, Connection connection) {
        // TODO check in the data store?
        if (serviceUser.isEmpty()) {
            WikiServlet.setParam_error(request, "Access forbidden for user \""
                    + serviceUser + "\"");
            return false;
        }
        return true;
    }
    
    protected String createSdr(final WikiPageBeanBase page) {
        final Calendar now = GregorianCalendar.getInstance();
        final long serverTime = System.currentTimeMillis() - page.getStartTime();
        long dbTime = 0l;
        for (Entry<String, List<Long>> stats : page.getStats().entrySet()) {
            String statName = stats.getKey();
            for (Long stat : stats.getValue()) {
                if (statName.endsWith(" (last op)")) {
                    // this is from a previous operation that lead to the
                    // current page view, e.g. during random page view
                    // -> exclude it here (it has already been accounted for)
                } else {
                    dbTime += stat;
                }
            }
        }
        return "{" + 
                "\"TenantID\": \"" + StringEscapeUtils.escapeJava(page.getServiceUser()) + "\"," +
                "\"MeteringInstructionId\": \"" + meteringInstructionId + "\"," +
                "\"Timestamp\": \"" + Revision.calendarToString(now) + "\"," +
                "\"RecordType\": \"NamedEvent\"," +
                "\"EventName\": \"" + eventName + "\"," +
                "\"Info\": {" +
                "\"Title\": \"" + StringEscapeUtils.escapeJava(page.getTitle()) + "\"," +
                "\"DBTime\": \"" + dbTime + "\"," +
                "\"ServerTime\": \"" + serverTime + "\"" +
                "}"+
                "}";
    }
    
    protected void pushSdrToServer(final WikiPageBeanBase page) {
        if (!page.getServiceUser().isEmpty()) {
            final String sdr = createSdr(page);
//            System.out.println("Sending sdr...\n" + sdr);
            try {
                HttpURLConnection urlConn = (HttpURLConnection) accountingServer
                        .openConnection();
                urlConn.setDoOutput(true);
                urlConn.setRequestMethod("PUT");
                OutputStreamWriter out = new OutputStreamWriter(
                        urlConn.getOutputStream());
                out.write(sdr);
                out.close();
                
                //read the result from the server (necessary for the request to be send!)
                BufferedReader in = new BufferedReader(new InputStreamReader(
                        urlConn.getInputStream()));
                StringBuilder sb = new StringBuilder();
                String line;
                while ((line = in.readLine()) != null) {
                    sb.append(line + '\n');
                }
//                System.out.println(sb.toString());
                in.close();
            } catch (ProtocolException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    @Override
    public String getName() {
        return "4CaaSt accounting";
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
        return "Registers page-view events with an accounting server.";
    }

    @Override
    public String getAuthor() {
        return "Nico Kruber";
    }

}
