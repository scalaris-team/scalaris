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

import java.lang.management.ManagementFactory;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;
import javax.servlet.ServletConfig;

import de.zib.scalaris.examples.wikipedia.WikiServletContext;
import de.zib.scalaris.examples.wikipedia.plugin.WikiPlugin;

/**
 * Registers the 4CaaSt monitoring plugin.
 *
 * @author Nico Kruber, kruber@zib.de
 */
public class FourCaastMonitoringPlugin implements WikiPlugin {

    /* (non-Javadoc)
     * @see de.zib.scalaris.examples.wikipedia.plugin.WikiPlugin#init(de.zib.scalaris.examples.wikipedia.WikiServletContext, javax.servlet.ServletConfig)
     */
    @Override
    public void init(WikiServletContext servlet, ServletConfig config) {
        System.out.println("Activating 4CaaSt monitoring plugin...");
        final FourCaastMonitoring monitor = new FourCaastMonitoring(servlet);
        servlet.registerEventHandler(monitor);
        try {
            final MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
            final ObjectName nodeMonitorName = new ObjectName("de.zib.scalaris:type=FourCaastMonitoring");
            final FourCaastMonitoring nodeMonitorMbean = monitor;
            mbs.registerMBean(nodeMonitorMbean, nodeMonitorName);
        } catch (final MalformedObjectNameException e) {
            throw new RuntimeException(e);
        } catch (final InstanceAlreadyExistsException e) {
            throw new RuntimeException(e);
        } catch (final MBeanRegistrationException e) {
            throw new RuntimeException(e);
        } catch (final NotCompliantMBeanException e) {
            throw new RuntimeException(e);
        }
    }
}
