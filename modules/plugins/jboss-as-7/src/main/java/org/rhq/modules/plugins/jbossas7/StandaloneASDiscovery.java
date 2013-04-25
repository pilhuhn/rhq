/*
 * RHQ Management Platform
 * Copyright (C) 2005-2013 Red Hat, Inc.
 * All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation version 2 of the License.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 675 Mass Ave, Cambridge, MA 02139, USA.
 */
package org.rhq.modules.plugins.jbossas7;

import java.io.File;
import java.util.Arrays;

import org.rhq.core.domain.configuration.Configuration;
import org.rhq.core.pluginapi.inventory.DiscoveredResourceDetails;
import org.rhq.core.pluginapi.inventory.ResourceDiscoveryContext;
import org.rhq.core.system.ProcessInfo;
import org.rhq.modules.plugins.jbossas7.helper.HostConfiguration;
import org.rhq.modules.plugins.jbossas7.helper.HostPort;
import org.rhq.modules.plugins.jbossas7.helper.StandaloneServerPluginConfiguration;

/**
 * Discovery component for "JBossAS7 Standalone Server" Resources.
 *
 * @author Ian Springer
 */
public class StandaloneASDiscovery extends BaseProcessDiscovery {

    private static final String SERVER_BASE_DIR_SYSPROP = "jboss.server.base.dir";
    private static final String SERVER_CONFIG_DIR_SYSPROP = "jboss.server.config.dir";
    private static final String SERVER_LOG_DIR_SYSPROP = "jboss.server.log.dir";

    @Override
    protected AS7Mode getMode() {
        return AS7Mode.STANDALONE;
    }

    @Override
    protected String getBaseDirSystemPropertyName() {
        return SERVER_BASE_DIR_SYSPROP;
    }

    @Override
    protected String getConfigDirSystemPropertyName() {
        return SERVER_CONFIG_DIR_SYSPROP;
    }

    @Override
    protected String getLogDirSystemPropertyName() {
        return SERVER_LOG_DIR_SYSPROP;
    }

    @Override
    protected String getDefaultBaseDirName() {
        return "standalone";
    }

    @Override
    protected String getLogFileName() {
        return "server.log";
    }

    @Override
    protected String buildDefaultResourceName(HostPort hostPort, HostPort managementHostPort,
        JBossProductType productType) {
        return String.format("%s (%s:%d)", productType.SHORT_NAME, managementHostPort.host, managementHostPort.port);
    }

    @Override
    protected String buildDefaultResourceDescription(HostPort hostPort, JBossProductType productType) {
        return String.format("Standalone %s server", productType.FULL_NAME);
    }

    @Override
    protected DiscoveredResourceDetails buildResourceDetails(ResourceDiscoveryContext discoveryContext,
        ProcessInfo process, AS7CommandLine commandLine) throws Exception {

        DiscoveredResourceDetails resourceDetails = super.buildResourceDetails(discoveryContext, process, commandLine);
        Configuration pluginConfig = resourceDetails.getPluginConfiguration();
        StandaloneServerPluginConfiguration standalonePluginConfig = new StandaloneServerPluginConfiguration(
            pluginConfig);

        // Do RHQ Server specific work
        if (isRhqServer(process)) {
            String name = resourceDetails.getResourceName();
            resourceDetails.setResourceName(name + " RHQ Server");
        }

        File hostXmlFile = standalonePluginConfig.getHostConfigFile();
        File deploymentScannerPath = null;

        if (hostXmlFile != null) {
            HostConfiguration hostConfig = loadHostConfiguration(hostXmlFile);
            deploymentScannerPath = hostConfig.getDeploymentScannerPath(commandLine);
            if (deploymentScannerPath != null && !deploymentScannerPath.isAbsolute() && !deploymentScannerPath.exists()) {
                deploymentScannerPath = new File(standalonePluginConfig.getBaseDir().toString(),
                    deploymentScannerPath.toString());
            }
        }

        standalonePluginConfig.setDeploymentScannerPath(deploymentScannerPath);

        return resourceDetails;
    }

    @Override
    protected ProcessInfo getPotentialStartScriptProcess(ProcessInfo process) {
        // If the server was started via standalone.sh/bat, its parent process will be standalone.sh/bat.
        return process.getParentProcess();
    }

    private boolean isRhqServer(ProcessInfo process) {

        // LINUX
        if (File.separatorChar == '/') {
            String prop = process.getEnvironmentVariable("JAVA_OPTS");
            return (null != prop && prop.contains("-Dapp.name=rhq-server"));
        }

        // Windows
        ProcessInfo parentProcess = process.getParentProcess();
        if (null != parentProcess) {
            String commandLine = Arrays.toString(parentProcess.getCommandLine());
            if (null != commandLine && commandLine.contains("rhq-server-wrapper.conf")) {
                return true;
            }
        }

        return false;
    }

}
