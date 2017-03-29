// ============================================================================
//
// Copyright (C) 2006-2017 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.marketo.tmarketolistoperation;

import java.util.EnumSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;
import org.talend.components.api.properties.ComponentProperties;
import org.talend.components.marketo.MarketoComponentDefinition;
import org.talend.daikon.runtime.RuntimeInfo;

public class TMarketoListOperationDefinition extends MarketoComponentDefinition {

    public static final String COMPONENT_NAME = "tMarketoListOperation";

    private transient static final Logger LOG = LoggerFactory.getLogger(TMarketoListOperationDefinition.class);

    public TMarketoListOperationDefinition() {
        super(COMPONENT_NAME);
    }

    @Override
    public Class<? extends ComponentProperties> getPropertyClass() {
        return TMarketoListOperationProperties.class;
    }

    @Override
    public Set<ConnectorTopology> getSupportedConnectorTopologies() {
        return EnumSet.of(ConnectorTopology.INCOMING, ConnectorTopology.INCOMING_AND_OUTGOING);
    }

    @Override
    public RuntimeInfo getRuntimeInfo(ExecutionEngine engine, ComponentProperties props, ConnectorTopology connectorTopology) {
        assertEngineCompatibility(engine);
        // assertConnectorTopologyCompatibility(connectorTopology);
        return getCommonRuntimeInfo(this.getClass().getClassLoader(), RUNTIME_SINK_CLASS);
    }
}
