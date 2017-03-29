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
package org.talend.components.marketo.tmarketoinput;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.EnumSet;

import org.junit.Before;
import org.junit.Test;
import org.talend.components.api.component.ConnectorTopology;
import org.talend.components.api.component.runtime.ExecutionEngine;

public class TMarketoInputDefinitionTest {

    private TMarketoInputDefinition def;

    @Before
    public void setUp() throws Exception {
        def = new TMarketoInputDefinition();
    }

    @Test
    public final void testTMarketoInputDefinition() {
        assertEquals("tMarketoInput", def.getName());
    }

    @Test
    public void testGetPropertyClass() throws Exception {
        assertEquals(TMarketoInputProperties.class, def.getPropertyClass());
    }

    @Test
    public void testGetSupportedConnectorTopologies() throws Exception {
        assertEquals(EnumSet.of(ConnectorTopology.OUTGOING), def.getSupportedConnectorTopologies());
    }

    @Test
    public void testGetRuntimeInfo() throws Exception {
        assertNotNull(def.getRuntimeInfo(ExecutionEngine.DI, null, ConnectorTopology.OUTGOING));
    }

}
