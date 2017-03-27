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
package org.talend.components.marketo.runtime.client;

import static org.talend.components.marketo.tmarketoconnection.TMarketoConnectionProperties.APIMode.REST;

import org.junit.Before;
import org.junit.Test;
import org.talend.components.marketo.runtime.MarketoBaseTestIT;
import org.talend.components.marketo.runtime.MarketoSink;
import org.talend.components.marketo.runtime.MarketoSource;
import org.talend.components.marketo.runtime.client.type.MarketoRecordResult;
import org.talend.components.marketo.tmarketobulkexec.TMarketoBulkExecProperties;
import org.talend.components.marketo.tmarketobulkexec.TMarketoBulkExecProperties.BulkImportTo;

public class MarketoRESTClientBulkExecTestIT extends MarketoBaseTestIT {

    TMarketoBulkExecProperties props;

    String coCSV;

    String leadCSV;

    @Before
    public void setUp() throws Exception {
        props = new TMarketoBulkExecProperties("test");
        props.connection.setupProperties();
        props.connection.endpoint.setValue(ENDPOINT_REST);
        props.connection.clientAccessId.setValue(USERID_REST);
        props.connection.secretKey.setValue(SECRETKEY_REST);
        props.connection.apiMode.setValue(REST);
        props.schemaInput.setupProperties();
        props.setupProperties();
        //
        coCSV = getClass().getResource("/customobjects.csv").getPath();
        leadCSV = getClass().getResource("/leads.csv").getPath();
    }

    @Test
    public void testBulkExecCustomObject() throws Exception {
        LOG.debug("coCSV = {}.", coCSV);
        props.bulkImportTo.setValue(BulkImportTo.CustomObjects);
        props.customObjectName.setValue("car_c");
        props.bulkFilePath.setValue(coCSV);
        MarketoSink sink = new MarketoSink();
        sink.initialize(null, props);
        sink.validate(null);

        MarketoSource source = new MarketoSource();
        source.initialize(null, props);
        MarketoRESTClient client = (MarketoRESTClient) source.getClientService(null);
        MarketoRecordResult result = client.bulkImportCustomObjects(props);
        LOG.debug("result = {}.", result);
    }
}
