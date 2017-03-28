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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.talend.components.marketo.tmarketoconnection.TMarketoConnectionProperties.APIMode.REST;

import java.io.File;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
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
        props.connection.setupLayout();
        props.schemaInput.setupProperties();
        props.schemaInput.setupLayout();
        props.setupProperties();
        props.setupLayout();
        //
        coCSV = getClass().getResource("/customobjects.csv").getPath();
        leadCSV = getClass().getResource("/leads.csv").getPath();
    }

    @Test
    public void testBulkExecCustomObject() throws Exception {
        props.bulkImportTo.setValue(BulkImportTo.CustomObjects);
        props.customObjectName.setValue("car_c");
        props.bulkFilePath.setValue(coCSV);
        props.logDownloadPath.setValue("/Users/undx/tmp/");
        props.pollWaitTime.setValue(5);
        props.afterBulkImportTo();
        Schema s = props.schemaFlow.schema.getValue();
        MarketoSink sink = new MarketoSink();
        sink.initialize(null, props);
        sink.validate(null);

        MarketoSource source = new MarketoSource();
        source.initialize(null, props);
        MarketoRESTClient client = (MarketoRESTClient) source.getClientService(null);
        MarketoRecordResult result = client.bulkImport(props);
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRecordCount());
        IndexedRecord r = result.getRecords().get(0);
        assertNotNull(r);
        assertEquals(1, r.get(s.getField("numOfObjectsProcessed").pos()));
        assertEquals(2, r.get(s.getField("numOfRowsFailed").pos()));
        assertEquals(0, r.get(s.getField("numOfRowsWithWarning").pos()));
        assertEquals("car_c", r.get(s.getField("objectApiName").pos()));
        assertEquals("import", r.get(s.getField("operation").pos()));
        assertEquals("Complete", r.get(s.getField("status").pos()));
        Object batchId = r.get(s.getField("batchId").pos());
        assertNotNull(batchId);
        String logf = String.format("/Users/undx/tmp/bulk_customobjects_car_c_%d_failures.csv",
                Integer.valueOf(batchId.toString()));
        assertEquals(logf, r.get(s.getField("failuresLogFile").pos()));
        File failuresFile = new File(logf);
        assertTrue(failuresFile.exists());
    }

    @Test
    public void testBulkExecLead() throws Exception {
        props.bulkImportTo.setValue(BulkImportTo.Leads);
        props.bulkFilePath.setValue(leadCSV);
        props.logDownloadPath.setValue("/Users/undx/tmp/");
        props.pollWaitTime.setValue(1);
        props.afterBulkImportTo();
        Schema s = props.schemaFlow.schema.getValue();
        MarketoSink sink = new MarketoSink();
        sink.initialize(null, props);
        sink.validate(null);

        MarketoSource source = new MarketoSource();
        source.initialize(null, props);
        MarketoRESTClient client = (MarketoRESTClient) source.getClientService(null);
        MarketoRecordResult result = client.bulkImport(props);
        assertTrue(result.isSuccess());
        assertEquals(1, result.getRecordCount());
        IndexedRecord r = result.getRecords().get(0);
        assertNotNull(r);
        assertEquals(2, r.get(s.getField("numOfLeadsProcessed").pos()));
        assertEquals(0, r.get(s.getField("numOfRowsFailed").pos()));
        assertEquals(1, r.get(s.getField("numOfRowsWithWarning").pos()));
        assertEquals("Complete", r.get(s.getField("status").pos()));
        Object batchId = r.get(s.getField("batchId").pos());
        assertNotNull(batchId);
        String logf = String.format("/Users/undx/tmp/bulk_leads_%d_warnings.csv", Integer.valueOf(batchId.toString()));
        assertEquals(logf, r.get(s.getField("warningsLogFile").pos()));
        File warningsFile = new File(logf);
        assertTrue(warningsFile.exists());
    }

}
