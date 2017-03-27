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
package org.talend.components.marketo.runtime.client.rest.type;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

public class BulkImportTest {

    BulkImport bi;

    @Before
    public void setUp() throws Exception {
        bi = new BulkImport();
    }

    @Test
    public void testSettersAndGetters() throws Exception {
        assertNull(bi.getBatchId());
        assertNull(bi.getMessage());
        assertNull(bi.getOperation());
        assertNull(bi.getImportId());
        assertNull(bi.getImportTime());
        assertNull(bi.getObjectApiName());
        assertNull(bi.getNumOfLeadsProcessed());
        assertNull(bi.getNumOfObjectsProcessed());
        assertNull(bi.getNumOfRowsFailed());
        assertNull(bi.getNumOfRowsWithWarning());
        assertNull(bi.getStatus());
        //
        assertNull(bi.getNumOfRowsProcessed());
        bi.setNumOfLeadsProcessed(2);
        assertEquals(Integer.valueOf(2), bi.getNumOfRowsProcessed());
        bi.setNumOfLeadsProcessed(null);
        bi.setNumOfObjectsProcessed(4);
        assertEquals(Integer.valueOf(4), bi.getNumOfRowsProcessed());
        //
        bi.setBatchId(1000);
        bi.setMessage("message");
        bi.setOperation("operation");
        bi.setImportId("importid");
        bi.setImportTime("importime");
        bi.setObjectApiName("objectapiname");
        bi.setNumOfLeadsProcessed(100);
        bi.setNumOfObjectsProcessed(101);
        bi.setNumOfRowsFailed(102);
        bi.setNumOfRowsWithWarning(103);
        bi.setStatus("status");
        assertEquals("1000", bi.getBatchId().toString());
        assertEquals("message", bi.getMessage());
        assertEquals("operation", bi.getOperation());
        assertEquals("importid", bi.getImportId());
        assertEquals("importime", bi.getImportTime());
        assertEquals("objectapiname", bi.getObjectApiName());
        assertEquals("100", bi.getNumOfLeadsProcessed().toString());
        assertEquals("101", bi.getNumOfObjectsProcessed().toString());
        assertEquals("102", bi.getNumOfRowsFailed().toString());
        assertEquals("103", bi.getNumOfRowsWithWarning().toString());
        assertEquals("status", bi.getStatus());
    }

    @Test
    public void testToString() throws Exception {
        String s1 = "BulkImport{batchId=null, importTime='null', importId='null', message='null', "
                + "numOfRowsProcessed=null, numOfLeadsProcessed=null, numOfObjectsProcessed=null, "
                + "numOfRowsFailed=null, numOfRowsWithWarning=null, objectApiName='null', operation='null', status='null'}";
        String s2 = "BulkImport{batchId=1000, importTime='importime', importId='importid', message='message', "
                + "numOfRowsProcessed=100, numOfLeadsProcessed=100, numOfObjectsProcessed=101, numOfRowsFailed=102, "
                + "numOfRowsWithWarning=103, objectApiName='objectapiname', operation='operation', status='status'}";
        assertEquals(s1, bi.toString());
        bi.setBatchId(1000);
        bi.setMessage("message");
        bi.setOperation("operation");
        bi.setImportId("importid");
        bi.setImportTime("importime");
        bi.setObjectApiName("objectapiname");
        bi.setNumOfLeadsProcessed(100);
        bi.setNumOfObjectsProcessed(101);
        bi.setNumOfRowsFailed(102);
        bi.setNumOfRowsWithWarning(103);
        bi.setStatus("status");
        assertEquals(s2, bi.toString());

    }
}
