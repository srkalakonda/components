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

import java.util.List;

import org.apache.avro.generic.IndexedRecord;
import org.talend.components.marketo.runtime.client.type.MarketoRecordResult;
import org.talend.components.marketo.runtime.client.type.MarketoSyncResult;
import org.talend.components.marketo.tmarketobulkexec.TMarketoBulkExecProperties;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties;
import org.talend.components.marketo.tmarketooutput.TMarketoOutputProperties;

public interface MarketoClientServiceExtended {

    // Custom Objects
    MarketoRecordResult describeCustomObject(TMarketoInputProperties parameters);

    MarketoRecordResult listCustomObjects(TMarketoInputProperties parameters);

    MarketoRecordResult getCustomObjects(TMarketoInputProperties parameters, String offset);

    MarketoSyncResult syncCustomObjects(TMarketoOutputProperties parameters, List<IndexedRecord> record);

    MarketoSyncResult deleteCustomObjects(TMarketoOutputProperties parameters, List<IndexedRecord> record);

    // Bulk imports
    MarketoRecordResult bulkImport(TMarketoBulkExecProperties parameters);

}
