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
package org.talend.components.marketo.runtime;

import static org.talend.components.api.component.ComponentDefinition.RETURN_ERROR_MESSAGE;
import static org.talend.components.marketo.MarketoComponentDefinition.RETURN_NB_CALL;

import java.io.IOException;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.avro.generic.IndexedRecord;
import org.talend.components.api.component.runtime.AbstractBoundedReader;
import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.marketo.runtime.client.MarketoRESTClient;
import org.talend.components.marketo.runtime.client.type.MarketoRecordResult;
import org.talend.components.marketo.tmarketobulkexec.TMarketoBulkExecProperties;

public class MarketoBulkExecReader extends AbstractBoundedReader<IndexedRecord> {

    protected RuntimeContainer adaptor;

    protected MarketoSource source;

    protected TMarketoBulkExecProperties properties;

    protected MarketoRESTClient client;

    protected MarketoRecordResult mktoResult;

    protected IndexedRecord record;

    protected String errorMessage;

    protected int apiCalls = 0;

    protected MarketoBulkExecReader(RuntimeContainer adaptor, MarketoSource source, TMarketoBulkExecProperties properties) {
        super(source);
        this.adaptor = adaptor;
        this.source = source;
        this.properties = properties;
    }

    @Override
    public boolean start() throws IOException {
        Boolean startable;
        client = (MarketoRESTClient) source.getClientService(null);
        mktoResult = client.bulkImport(properties);
        startable = mktoResult.isSuccess();
        apiCalls++;
        if (startable) {
            record = mktoResult.getRecords().get(0);
        }
        return startable;
    }

    @Override
    public boolean advance() throws IOException {
        // we have only one possible row.
        return false;
    }

    @Override
    public IndexedRecord getCurrent() throws NoSuchElementException {
        return record;
    }

    @Override
    public Map<String, Object> getReturnValues() {
        Result result = new Result();
        result.totalCount = apiCalls;
        Map<String, Object> res = result.toMap();
        res.put(RETURN_NB_CALL, apiCalls);
        res.put(RETURN_ERROR_MESSAGE, errorMessage);
        return res;
    }
}
