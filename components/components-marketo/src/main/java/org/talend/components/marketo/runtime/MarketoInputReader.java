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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.IndexedRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.talend.components.api.component.runtime.AbstractBoundedReader;
import org.talend.components.api.component.runtime.Result;
import org.talend.components.api.container.RuntimeContainer;
import org.talend.components.marketo.runtime.client.MarketoClientService;
import org.talend.components.marketo.runtime.client.MarketoRESTClient;
import org.talend.components.marketo.runtime.client.type.MarketoRecordResult;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties;
import org.talend.components.marketo.tmarketoinput.TMarketoInputProperties.InputOperation;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;

public class MarketoInputReader extends AbstractBoundedReader<IndexedRecord> {

    private MarketoSource source;

    private TMarketoInputProperties properties;

    private int apiCalls = 0;

    private String errorMessage;

    private MarketoClientService client;

    private MarketoRecordResult mktoResult;

    private List<IndexedRecord> records;

    private int recordIndex;

    private Boolean isDynamic = Boolean.FALSE;

    private static final Logger LOG = LoggerFactory.getLogger(MarketoInputReader.class);

    private static final I18nMessages messages = GlobalI18N.getI18nMessageProvider().getI18nMessages(MarketoInputReader.class);

    public MarketoInputReader(RuntimeContainer adaptor, MarketoSource source, TMarketoInputProperties properties) {
        super(source);
        this.source = source;
        this.properties = properties;
        // check if we've a dynamic schema...
        isDynamic = AvroUtils.isIncludeAllFields(this.properties.schemaInput.schema.getValue());
    }

    public void adaptSchemaToDynamic() throws IOException {
        Schema design = this.properties.schemaInput.schema.getValue();
        if (!isDynamic) {
            return;
        }
        try {
            Schema runtimeSchema;
            if (!properties.inputOperation.getValue().equals(InputOperation.CustomObject)) {
                runtimeSchema = source.getDynamicSchema("", design);
                // preserve mappings to re-apply them after
                Map<String, String> mappings = properties.mappingInput.getNameMappingsForMarketo();
                List<String> columnNames = new ArrayList<>();
                List<String> mktoNames = new ArrayList<>();
                for (Field f : runtimeSchema.getFields()) {
                    columnNames.add(f.name());
                    if (mappings.get(f.name()) != null) {
                        mktoNames.add(mappings.get(f.name()));
                    } else {
                        mktoNames.add("");
                    }
                }
                properties.mappingInput.columnName.setValue(columnNames);
                properties.mappingInput.marketoColumnName.setValue(mktoNames);
            } else {
                runtimeSchema = source.getDynamicSchema(properties.customObjectName.getValue(), design);
            }
            properties.schemaInput.schema.setValue(runtimeSchema);
        } catch (IOException e) {
            LOG.error(e.getMessage());
            throw e;
        }

    }

    public MarketoRecordResult executeOperation(String position) throws IOException {
        switch (properties.inputOperation.getValue()) {
        case getLead:
            if (isDynamic) {
                adaptSchemaToDynamic();
            }
            return client.getLead(properties, position);
        case getMultipleLeads:
            if (isDynamic) {
                adaptSchemaToDynamic();
            }
            return client.getMultipleLeads(properties, position);
        case getLeadActivity:
            return client.getLeadActivity(properties, position);
        case getLeadChanges:
            return client.getLeadChanges(properties, position);
        case CustomObject:
            switch (properties.customObjectAction.getValue()) {
            case describe:
                return ((MarketoRESTClient) client).describeCustomObject(properties);
            case list:
                return ((MarketoRESTClient) client).listCustomObjects(properties);
            case get:
                if (isDynamic) {
                    adaptSchemaToDynamic();
                }
                return ((MarketoRESTClient) client).getCustomObjects(properties, position);
            }
        }
        throw new IOException(messages.getMessage("error.reader.invalid.operation"));
    }

    @Override
    public boolean start() throws IOException {
        Boolean startable;
        client = source.getClientService(null);
        mktoResult = executeOperation(null);
        startable = mktoResult.getRecordCount() > 0;
        apiCalls++;
        if (startable) {
            records = mktoResult.getRecords();
            recordIndex = 0;
        }
        return startable;
    }

    @Override
    public boolean advance() throws IOException {
        recordIndex++;
        if (recordIndex < records.size()) {
            return true;
        }
        if (mktoResult.getRemainCount() == 0) {
            return false;
        }
        // fetch more data
        mktoResult = executeOperation(mktoResult.getStreamPosition());
        boolean advanceable = mktoResult.getRecordCount() > 0;
        apiCalls++;
        if (advanceable) {
            records = mktoResult.getRecords();
            recordIndex = 0;
        }
        return advanceable;
    }

    @Override
    public IndexedRecord getCurrent() throws NoSuchElementException {
        return records.get(recordIndex);
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
