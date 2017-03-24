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

package org.talend.components.netsuite;

import static org.talend.components.netsuite.NetSuiteDatasetRuntimeImpl.getNsFieldByName;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.talend.components.netsuite.client.CustomMetaDataSource;
import org.talend.components.netsuite.client.model.BasicMetaData;
import org.talend.components.netsuite.client.model.CustomFieldDesc;
import org.talend.components.netsuite.client.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.client.model.RecordTypeInfo;
import org.talend.daikon.avro.AvroUtils;

/**
 *
 */
public class SchemaCustomMetaDataSource implements CustomMetaDataSource {
    protected BasicMetaData basicMetaData;
    protected CustomMetaDataSource defaultSource;
    protected Schema schema;

    public SchemaCustomMetaDataSource(BasicMetaData basicMetaData,
            CustomMetaDataSource defaultSource, Schema schema) {
        this.basicMetaData = basicMetaData;
        this.defaultSource = defaultSource;
        this.schema = schema;
    }

    @Override
    public Collection<CustomRecordTypeInfo> getCustomRecordTypes() {
        return defaultSource.getCustomRecordTypes();
    }

    @Override
    public Map<String, CustomFieldDesc> getCustomFields(RecordTypeInfo recordTypeInfo) {
        if (!AvroUtils.isIncludeAllFields(schema)) {
            Map<String, CustomFieldDesc> customFieldDescMap = new HashMap<>();
            for (Schema.Field field : schema.getFields()) {
                CustomFieldDesc customFieldDesc = NetSuiteDatasetRuntimeImpl.readCustomField(field);
                if (customFieldDesc != null) {
                    customFieldDescMap.put(customFieldDesc.getName(), customFieldDesc);
                }
            }
            return customFieldDescMap;
        }
        return defaultSource.getCustomFields(recordTypeInfo);
    }

    @Override
    public CustomRecordTypeInfo getCustomRecordType(String typeName) {
        Schema.Field keyField = getNsFieldByName(schema, "internalId");
        if (keyField != null) {
            CustomRecordTypeInfo customRecordTypeInfo =
                    NetSuiteDatasetRuntimeImpl.readCustomRecord(basicMetaData, keyField);
            if (customRecordTypeInfo != null) {
                return customRecordTypeInfo;
            }
        }
        return defaultSource.getCustomRecordType(typeName);
    }
}
