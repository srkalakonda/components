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

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.talend.components.netsuite.client.CustomMetaDataSource;
import org.talend.components.netsuite.client.NetSuiteException;
import org.talend.components.netsuite.client.NsRef;
import org.talend.components.netsuite.client.model.BasicMetaData;
import org.talend.components.netsuite.client.model.CustomFieldDesc;
import org.talend.components.netsuite.client.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.client.model.RecordTypeDesc;
import org.talend.components.netsuite.client.model.RecordTypeInfo;
import org.talend.components.netsuite.client.model.RefType;
import org.talend.components.netsuite.client.model.customfield.CustomFieldRefType;
import org.talend.daikon.avro.AvroUtils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 *
 */
public class SchemaCustomMetaDataSource implements CustomMetaDataSource {
    protected static final ObjectMapper objectMapper = new ObjectMapper();

    protected BasicMetaData basicMetaData;
    protected CustomMetaDataSource defaultSource;
    protected Schema schema;

    public SchemaCustomMetaDataSource(BasicMetaData basicMetaData,
            CustomMetaDataSource defaultSource, Schema schema) {
        this.basicMetaData = basicMetaData;
        this.defaultSource = defaultSource;
        this.schema = schema;
    }

    public static JsonNode writeCustomRecord(JsonNodeFactory nodeFactory, CustomRecordTypeInfo recordTypeInfo) {
        ObjectNode node = nodeFactory.objectNode();

        NsRef ref = recordTypeInfo.getRef();
        RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();

        node.set("scriptId", nodeFactory.textNode(ref.getScriptId()));
        node.set("internalId", nodeFactory.textNode(ref.getInternalId()));
        node.set("customizationType", nodeFactory.textNode(ref.getType()));
        node.set("recordType", nodeFactory.textNode(recordTypeDesc.getType()));

        return node;
    }

    public CustomRecordTypeInfo readCustomRecord(JsonNode node) {
        String scriptId = node.get("scriptId").asText();
        String internalId = node.get("internalId").asText();
        String customizationType = node.get("customizationType").asText();
        String recordType = node.get("recordType").asText();

        NsRef ref = new NsRef();
        ref.setRefType(RefType.CUSTOMIZATION_REF);
        ref.setScriptId(scriptId);
        ref.setInternalId(internalId);
        ref.setType(customizationType);

        RecordTypeDesc recordTypeDesc = basicMetaData.getRecordType(recordType);
        CustomRecordTypeInfo recordTypeInfo = new CustomRecordTypeInfo(scriptId, recordTypeDesc, ref);

        return recordTypeInfo;
    }

    public static Map<String, CustomFieldDesc> readCustomFields(JsonNode node) {
        Map<String, CustomFieldDesc> customFieldDescMap = new HashMap<>();
        for (int i = 0; i < node.size(); i++) {
            JsonNode fieldNode = node.get(i);
            CustomFieldDesc customFieldDesc = readCustomField(fieldNode);
            customFieldDescMap.put(customFieldDesc.getName(), customFieldDesc);
        }
        return customFieldDescMap;
    }

    public static JsonNode writeCustomFields(JsonNodeFactory nodeFactory,
            Map<String, CustomFieldDesc> customFieldDescMap) {
        ArrayNode fieldListNode = nodeFactory.arrayNode();
        for (CustomFieldDesc customFieldDesc : customFieldDescMap.values()) {
            JsonNode fieldNode = writeCustomField(nodeFactory, customFieldDesc);
            fieldListNode.add(fieldNode);
        }
        return fieldListNode;
    }

    public static JsonNode writeCustomField(JsonNodeFactory nodeFactory, CustomFieldDesc fieldDesc) {
        ObjectNode node = nodeFactory.objectNode();

        NsRef ref = fieldDesc.getRef();
        CustomFieldRefType customFieldRefType = fieldDesc.getCustomFieldType();

        node.set("scriptId", nodeFactory.textNode(ref.getScriptId()));
        node.set("internalId", nodeFactory.textNode(ref.getInternalId()));
        node.set("customizationType", nodeFactory.textNode(ref.getType()));
        node.set("valueType", nodeFactory.textNode(customFieldRefType.name()));

        return node;
    }

    public static CustomFieldDesc readCustomField(JsonNode node) {
        String scriptId = node.get("scriptId").asText();
        String internalId = node.get("internalId").asText();
        String customizationType = node.get("customizationType").asText();
        String type = node.get("valueType").asText();

        NsRef ref = new NsRef();
        ref.setRefType(RefType.CUSTOMIZATION_REF);
        ref.setScriptId(scriptId);
        ref.setInternalId(internalId);
        ref.setType(customizationType);

        CustomFieldRefType customFieldRefType = CustomFieldRefType.valueOf(type);

        CustomFieldDesc fieldDesc = new CustomFieldDesc();
        fieldDesc.setCustomFieldType(customFieldRefType);
        fieldDesc.setRef(ref);
        fieldDesc.setName(scriptId);
        fieldDesc.setValueType(NetSuiteDatasetRuntimeImpl.getCustomFieldValueClass(customFieldRefType));
        fieldDesc.setNullable(true);

        return fieldDesc;
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
                String customFieldJson = field.getProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD);
                if (StringUtils.isNotEmpty(customFieldJson)) {
                    try {
                        JsonNode node = objectMapper.readTree(customFieldJson);
                        CustomFieldDesc customFieldDesc = readCustomField(node);
                        customFieldDescMap.put(customFieldDesc.getName(), customFieldDesc);
                    } catch (IOException e) {
                        throw new NetSuiteException(e.getMessage(), e);
                    }
                }
            }
        }
        return defaultSource.getCustomFields(recordTypeInfo);
    }

    @Override
    public CustomRecordTypeInfo getCustomRecordType(String typeName) {
        Schema.Field idField = getNsFieldByName(schema, "scriptId");
        if (idField != null) {
            String customRecordJson = idField.getProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD);
            if (StringUtils.isEmpty(customRecordJson)) {
                try {
                    JsonNode node = objectMapper.readTree(customRecordJson);
                    CustomRecordTypeInfo customRecordTypeInfo = readCustomRecord(node);
                    return customRecordTypeInfo;
                } catch (IOException e) {
                    throw new NetSuiteException(e.getMessage(), e);
                }
            }
        }
        return defaultSource.getCustomRecordType(typeName);
    }
}
