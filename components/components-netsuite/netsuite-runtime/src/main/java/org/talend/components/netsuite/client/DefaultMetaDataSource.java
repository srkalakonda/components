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

package org.talend.components.netsuite.client;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.talend.components.netsuite.client.model.BasicMetaData;
import org.talend.components.netsuite.client.model.BasicRecordType;
import org.talend.components.netsuite.client.model.CustomFieldDesc;
import org.talend.components.netsuite.client.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.client.model.FieldDesc;
import org.talend.components.netsuite.client.model.RecordTypeDesc;
import org.talend.components.netsuite.client.model.RecordTypeInfo;
import org.talend.components.netsuite.client.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.client.model.TypeDesc;
import org.talend.daikon.NamedThing;
import org.talend.daikon.SimpleNamedThing;

/**
 *
 */
public class DefaultMetaDataSource implements MetaDataSource {
    protected NetSuiteClientService<?> clientService;
    protected boolean customizationEnabled = true;
    protected CustomMetaDataSource customMetaDataSource;

    public DefaultMetaDataSource(NetSuiteClientService<?> clientService) {
        this.clientService = clientService;

        customMetaDataSource = clientService.createDefaultCustomMetaDataSource();
    }

    public NetSuiteClientService<?> getClientService() {
        return clientService;
    }

    @Override
    public boolean isCustomizationEnabled() {
        return customizationEnabled;
    }

    @Override
    public void setCustomizationEnabled(boolean customizationEnabled) {
        this.customizationEnabled = customizationEnabled;
    }

    @Override public BasicMetaData getBasicMetaData() {
        return clientService.getBasicMetaData();
    }

    @Override public CustomMetaDataSource getCustomMetaDataSource() {
        return customMetaDataSource;
    }

    @Override public void setCustomMetaDataSource(CustomMetaDataSource customMetaDataSource) {
        this.customMetaDataSource = customMetaDataSource;
    }

    @Override public Collection<RecordTypeInfo> getRecordTypes() {
        List<RecordTypeInfo> recordTypes = new ArrayList<>();

        Collection<RecordTypeDesc> standardRecordTypes = clientService.getBasicMetaData().getRecordTypes();
        for (RecordTypeDesc recordType : standardRecordTypes) {
            recordTypes.add(new RecordTypeInfo(recordType));
        }

        if (customizationEnabled) {
            recordTypes.addAll(customMetaDataSource.getCustomRecordTypes());
        }

        return recordTypes;
    }

    @Override public Collection<NamedThing> getSearchableTypes() throws NetSuiteException {
        List<NamedThing> searchableTypes = new ArrayList<>(256);

        Collection<RecordTypeInfo> recordTypes = getRecordTypes();

        for (RecordTypeInfo recordTypeInfo : recordTypes) {
            RecordTypeDesc recordTypeDesc = recordTypeInfo.getRecordType();
            if (recordTypeDesc.getSearchRecordType() != null) {
                SearchRecordTypeDesc searchRecordType = clientService.getBasicMetaData().getSearchRecordType(recordTypeDesc);
                if (searchRecordType != null) {
                    searchableTypes.add(new SimpleNamedThing(recordTypeInfo.getName(), recordTypeInfo.getDisplayName()));
                }
            }
        }

        return searchableTypes;
    }

    @Override public TypeDesc getTypeInfo(final Class<?> clazz) {
        return getTypeInfo(clazz.getSimpleName());
    }

    @Override public TypeDesc getTypeInfo(final String typeName) {
        TypeDesc baseTypeDesc;
        String targetTypeName = null;
        Class<?> targetTypeClass;
        List<FieldDesc> baseFieldDescList;

        RecordTypeInfo recordTypeInfo = getRecordType(typeName);
        if (recordTypeInfo != null) {
            if (recordTypeInfo instanceof CustomRecordTypeInfo) {
                CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
                baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(customRecordTypeInfo.getRecordType().getTypeName());
                targetTypeName = customRecordTypeInfo.getName();
            } else {
                baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(typeName);
            }
        } else {
            baseTypeDesc = clientService.getBasicMetaData().getTypeInfo(typeName);
        }

        if (targetTypeName == null) {
            targetTypeName = baseTypeDesc.getTypeName();
        }
        targetTypeClass = baseTypeDesc.getTypeClass();
        baseFieldDescList = baseTypeDesc.getFields();

        List<FieldDesc> resultFieldDescList = new ArrayList<>(baseFieldDescList.size() + 10);

        // Add basic fields except field list containers (custom field list, null field list)
        for (FieldDesc fieldDesc : baseFieldDescList) {
            String fieldName = fieldDesc.getName();
            if (fieldName.equals("customFieldList") || fieldName.equals("nullFieldList")) {
                continue;
            }
            resultFieldDescList.add(fieldDesc);
        }

        if (recordTypeInfo != null) {
            if (customizationEnabled) {
                // Add custom fields
                Map<String, CustomFieldDesc> customFieldMap =
                        customMetaDataSource.getCustomFields(recordTypeInfo);
                for (CustomFieldDesc fieldInfo : customFieldMap.values()) {
                    resultFieldDescList.add(fieldInfo);
                }
            }
        }

        Collections.sort(resultFieldDescList, new Comparator<FieldDesc>() {
            @Override public int compare(FieldDesc o1, FieldDesc o2) {
                int result = Boolean.compare(o1.isKey(), o2.isKey());
                if (result == 0) {
                    result = o1.getName().compareTo(o2.getName());
                }
                return result;
            }
        });

        return new TypeDesc(targetTypeName, targetTypeClass, resultFieldDescList);
    }

    @Override public RecordTypeInfo getRecordType(String typeName) {
        RecordTypeDesc recordType = clientService.getBasicMetaData().getRecordType(typeName);
        if (recordType != null) {
            return new RecordTypeInfo(recordType);
        }
        if (customizationEnabled) {
            return customMetaDataSource.getCustomRecordType(typeName);
        }
        return null;
    }

    @Override public SearchRecordTypeDesc getSearchRecordType(String recordTypeName) {
        SearchRecordTypeDesc searchRecordType = clientService.getBasicMetaData().getSearchRecordType(recordTypeName);
        if (searchRecordType != null) {
            return searchRecordType;
        }
        RecordTypeInfo recordTypeInfo = getRecordType(recordTypeName);
        if (recordTypeInfo != null) {
            return getSearchRecordType(recordTypeInfo.getRecordType());
        }
        return null;
    }

    @Override public SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType) {
        if (recordType.getSearchRecordType() != null) {
            return clientService.getBasicMetaData().getSearchRecordType(recordType.getSearchRecordType());
        }
        if (recordType.getType().equals(BasicRecordType.CUSTOM_RECORD_TYPE.getType())) {
            return clientService.getBasicMetaData().getSearchRecordType(BasicRecordType.CUSTOM_RECORD.getType());
        }
        if (recordType.getType().equals(BasicRecordType.CUSTOM_TRANSACTION_TYPE.getType())) {
            return clientService.getBasicMetaData().getSearchRecordType(BasicRecordType.TRANSACTION.getType());
        }
        return null;
    }

}
