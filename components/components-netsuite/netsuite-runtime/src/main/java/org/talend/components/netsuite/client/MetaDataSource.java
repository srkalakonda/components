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

import java.util.Collection;

import org.talend.components.netsuite.client.model.BasicMetaData;
import org.talend.components.netsuite.client.model.RecordTypeDesc;
import org.talend.components.netsuite.client.model.RecordTypeInfo;
import org.talend.components.netsuite.client.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.client.model.TypeDesc;
import org.talend.daikon.NamedThing;

/**
 *
 */
public interface MetaDataSource {

    boolean isCustomizationEnabled();

    void setCustomizationEnabled(boolean customizationEnabled);

    BasicMetaData getBasicMetaData();

    CustomMetaDataSource getCustomMetaDataSource();

    void setCustomMetaDataSource(CustomMetaDataSource customMetaDataSource);

    Collection<RecordTypeInfo> getRecordTypes();

    Collection<NamedThing> getSearchableTypes() throws NetSuiteException;

    TypeDesc getTypeInfo(Class<?> clazz);

    TypeDesc getTypeInfo(String typeName);

    RecordTypeInfo getRecordType(String typeName);

    SearchRecordTypeDesc getSearchRecordType(String recordTypeName);

    SearchRecordTypeDesc getSearchRecordType(RecordTypeDesc recordType);
}
