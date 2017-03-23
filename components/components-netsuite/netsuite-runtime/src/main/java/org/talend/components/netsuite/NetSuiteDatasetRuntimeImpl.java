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

import static org.talend.components.netsuite.client.model.beans.Beans.toInitialLower;
import static org.talend.components.netsuite.client.model.beans.Beans.toInitialUpper;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.datatype.XMLGregorianCalendar;

import org.apache.avro.Schema;
import org.talend.components.api.exception.ComponentException;
import org.talend.components.netsuite.client.MetaDataSource;
import org.talend.components.netsuite.client.NetSuiteException;
import org.talend.components.netsuite.client.model.CustomFieldDesc;
import org.talend.components.netsuite.client.model.CustomRecordTypeInfo;
import org.talend.components.netsuite.client.model.FieldDesc;
import org.talend.components.netsuite.client.model.RecordTypeInfo;
import org.talend.components.netsuite.client.model.RefType;
import org.talend.components.netsuite.client.model.SearchRecordTypeDesc;
import org.talend.components.netsuite.client.model.TypeDesc;
import org.talend.components.netsuite.client.model.customfield.CustomFieldRefType;
import org.talend.components.netsuite.client.model.search.SearchFieldOperatorName;
import org.talend.components.netsuite.schema.SearchFieldInfo;
import org.talend.components.netsuite.schema.SearchInfo;
import org.talend.daikon.NamedThing;
import org.talend.daikon.SimpleNamedThing;
import org.talend.daikon.avro.AvroUtils;
import org.talend.daikon.avro.SchemaConstants;
import org.talend.daikon.di.DiSchemaConstants;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;

/**
 *
 */
public class NetSuiteDatasetRuntimeImpl implements NetSuiteDatasetRuntime {
    private MetaDataSource metaDataSource;

    public NetSuiteDatasetRuntimeImpl(MetaDataSource metaDataSource) throws NetSuiteException {
        this.metaDataSource = metaDataSource;
    }

    @Override
    public List<NamedThing> getRecordTypes() {
        try {
            Collection<RecordTypeInfo> recordTypeList = metaDataSource.getRecordTypes();

            List<NamedThing> recordTypes = new ArrayList<>(recordTypeList.size());
            for (RecordTypeInfo recordTypeInfo : recordTypeList) {
                recordTypes.add(new SimpleNamedThing(recordTypeInfo.getName(), recordTypeInfo.getDisplayName()));
            }

            // Sort by display name alphabetically
            Collections.sort(recordTypes, new Comparator<NamedThing>() {
                @Override public int compare(NamedThing o1, NamedThing o2) {
                    return o1.getDisplayName().compareTo(o2.getDisplayName());
                }
            });
            return recordTypes;
        } catch (NetSuiteException e) {
            throw new ComponentException(e);
        }
    }

    @Override
    public Schema getSchema(String typeName) {
        try {
            final RecordTypeInfo recordTypeInfo = metaDataSource.getRecordType(typeName);
            final TypeDesc typeDesc = metaDataSource.getTypeInfo(typeName);

            List<FieldDesc> fieldDescList = new ArrayList<>(typeDesc.getFields());
            Collections.sort(fieldDescList, new Comparator<FieldDesc>() {
                @Override public int compare(FieldDesc o1, FieldDesc o2) {
                    int result = Boolean.compare(o1.isKey(), o2.isKey());
                    if (result > 0) {
                        return 1;
                    }
                    if (result == 0) {
                        result = o1.getName().compareTo(o2.getName());
                    }
                    return result;
                }
            });

            Schema schema = inferSchemaForType(typeDesc.getTypeName(), fieldDescList);
            enrichSchemaByCustomMetaData(schema, recordTypeInfo, fieldDescList);

            return schema;
        } catch (NetSuiteException e) {
            throw new ComponentException(e);
        }
    }

    @Override
    public List<NamedThing> getSearchableTypes() {
        try {
            List<NamedThing> searchableTypes = new ArrayList<>(metaDataSource.getSearchableTypes());
            // Sort by display name alphabetically
            Collections.sort(searchableTypes, new Comparator<NamedThing>() {
                @Override public int compare(NamedThing o1, NamedThing o2) {
                    return o1.getDisplayName().compareTo(o2.getDisplayName());
                }
            });
            return searchableTypes;
        } catch (NetSuiteException e) {
            throw new ComponentException(e);
        }
    }

    @Override
    public SearchInfo getSearchInfo(String typeName) {
        try {
            final SearchRecordTypeDesc searchInfo = metaDataSource.getSearchRecordType(typeName);
            final TypeDesc searchRecordInfo = metaDataSource.getBasicMetaData()
                    .getTypeInfo(searchInfo.getSearchBasicClass());

            List<FieldDesc> fieldDescList = searchRecordInfo.getFields();

            List<SearchFieldInfo> fields = new ArrayList<>(fieldDescList.size());
            for (FieldDesc fieldDesc : fieldDescList) {
                SearchFieldInfo field = new SearchFieldInfo(fieldDesc.getName(), fieldDesc.getValueType());
                fields.add(field);
            }
            // Sort by name alphabetically
            Collections.sort(fields, new Comparator<SearchFieldInfo>() {
                @Override public int compare(SearchFieldInfo o1, SearchFieldInfo o2) {
                    return o1.getName().compareTo(o2.getName());
                }
            });

            return new SearchInfo(searchRecordInfo.getTypeName(), fields);

        } catch (NetSuiteException e) {
            throw new ComponentException(e);
        }
    }

    @Override
    public Schema getSchemaForUpdate(String typeName) {
        try {
            final RecordTypeInfo recordTypeInfo = metaDataSource.getRecordType(typeName);
            final TypeDesc typeDesc = metaDataSource.getTypeInfo(typeName);

            Schema schema = NetSuiteDatasetRuntimeImpl.inferSchemaForType(typeDesc.getTypeName(), typeDesc.getFields());
            enrichSchemaByCustomMetaData(schema, recordTypeInfo, typeDesc.getFields());

            return schema;
        } catch (NetSuiteException e) {
            throw new ComponentException(e);
        }
    }

    @Override
    public Schema getSchemaForDelete(String typeName) {
        try {
            final RecordTypeInfo referencedRecordTypeInfo = metaDataSource.getRecordType(typeName);
            final RefType refType = referencedRecordTypeInfo.getRefType();
            final TypeDesc typeDesc = metaDataSource.getTypeInfo(refType.getTypeName());

            Schema schema = NetSuiteDatasetRuntimeImpl.inferSchemaForType(typeDesc.getTypeName(), typeDesc.getFields());
            enrichSchemaByCustomMetaData(schema, referencedRecordTypeInfo, null);

            return schema;
        } catch (NetSuiteException e) {
            throw new ComponentException(e);
        }
    }

    @Override
    public List<String> getSearchFieldOperators() {
        List<SearchFieldOperatorName> operatorList =
                new ArrayList<>(metaDataSource.getBasicMetaData().getSearchOperatorNames());
        List<String> operatorNames = new ArrayList<>(operatorList.size());
        for (SearchFieldOperatorName operatorName : operatorList) {
            operatorNames.add(operatorName.getQualifiedName());
        }
        // Sort by name alphabetically
        Collections.sort(operatorNames);
        return operatorNames;
    }

    /**
     * Infers an Avro schema for the given type. This can be an expensive operation so the schema
     * should be cached where possible. This is always an {@link Schema.Type#RECORD}.
     *
     * @param name name of a record.
     * @return the schema for data given from the object.
     */
    public static Schema inferSchemaForType(String name, List<FieldDesc> fieldDescList) {
        List<Schema.Field> fields = new ArrayList<>();

        for (FieldDesc fieldDesc : fieldDescList) {
            final String fieldName = fieldDesc.getName();
            final String avroFieldName = toInitialUpper(fieldName);

            Schema.Field avroField = new Schema.Field(avroFieldName,
                    inferSchemaForField(fieldDesc), null, (Object) null);

            // Add some Talend6 custom properties to the schema.
            Schema avroFieldSchema = AvroUtils.unwrapIfNullable(avroField.schema());

            avroField.addProp(DiSchemaConstants.TALEND6_COLUMN_ORIGINAL_DB_COLUMN_NAME, fieldDesc.getName());

            if (AvroUtils.isSameType(avroFieldSchema, AvroUtils._string())) {
                if (fieldDesc.getLength() != 0) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_DB_LENGTH, String.valueOf(fieldDesc.getLength()));
                }
            }

            if (fieldDesc instanceof CustomFieldDesc) {
                CustomFieldDesc customFieldInfo = (CustomFieldDesc) fieldDesc;
                CustomFieldRefType customFieldRefType = customFieldInfo.getCustomFieldType();

                avroField.addProp(DiSchemaConstants.TALEND6_COLUMN_SOURCE_TYPE, customFieldRefType.getTypeName());

                if (customFieldRefType == CustomFieldRefType.DATE) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd'T'HH:mm:ss'.000Z'");
                }
            } else {
                Class<?> fieldType = fieldDesc.getValueType();

                avroField.addProp(DiSchemaConstants.TALEND6_COLUMN_SOURCE_TYPE, fieldType.getSimpleName());

                if (fieldType == XMLGregorianCalendar.class) {
                    avroField.addProp(SchemaConstants.TALEND_COLUMN_PATTERN, "yyyy-MM-dd'T'HH:mm:ss'.000Z'");
                }
            }

            if (avroField.defaultVal() != null) {
                avroField.addProp(SchemaConstants.TALEND_COLUMN_DEFAULT, String.valueOf(avroField.defaultVal()));
            }

            if (fieldDesc.isKey()) {
                avroField.addProp(SchemaConstants.TALEND_COLUMN_IS_KEY, Boolean.TRUE.toString());
            }

            fields.add(avroField);
        }

        return Schema.createRecord(name, null, null, false, fields);
    }

    /**
     * Infers an Avro schema for the given FieldDesc. This can be an expensive operation so the schema should be
     * cached where possible. The return type will be the Avro Schema that can contain the fieldDesc data without loss of
     * precision.
     *
     * @param fieldDesc the <code>FieldDesc</code> to analyse.
     * @return the schema for data that the fieldDesc describes.
     */
    public static Schema inferSchemaForField(FieldDesc fieldDesc) {
        Schema base;

        if (fieldDesc instanceof CustomFieldDesc) {
            CustomFieldDesc customFieldInfo = (CustomFieldDesc) fieldDesc;
            CustomFieldRefType customFieldRefType = customFieldInfo.getCustomFieldType();

            if (customFieldRefType == CustomFieldRefType.BOOLEAN) {
                base = AvroUtils._boolean();
            } else if (customFieldRefType == CustomFieldRefType.LONG) {
                base = AvroUtils._long();
            } else if (customFieldRefType == CustomFieldRefType.DOUBLE) {
                base = AvroUtils._double();
            } else if (customFieldRefType == CustomFieldRefType.DATE) {
                base = AvroUtils._logicalTimestamp();
            } else if (customFieldRefType == CustomFieldRefType.STRING) {
                base = AvroUtils._string();
            } else {
                base = AvroUtils._string();
            }

        } else {
            Class<?> fieldType = fieldDesc.getValueType();

            if (fieldType == Boolean.TYPE || fieldType == Boolean.class) {
                base = AvroUtils._boolean();
            } else if (fieldType == Integer.TYPE || fieldType == Integer.class) {
                base = AvroUtils._int();
            } else if (fieldType == Long.TYPE || fieldType == Long.class) {
                base = AvroUtils._long();
            } else if (fieldType == Float.TYPE || fieldType == Float.class) {
                base = AvroUtils._float();
            } else if (fieldType == Double.TYPE || fieldType == Double.class) {
                base = AvroUtils._double();
            } else if (fieldType == XMLGregorianCalendar.class) {
                base = AvroUtils._logicalTimestamp();
            } else if (fieldType == String.class) {
                base = AvroUtils._string();
            } else if (fieldType.isEnum()) {
                base = AvroUtils._string();
            } else {
                base = AvroUtils._string();
            }
        }

        base = fieldDesc.isNullable() ? AvroUtils.wrapAsNullable(base) : base;

        return base;
    }

    public static void enrichSchemaByCustomMetaData(final Schema schema,
            final RecordTypeInfo recordTypeInfo, final Collection<FieldDesc> fieldDescList) {

        if (recordTypeInfo == null) {
            // Not a record
            return;
        }

        if (recordTypeInfo instanceof CustomRecordTypeInfo) {
            CustomRecordTypeInfo customRecordTypeInfo = (CustomRecordTypeInfo) recordTypeInfo;
            Schema.Field idField = getNsFieldByName(schema, "scriptId");
            if (idField != null) {
                JsonNode node = SchemaCustomMetaDataSource.writeCustomRecord(
                        JsonNodeFactory.instance, customRecordTypeInfo);
                idField.addProp(NetSuiteSchemaConstants.NS_CUSTOM_RECORD, node.toString());
            }
        }

        if (fieldDescList != null && !fieldDescList.isEmpty()) {
            Map<String, CustomFieldDesc> customFieldDescMap = getCustomFieldDescMap(fieldDescList);
            if (!customFieldDescMap.isEmpty()) {
                for (Schema.Field field : schema.getFields()) {
                    String nsFieldName = getNsFieldName(field);
                    CustomFieldDesc customFieldDesc = customFieldDescMap.get(nsFieldName);
                    if (customFieldDesc != null) {
                        JsonNode node = SchemaCustomMetaDataSource.writeCustomField(
                                JsonNodeFactory.instance, customFieldDesc);
                        field.addProp(NetSuiteSchemaConstants.NS_CUSTOM_FIELD, node.toString());
                    }
                }
            }
        }
    }

    public static Map<String, CustomFieldDesc> getCustomFieldDescMap(Collection<FieldDesc> fieldDescList) {
        Map<String, CustomFieldDesc> customFieldDescMap = new HashMap<>();
        for (FieldDesc fieldDesc : fieldDescList) {
            if (fieldDesc instanceof CustomFieldDesc) {
                CustomFieldDesc customFieldDesc = fieldDesc.asCustom();
                customFieldDescMap.put(customFieldDesc.getName(), customFieldDesc);
            }
        }
        return customFieldDescMap;
    }

    public static Class<?> getCustomFieldValueClass(CustomFieldDesc fieldDesc) {
        return getCustomFieldValueClass(fieldDesc.getCustomFieldType());
    }

    public static Class<?> getCustomFieldValueClass(CustomFieldRefType customFieldRefType) {
        Class<?> valueClass = null;
        switch (customFieldRefType) {
        case BOOLEAN:
            valueClass = Boolean.TYPE;
            break;
        case STRING:
            valueClass = String.class;
            break;
        case LONG:
            valueClass = Long.class;
            break;
        case DOUBLE:
            valueClass = Double.class;
            break;
        case DATE:
            valueClass = XMLGregorianCalendar.class;
            break;
        case SELECT:
        case MULTI_SELECT:
            valueClass = String.class;
            break;
        }
        return valueClass;
    }

    public static String getNsFieldName(Schema.Field field) {
        String name = field.getProp(DiSchemaConstants.TALEND6_COLUMN_ORIGINAL_DB_COLUMN_NAME);
        return name != null ? toInitialLower(name) : toInitialLower(field.name());
    }

    public static Schema.Field getNsFieldByName(Schema schema, String fieldName) {
        for (Schema.Field field : schema.getFields()) {
            String nsFieldName = getNsFieldName(field);
            if (fieldName.equals(nsFieldName)) {
                return field;
            }
        }
        return null;
    }

}
