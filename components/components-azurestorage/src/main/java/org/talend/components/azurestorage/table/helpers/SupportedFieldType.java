// ============================================================================
//
// Copyright (C) 2006-2016 Talend Inc. - www.talend.com
//
// This source code is available under agreement available at
// %InstallDIR%\features\org.talend.rcp.branding.%PRODUCTNAME%\%PRODUCTNAME%license.txt
//
// You should have received a copy of the agreement
// along with this program; if not, write to Talend SA
// 9 rue Pages 92150 Suresnes, France
//
// ============================================================================
package org.talend.components.azurestorage.table.helpers;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.storage.table.EdmType;

public enum SupportedFieldType {

    STRING("STRING"),

    NUMERIC("NUMERIC"),

    DATE("DATE"),

    INT64("INT64"),

    BINARY("BINARY"),

    GUID("GUID"),

    BOOLEAN("BOOLEAN");

    private String displayName;

    private static Map<String, SupportedFieldType> mapPossibleValues = new HashMap<>();

    private static List<String> possibleValues = new ArrayList<>();

    static {
        for (SupportedFieldType supportedFieldType : values()) {
            possibleValues.add(supportedFieldType.displayName);
            mapPossibleValues.put(supportedFieldType.displayName, supportedFieldType);
        }
    }

    private SupportedFieldType(String displayName) {
        this.displayName = displayName;
    }

    public static List<String> possibleValues() {
        return possibleValues;
    }

    public static SupportedFieldType parse(String s) {
        if (!mapPossibleValues.containsKey(s)) {
            throw new IllegalArgumentException(String.format("Invalid value %s, it must be %s", s, possibleValues));
        }
        return mapPossibleValues.get(s);
    }

    /**
     * Convert String type names to Azure Type {@link EdmType}
     */
    public static EdmType getEdmType(String ft) {
        switch (parse(ft)) {
        case STRING:
            return EdmType.STRING;
        case NUMERIC:
            return EdmType.INT32;
        case INT64:
            return EdmType.INT64;
        case DATE:
            return EdmType.DATE_TIME;
        case BINARY:
            return EdmType.BINARY;
        case GUID:
            return EdmType.GUID;
        case BOOLEAN:
            return EdmType.BOOLEAN;
        default:
            return null;
        }
    }

    @Override
    public String toString() {
        return this.displayName;
    }
}