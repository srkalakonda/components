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

import com.microsoft.azure.storage.table.TableQuery.Operators;

public enum Predicate {
    AND("AND"),
    OR("OR"),
    NOT("NOT");

    private String displayName;

    private static Map<String, Predicate> mapPossibleValues = new HashMap<>();

    private static List<String> possibleValues = new ArrayList<>();

    static {
        for (Predicate predicate : values()) {
            mapPossibleValues.put(predicate.displayName, predicate);
            possibleValues.add(predicate.displayName);
        }
    }

    private Predicate(String displayName) {
        this.displayName = displayName;
    }

    public static List<String> possibleValues() {
        return possibleValues;
    }

    private static Predicate parse(String s) {
        if (!mapPossibleValues.containsKey(s)) {
            throw new IllegalArgumentException(String.format("Invalid value %s, it must be %s", s, possibleValues));
        }
        return mapPossibleValues.get(s);
    }

    /**
     * Convert String predicat to Azure Type {@link Operators}
     */
    public static String getOperator(String p) {

        switch (parse(p)) {
        case AND:
            return Operators.AND;
        case OR:
            return Operators.OR;
        case NOT:
            return Operators.NOT;
        default:
            return null;
        }
    }

    @Override
    public String toString() {
        return this.displayName;
    }
}