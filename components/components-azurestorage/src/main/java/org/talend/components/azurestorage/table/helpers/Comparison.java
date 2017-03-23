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
import java.util.List;

public enum Comparison {

    EQUAL("EQUAL"),

    NOT_EQUAL("NOT EQUAL"),

    GREATER_THAN("GREATER THAN"),

    GREATER_THAN_OR_EQUAL("GREATER THAN OR EQUAL"),

    LESS_THAN("LESS THAN"),

    LESS_THAN_OR_EQUAL("LESS THAN OR EQUAL");

    private String displayName;

    private static List<String> possibleValues = new ArrayList<>();

    private Comparison(String displayName) {
        this.displayName = displayName;
    }

    public static List<String> possibleValues() {
        if (possibleValues.isEmpty()) {
            for (Comparison predicat : values()) {
                possibleValues.add(predicat.displayName);
            }
        }
        return possibleValues;
    }

    public static Comparison parse(String s) {

        if (!possibleValues().contains(s)) {
            throw new IllegalArgumentException(String.format("Invalid value %s, it must be %s", s, possibleValues));
        }

        for (Comparison predicat : values()) {
            if (predicat.displayName.equals(s)) {
                return predicat;
            }
        }

        return null; // can't happen
    }

    @Override
    public String toString() {
        return this.displayName;
    }
}