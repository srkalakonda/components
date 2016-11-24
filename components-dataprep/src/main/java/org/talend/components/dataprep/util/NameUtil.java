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
package org.talend.components.dataprep.util;

import java.util.Set;

public class NameUtil {

    /**
     * correct the field name and make it valid for AVRO schema
     * for example :
     * input : "CA HT", output "CA_HT"
     * input : "column?!^Name", output "column___Name"
     * input : "P1_Vente_Qté", output "P1_Vente_Qt_"
     * 
     * @param name : the name will be correct
     * @param nameIndex : a index which is used to generate the column name when too much underline in the name
     * @param previousNames : the previous valid names, this is used to make sure that every name is different
     * @return the valid name, if the input name is null or empty, or the previousName is empty, return the input name directly
     */
    public static String correct(String name, int nameIndex, Set<String> previousNames) {
        if (name == null || name.isEmpty() || previousNames == null) {
            return name;
        }

        StringBuilder str = new StringBuilder();
        int underLineCount = 0;
        for (int i = 0; i < name.length(); i++) {
            char c = name.charAt(i);
            if (((c >= 'a') && (c <= 'z')) || ((c >= 'A') && (c <= 'Z')) || ((c >= '0') && (c <= '9') && (i != 0))) {
                str.append(c);
            } else if (c == '_') {
                str.append(c);
                underLineCount++;
            } else {
                str.append('_');
                underLineCount++;
            }
        }

        String result = null;
        if (underLineCount > (name.length() / 2)) {
            result = "Column" + nameIndex;
        } else {
            result = str.toString();
        }

        return getUniqueName(result, previousNames);
    }

    private static String getUniqueName(String name, Set<String> previousNames) {
        boolean allIsDifferent = false;
        int index = 0;
        String currentName = name;
        while (!allIsDifferent) {
            allIsDifferent = true;

            if (previousNames.contains(currentName)) {
                allIsDifferent = false;
            }

            if (!allIsDifferent) {
                currentName = currentName + (++index);
            }
        }
        return currentName;
    }

}
