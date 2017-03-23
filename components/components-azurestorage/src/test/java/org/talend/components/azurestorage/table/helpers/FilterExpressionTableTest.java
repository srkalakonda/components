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
package org.talend.components.azurestorage.table.helpers;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.BINARY;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.BOOLEAN;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.DATE;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.GUID;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.INT64;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.NUMERIC;
import static org.talend.components.azurestorage.table.helpers.SupportedFieldType.STRING;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.talend.components.azurestorage.table.AzureStorageTableProperties;
import org.talend.daikon.properties.ValidationResult;

import com.microsoft.azure.storage.table.EdmType;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

public class FilterExpressionTableTest {

    List<String> columns = new ArrayList<>();

    List<String> values = new ArrayList<>();

    List<String> functions = new ArrayList<>();

    List<String> predicates = new ArrayList<>();

    List<String> fieldTypes = new ArrayList<>();

    FilterExpressionTable fet = new FilterExpressionTable("tests");

    public void clearLists() {
        columns.clear();
        values.clear();
        functions.clear();
        predicates.clear();
        fieldTypes.clear();
    }

    public void setTableVals() {
        fet.setupProperties();
        fet.column.setValue(columns);
        fet.function.setValue(functions);
        fet.operand.setValue(values);
        fet.predicate.setValue(predicates);
        fet.fieldType.setValue(fieldTypes);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testGetPossibleValues() {
        fet.setupProperties();
        assertArrayEquals(fet.function.getPossibleValues().toArray(), Comparison.possibleValues().toArray());
        assertArrayEquals(fet.predicate.getPossibleValues().toArray(), Predicate.possibleValues().toArray());
        fet.function.setStoredValue(Arrays.asList("GREATER THAN", "EQUAL", "LESS THAN"));
        assertEquals(
                Arrays.asList(Comparison.GREATER_THAN.toString(), Comparison.EQUAL.toString(), Comparison.LESS_THAN.toString())
                        .toString(),
                fet.function.getValue().toString());

        assertEquals(Arrays.asList(STRING.toString(), NUMERIC.toString(), DATE.toString(), INT64.toString(), BINARY.toString(),
                GUID.toString(), BOOLEAN.toString()), fet.fieldType.getPossibleValues());
    }

    @Test
    public void testFilterExpressionTable() {
        String query;
        clearLists();
        //
        columns.add(AzureStorageTableProperties.TABLE_PARTITION_KEY);
        functions.add(Comparison.EQUAL.toString());
        values.add("12345");
        predicates.add(Predicate.NOT.toString());
        fieldTypes.add(STRING.toString());
        setTableVals();
        query = fet.getCombinedFilterConditions();
        assertEquals(query, "PartitionKey eq '12345'");
        //
        columns.add(AzureStorageTableProperties.TABLE_ROW_KEY);
        functions.add(Comparison.GREATER_THAN.toString());
        values.add("AVID12345");
        predicates.add(Predicate.AND.toString());
        fieldTypes.add(STRING.toString());
        setTableVals();
        query = fet.getCombinedFilterConditions();
        assertEquals(query, "(PartitionKey eq '12345') and (RowKey gt 'AVID12345')");
        //
        columns.add(AzureStorageTableProperties.TABLE_TIMESTAMP);
        functions.add(Comparison.GREATER_THAN_OR_EQUAL.toString());
        values.add("2016-01-01 00:00:00");
        predicates.add(Predicate.OR.toString());
        fieldTypes.add(DATE.toString());
        setTableVals();
        query = fet.getCombinedFilterConditions();
        assertEquals(query,
                "((PartitionKey eq '12345') and (RowKey gt 'AVID12345')) or (Timestamp ge datetime'2016-01-01 00:00:00')");
        //
        columns.add("AnUnknownProperty");
        functions.add(Comparison.LESS_THAN.toString());
        values.add("WEB345");
        predicates.add(Predicate.OR.toString());
        fieldTypes.add(STRING.toString());
        setTableVals();
        query = fet.getCombinedFilterConditions();
        assertEquals(query,
                "(((PartitionKey eq '12345') and (RowKey gt 'AVID12345')) or (Timestamp ge datetime'2016-01-01 00:00:00')) or (AnUnknownProperty lt 'WEB345')");

        // Boolean
        columns.add("ABooleanProperty");
        functions.add(Comparison.EQUAL.toString());
        values.add("true");
        predicates.add(Predicate.AND.toString());
        fieldTypes.add(BOOLEAN.toString());
        setTableVals();
        query = fet.getCombinedFilterConditions();
        assertEquals(
                "((((PartitionKey eq '12345') and (RowKey gt 'AVID12345')) or (Timestamp ge datetime'2016-01-01 00:00:00')) or (AnUnknownProperty lt 'WEB345')) and (ABooleanProperty eq true)",
                query);

    }

    @Test
    public void testValidateFilterExpession() {
        clearLists();
        // empty
        assertEquals(ValidationResult.OK, fet.validateFilterExpession());
        // ok
        columns.add(AzureStorageTableProperties.TABLE_PARTITION_KEY);
        functions.add(Comparison.EQUAL.toString());
        values.add("12345");
        predicates.add(Predicate.NOT.toString());
        setTableVals();
        assertEquals(ValidationResult.OK, fet.validateFilterExpession());
        // missing value
        columns.add(AzureStorageTableProperties.TABLE_ROW_KEY);
        functions.add(Comparison.GREATER_THAN.toString());
        values.add("");
        predicates.add(Predicate.AND.toString());
        setTableVals();
        assertEquals(ValidationResult.Result.ERROR, fet.validateFilterExpession().getStatus());
        // missing column
        columns.add("");
        functions.add(Comparison.GREATER_THAN.toString());
        values.add("123456");
        predicates.add(Predicate.AND.toString());
        setTableVals();
        assertEquals(ValidationResult.Result.ERROR, fet.validateFilterExpession().getStatus());
    }

    @Test
    public void testGetFieldType() {
        assertEquals(EdmType.STRING, fet.getType(STRING.toString()));
        assertEquals(EdmType.INT64, fet.getType(INT64.toString()));
        assertEquals(EdmType.INT32, fet.getType(NUMERIC.toString()));
        assertEquals(EdmType.BINARY, fet.getType(BINARY.toString()));
        assertEquals(EdmType.GUID, fet.getType(GUID.toString()));
        assertEquals(EdmType.DATE_TIME, fet.getType(DATE.toString()));
        assertEquals(EdmType.BOOLEAN, fet.getType(BOOLEAN.toString()));
    }

    @Test
    public void testGetComparison() {
        assertEquals(QueryComparisons.EQUAL, fet.getComparison(Comparison.EQUAL.toString()));
        assertEquals(QueryComparisons.NOT_EQUAL, fet.getComparison(Comparison.NOT_EQUAL.toString()));
        assertEquals(QueryComparisons.GREATER_THAN, fet.getComparison(Comparison.GREATER_THAN.toString()));
        assertEquals(QueryComparisons.GREATER_THAN_OR_EQUAL, fet.getComparison(Comparison.GREATER_THAN_OR_EQUAL.toString()));
        assertEquals(QueryComparisons.LESS_THAN, fet.getComparison(Comparison.LESS_THAN.toString()));
        assertEquals(QueryComparisons.LESS_THAN_OR_EQUAL, fet.getComparison(Comparison.LESS_THAN_OR_EQUAL.toString()));
    }

    @Test
    public void testGetOperator() {
        assertEquals(Operators.AND, fet.getOperator(Predicate.AND.toString()));
        assertEquals(Operators.OR, fet.getOperator(Predicate.OR.toString()));
        assertEquals(Operators.NOT, fet.getOperator(Predicate.NOT.toString()));
    }

}
