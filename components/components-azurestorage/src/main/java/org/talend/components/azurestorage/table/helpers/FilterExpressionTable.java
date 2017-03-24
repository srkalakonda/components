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

import static org.talend.daikon.properties.property.PropertyFactory.newProperty;

import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.reflect.TypeLiteral;
import org.talend.components.api.properties.ComponentPropertiesImpl;
import org.talend.daikon.i18n.GlobalI18N;
import org.talend.daikon.i18n.I18nMessages;
import org.talend.daikon.properties.presentation.Form;
import org.talend.daikon.properties.presentation.Widget;
import org.talend.daikon.properties.property.Property;

import com.microsoft.azure.storage.table.EdmType;
import com.microsoft.azure.storage.table.TableQuery;
import com.microsoft.azure.storage.table.TableQuery.Operators;
import com.microsoft.azure.storage.table.TableQuery.QueryComparisons;

public class FilterExpressionTable extends ComponentPropertiesImpl {

    private static final long serialVersionUID = 7449848799595979627L;

    public static final String ADD_QUOTES = "ADD_QUOTES";

    public static final TypeLiteral<List<String>> LIST_STRING_TYPE = new TypeLiteral<List<String>>() {
    };

    public Property<List<String>> column = newProperty(LIST_STRING_TYPE, "column"); // $NON-NLS-0$

    public Property<List<String>> operand = newProperty(LIST_STRING_TYPE, "operand"); //$NON-NLS-1$

    public Property<List<String>> function = newProperty(LIST_STRING_TYPE, "function");

    public Property<List<String>> predicate = newProperty(LIST_STRING_TYPE, "predicate");

    public Property<List<String>> fieldType = newProperty(LIST_STRING_TYPE, "fieldType");

    private final List<String> schemaColumnNames = new LinkedList<>();

    private static final I18nMessages i18nMessages = GlobalI18N.getI18nMessageProvider()
            .getI18nMessages(FilterExpressionTable.class);

    public FilterExpressionTable(String name) {
        super(name);
        updateColumnsNames();
    }

    public void updateSchemaColumnNames(List<String> columnNames) {
        this.schemaColumnNames.clear();
        if (columnNames != null) {
            this.schemaColumnNames.addAll(columnNames);
            updateColumnsNames();
        }
    }

    private void updateColumnsNames() {
        column.setPossibleValues(schemaColumnNames);
        if (schemaColumnNames.isEmpty()) {
            column.setValue(null);
            function.setValue(null);
            operand.setValue(null);
            predicate.setValue(null);
            fieldType.setValue(null);
        }
    }

    @Override
    public void setupProperties() {
        super.setupProperties();

        operand.setTaggedValue(ADD_QUOTES, true);
        function.setPossibleValues(Comparison.possibleValues());
        predicate.setPossibleValues(Predicate.possibleValues());
        fieldType.setPossibleValues(SupportedFieldType.possibleValues());

        updateColumnsNames();
    }

    @Override
    public void setupLayout() {
        super.setupLayout();

        Form mainForm = new Form(this, Form.MAIN);
        mainForm.addColumn(Widget.widget(column).setWidgetType(Widget.ENUMERATION_WIDGET_TYPE));
        mainForm.addColumn(Widget.widget(function).setWidgetType(Widget.ENUMERATION_WIDGET_TYPE));
        mainForm.addColumn(operand);
        mainForm.addColumn(Widget.widget(predicate).setWidgetType(Widget.ENUMERATION_WIDGET_TYPE));
        mainForm.addColumn(Widget.widget(fieldType).setWidgetType(Widget.ENUMERATION_WIDGET_TYPE));
    }

    public String getComparison(String f) {
        switch (Comparison.parse(f)) {
        case EQUAL:
            return QueryComparisons.EQUAL;
        case NOT_EQUAL:
            return QueryComparisons.NOT_EQUAL;
        case GREATER_THAN:
            return QueryComparisons.GREATER_THAN;
        case GREATER_THAN_OR_EQUAL:
            return QueryComparisons.GREATER_THAN_OR_EQUAL;
        case LESS_THAN:
            return QueryComparisons.LESS_THAN;
        case LESS_THAN_OR_EQUAL:
            return QueryComparisons.LESS_THAN_OR_EQUAL;
        default:
            return null;
        }
    }

    public String getOperator(String p) {

        switch (Predicate.parse(p)) {
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

    public EdmType getType(String ft) {
        switch (SupportedFieldType.parse(ft)) {
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

    private boolean canGenerateFilterExpession() {

        if (column.getValue() == null || fieldType.getValue() == null || function.getValue() == null || operand.getValue() == null
                || predicate.getValue() == null) {

            return false;
        }

        boolean canGenerate = true;
        for (int i = 0; i < column.getValue().size(); i++) {
            canGenerate = canGenerate && StringUtils.isNotEmpty(column.getValue().get(i))
                    && StringUtils.isNotEmpty(fieldType.getValue().get(i)) && StringUtils.isNotEmpty(function.getValue().get(i))
                    && StringUtils.isNotEmpty(operand.getValue().get(i)) && StringUtils.isNotEmpty(predicate.getValue().get(i));

            if (!canGenerate) {
                return false;
            }
        }

        return true;
    }

    public String generateCombinedFilterConditions() {
        String filter = "";
        if (canGenerateFilterExpession()) {
            for (int idx = 0; idx < column.getValue().size(); idx++) {
                String c = column.getValue().get(idx);
                String cfn = function.getValue().get(idx);
                String cop = predicate.getValue().get(idx);
                String typ = fieldType.getValue().get(idx);

                String f = getComparison(cfn);
                String v = operand.getValue().get(idx);
                String p = getOperator(cop);

                EdmType t = getType(typ);

                String flt = TableQuery.generateFilterCondition(c, f, v, t);

                if (!filter.isEmpty()) {
                    filter = TableQuery.combineFilters(filter, p, flt);
                } else {
                    filter = flt;
                }
            }
        }
        return filter;
    }

}
