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

import org.talend.daikon.di.DiSchemaConstants;

/**
 *
 */
public interface NetSuiteSchemaConstants {

    String NS_PREFIX = DiSchemaConstants.TALEND6_ADDITIONAL_PROPERTIES + "netsuite.";

    String NS_CUSTOM_RECORD = NS_PREFIX + "custom.record";
    String NS_CUSTOM_FIELD = NS_PREFIX + "custom.field";

}
