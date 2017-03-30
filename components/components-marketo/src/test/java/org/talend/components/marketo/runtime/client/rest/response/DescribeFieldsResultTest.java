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
package org.talend.components.marketo.runtime.client.rest.response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.Arrays;

import org.junit.Before;
import org.junit.Test;
import org.talend.components.marketo.runtime.client.rest.type.FieldDescription;

public class DescribeFieldsResultTest {

    DescribeFieldsResult r;

    @Before
    public void setUp() throws Exception {
        r = new DescribeFieldsResult();
        r.setResult(Arrays.asList(new FieldDescription()));
    }

    @Test
    public void testGetResult() throws Exception {
        assertNotNull(r.getResult());
    }

    @Test
    public void testToString() throws Exception {
        String s = "DescribeFieldsResult{requestId='null', success=false, errors=null, "
                + "result=[FieldDescription{id=null, displayName='null', dataType='null', length=null, rest=null, "
                + "soap=null}], moreResult=false}";
        assertEquals(s, r.toString());
    }
}
