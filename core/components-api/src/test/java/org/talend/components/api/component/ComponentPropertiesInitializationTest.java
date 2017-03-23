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
package org.talend.components.api.component;

import org.databene.contiperf.PerfTest;
import org.databene.contiperf.Required;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.talend.components.api.test.junit.ContiPerfRuleAdaptor;
import org.talend.components.api.testcomponent.TestComponentDefinition;
import org.talend.components.api.testcomponent.TestComponentProperties;

/**
 * created by dmytro.chmyga on Mar 23, 2017
 */
public class ComponentPropertiesInitializationTest {

    @Rule
    public ExpectedException exceptionRule = ExpectedException.none();

    @Rule
    public ContiPerfRuleAdaptor perfAdaptor = new ContiPerfRuleAdaptor();

    @Test
    @PerfTest(invocations = 5, threads = 1)
    @Required(max = 200, average = 100)
    public void test() {
        TestComponentDefinition cd = new TestComponentDefinition();

        TestComponentProperties prop = (TestComponentProperties) cd.createRuntimeProperties();
        prop.init();
    }

}
