//==============================================================================
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
//==============================================================================
package org.talend.components.service.rest.configuration;

import static org.slf4j.LoggerFactory.*;

import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.service.common.DefinitionRegistry;
import org.talend.components.service.spi.ServiceSpiFactory;
import org.talend.daikon.definition.service.DefinitionRegistryService;

/**
 * Configuration that deals with ComponentRegistry setup.
 */
@Configuration
public class ComponentsRegistrySetup {

    /** This class' logger. */
    private static final Logger LOGGER = getLogger(ComponentsRegistrySetup.class);

    @Autowired
    private ApplicationContext context;

    private DefinitionRegistry registry;

    @Bean
    public ComponentService getComponentService() {
        return new ComponentServiceImpl(getComponentRegistry());
    }

    @Bean
    public DefinitionRegistryService getDefintionRegistryService() {
        return getComponentRegistry();
    }

    private DefinitionRegistry getComponentRegistry() {
        // using the spi registry
        return ServiceSpiFactory.getDefinitionRegistry();
    }
}
