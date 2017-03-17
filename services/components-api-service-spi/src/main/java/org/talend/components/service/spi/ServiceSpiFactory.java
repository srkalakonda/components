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
package org.talend.components.service.spi;

import java.net.URL;
import java.util.ServiceLoader;

import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.ComponentServiceImpl;
import org.talend.components.api.service.common.DefinitionRegistry;

/**
 * Provide a {@link DefinitionRegistry} and {@link ComponentService} based on the jars currently available in the
 * classpath.
 */
public class ServiceSpiFactory {

    /** Singleton for definition registry. This will be reloaded if set to null. */
    private static DefinitionRegistry defReg;

    /** Singleton for the component service. This will be reloaded if set to null. */
    private static ComponentService componentService;

    private static ExtensibleUrlClassLoader extensibleClassLoader;

    /**
     * return a singleton registry of all the definition red from the java ServiceLoader
     */
    public static synchronized DefinitionRegistry getDefinitionRegistry() {
        if (defReg == null) {
            // Create a new instance of the definition registry.
            DefinitionRegistry reg = new DefinitionRegistry();
            for (ComponentInstaller installer : ServiceLoader.load(ComponentInstaller.class)) {
                installer.install(reg);
            }
            reg.lock();
            // Only assign it to the singleton after being locked.
            defReg = reg;
        }
        return defReg;
    }

    /**
     * create the singleton registry adding the classpath to the jvm classpath. if the classpath is null (or empty) then the
     * existing registry is returned, otherwise a new registry is constructed with the new classpath added to the
     * current jvm classpath .
     */
    public static synchronized DefinitionRegistry createDefinitionRegistry(URL[] classpath) {
        if (defReg == null || (classpath != null && classpath.length > 0)) {
            extensibleClassLoader = new ExtensibleUrlClassLoader(ServiceSpiFactory.class.getClassLoader());
            createInternalDefintionRegistry(classpath);
        }
        return defReg;
    }

    /**
     * return a singleton registry adding the classpath to the existing registry classpath if any. if the classpath is
     * null then the existing registry is returned, otherwise a new registry is constructed with the classpath added
     * current registry classpath.
     */
    public static synchronized DefinitionRegistry createUpdatedDefinitionRegistry(URL[] classpath) {
        if (defReg == null || classpath != null) {
            if (extensibleClassLoader == null) {
                extensibleClassLoader = new ExtensibleUrlClassLoader(ServiceSpiFactory.class.getClassLoader());
            } // else use the existing classloader
            createInternalDefintionRegistry(classpath);
        }
        return defReg;
    }

    private static void createInternalDefintionRegistry(URL[] classpath) {
        // add the classpath to the classloader.
        if (classpath != null && classpath.length > 0) {
            for (URL url : classpath) {
                extensibleClassLoader.addURL(url);
            }
        }
        // Create a new instance of the definition registry.
        DefinitionRegistry reg = new DefinitionRegistry();
        for (ComponentInstaller installer : ServiceLoader.load(ComponentInstaller.class, extensibleClassLoader)) {
            installer.install(reg);
        }
        reg.lock();
        // Only assign it to the singleton after being locked.
        defReg = reg;
    }

    public static ComponentService getComponentService() {
        if (componentService == null) {
            componentService = new ComponentServiceImpl(getDefinitionRegistry());
        }
        return componentService;
    }

    static void resetDefinitionRegistryOnlyForTest(){
        defReg = null;
    }

}
