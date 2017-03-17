package org.talend.components.service.spi;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.ServiceLoader;

import org.junit.Test;
import org.talend.components.api.ComponentFamilyDefinition;
import org.talend.components.api.ComponentInstaller;
import org.talend.components.api.service.ComponentService;
import org.talend.components.api.service.common.DefinitionRegistry;
import org.talend.components.localio.LocalIOFamilyDefinition;
import org.talend.components.localio.fixedflowinput.FixedFlowInputDefinition;
import org.talend.components.simplefileio.SimpleFileIoComponentFamilyDefinition;
import org.talend.daikon.definition.Definition;
import org.talend.daikon.runtime.RuntimeUtil;

/**
 * Unit tests for {@link ServiceSpiFactory}.
 */
public class ServiceSpiFactoryTest {

    private static final class MutableUrlClassLoader extends URLClassLoader {

        private MutableUrlClassLoader(URL[] urls) {
            super(urls);
        }

        @Override
        public void addURL(URL url) {
            super.addURL(url);
        }
    }

    @Test
    public void testGetComponentService() throws Exception {
        ComponentService cs = ServiceSpiFactory.getComponentService();
        assertThat(cs, not(nullValue()));

        DefinitionRegistry defReg = ServiceSpiFactory.getDefinitionRegistry();
        assertThat(cs, not(nullValue()));

        Map<String, ComponentFamilyDefinition> families = defReg.getComponentFamilies();
        assertThat(families, hasEntry(is("LocalIO"), isA((Class) LocalIOFamilyDefinition.class)));
        assertThat(families, hasEntry(is("SimpleFileIo"), isA((Class) SimpleFileIoComponentFamilyDefinition.class)));

        Map<String, Definition> definitions = defReg.getDefinitions();
        assertThat(definitions, hasEntry(is("FixedFlowInput"), isA((Class) FixedFlowInputDefinition.class)));
        assertThat(definitions, hasEntry(is("SimpleFileIoDatastore"), isA((Class) SimpleFileIoComponentFamilyDefinition.class)));
        assertThat(definitions, hasEntry(is("SimpleFileIoDataset"), isA((Class) SimpleFileIoComponentFamilyDefinition.class)));
        assertThat(definitions, hasEntry(is("SimpleFileIoInput"), isA((Class) SimpleFileIoComponentFamilyDefinition.class)));
        assertThat(definitions, hasEntry(is("SimpleFileIoOutput"), isA((Class) SimpleFileIoComponentFamilyDefinition.class)));
    }

    @Test
    public void testDynamicClassLoaderService() throws MalformedURLException {
        // this will check that the java service loader works on a classloader that is mutable
        RuntimeUtil.registerMavenUrlHandler();
        // given
        DefinitionRegistry reg = new DefinitionRegistry();
        MutableUrlClassLoader urlClassLoader = new MutableUrlClassLoader(new URL[0]);
        for (ComponentInstaller installer : ServiceLoader.load(ComponentInstaller.class, urlClassLoader)) {
            installer.install(reg);
        }
        Map<String, Definition> definitions = reg.getDefinitions();
        assertThat(definitions, hasEntry(is("FixedFlowInput"), isA((Class) FixedFlowInputDefinition.class)));
        assertThat(definitions, not(hasKey(is("MultiRuntimeComponent"))));

        // when
        urlClassLoader.addURL(new URL("mvn:org.talend.components/multiple-runtime-comp"));

        // then
        reg = new DefinitionRegistry();
        for (ComponentInstaller installer : ServiceLoader.load(ComponentInstaller.class, urlClassLoader)) {
            installer.install(reg);
        }
        definitions = reg.getDefinitions();
        assertThat(definitions, hasEntry(is("FixedFlowInput"), isA((Class) FixedFlowInputDefinition.class)));
        assertThat(definitions, hasKey(is("MultiRuntimeComponent")));

    }

}