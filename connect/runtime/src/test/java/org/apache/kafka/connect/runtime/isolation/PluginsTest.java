/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.connect.runtime.isolation;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map.Entry;

import org.apache.kafka.common.Configurable;
import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.provider.ConfigProvider;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.json.JsonConverter;
import org.apache.kafka.connect.json.JsonConverterConfig;
import org.apache.kafka.connect.rest.ConnectRestExtension;
import org.apache.kafka.connect.rest.ConnectRestExtensionContext;
import org.apache.kafka.connect.runtime.WorkerConfig;
import org.apache.kafka.connect.runtime.isolation.Plugins.ClassLoaderUsage;
import org.apache.kafka.connect.runtime.isolation.TestPlugins.TestPlugin;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.ConverterConfig;
import org.apache.kafka.connect.storage.ConverterType;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.SimpleHeaderConverter;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class PluginsTest {

    private Plugins plugins;
    private Map<String, String> props;
    private AbstractConfig config;
    private TestConverter converter;
    private TestHeaderConverter headerConverter;
    private TestInternalConverter internalConverter;

    @Before
    public void setup() {
        Map<String, String> pluginProps = new HashMap<>();

        // Set up the plugins with some test plugins to test isolation
        pluginProps.put(WorkerConfig.PLUGIN_PATH_CONFIG, String.join(",", TestPlugins.pluginPath()));
        plugins = new Plugins(pluginProps);
        props = new HashMap<>(pluginProps);
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, TestConverter.class.getName());
        props.put("key.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        props.put("value.converter." + JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "true");
        props.put("key.converter.extra.config", "foo1");
        props.put("value.converter.extra.config", "foo2");
        props.put(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, TestHeaderConverter.class.getName());
        props.put("header.converter.extra.config", "baz");

        createConfig();
    }

    protected void createConfig() {
        this.config = new TestableWorkerConfig(props);
    }

    @Test
    public void shouldInstantiateAndConfigureConverters() {
        instantiateAndConfigureConverter(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.CURRENT_CLASSLOADER);
        // Validate extra configs got passed through to overridden converters
        assertEquals("true", converter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
        assertEquals("foo1", converter.configs.get("extra.config"));

        instantiateAndConfigureConverter(WorkerConfig.VALUE_CONVERTER_CLASS_CONFIG, ClassLoaderUsage.PLUGINS);
        // Validate extra configs got passed through to overridden converters
        assertEquals("true", converter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
        assertEquals("foo2", converter.configs.get("extra.config"));
    }

    @Test
    public void shouldInstantiateAndConfigureInternalConverters() {
        instantiateAndConfigureInternalConverter(true, Collections.singletonMap(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG, "false"));
        // Validate schemas.enable is set to false
        assertEquals("false", internalConverter.configs.get(JsonConverterConfig.SCHEMAS_ENABLE_CONFIG));
    }

    @Test
    public void shouldInstantiateAndConfigureExplicitlySetHeaderConverterWithCurrentClassLoader() {
        assertNotNull(props.get(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG));
        HeaderConverter headerConverter = plugins.newHeaderConverter(config,
                                                                     WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
                                                                     ClassLoaderUsage.CURRENT_CLASSLOADER);
        assertNotNull(headerConverter);
        assertTrue(headerConverter instanceof TestHeaderConverter);
        this.headerConverter = (TestHeaderConverter) headerConverter;

        // Validate extra configs got passed through to overridden converters
        assertConverterType(ConverterType.HEADER, this.headerConverter.configs);
        assertEquals("baz", this.headerConverter.configs.get("extra.config"));

        headerConverter = plugins.newHeaderConverter(config,
                                                     WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
                                                     ClassLoaderUsage.PLUGINS);
        assertNotNull(headerConverter);
        assertTrue(headerConverter instanceof TestHeaderConverter);
        this.headerConverter = (TestHeaderConverter) headerConverter;

        // Validate extra configs got passed through to overridden converters
        assertConverterType(ConverterType.HEADER, this.headerConverter.configs);
        assertEquals("baz", this.headerConverter.configs.get("extra.config"));
    }

    @Test
    public void shouldInstantiateAndConfigureConnectRestExtension() {
        props.put(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG,
                  TestConnectRestExtension.class.getName());
        createConfig();

        List<ConnectRestExtension> connectRestExtensions =
            plugins.newPlugins(config.getList(WorkerConfig.REST_EXTENSION_CLASSES_CONFIG),
                               config,
                               ConnectRestExtension.class);
        assertNotNull(connectRestExtensions);
        assertEquals("One Rest Extension expected", 1, connectRestExtensions.size());
        assertNotNull(connectRestExtensions.get(0));
        assertTrue("Should be instance of TestConnectRestExtension",
                   connectRestExtensions.get(0) instanceof TestConnectRestExtension);
        assertNotNull(((TestConnectRestExtension) connectRestExtensions.get(0)).configs);
        assertEquals(config.originals(),
                     ((TestConnectRestExtension) connectRestExtensions.get(0)).configs);
    }

    @Test
    public void shouldInstantiateAndConfigureDefaultHeaderConverter() {
        props.remove(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG);
        createConfig();

        // Because it's not explicitly set on the supplied configuration, the logic to use the current classloader for the connector
        // will exit immediately, and so this method always returns null
        HeaderConverter headerConverter = plugins.newHeaderConverter(config,
                                                                     WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
                                                                     ClassLoaderUsage.CURRENT_CLASSLOADER);
        assertNull(headerConverter);
        // But we should always find it (or the worker's default) when using the plugins classloader ...
        headerConverter = plugins.newHeaderConverter(config,
                                                     WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
                                                     ClassLoaderUsage.PLUGINS);
        assertNotNull(headerConverter);
        assertTrue(headerConverter instanceof SimpleHeaderConverter);
    }

    @Test
    public void shouldThrowIfPluginThrows() {
        assertThrows(ConnectException.class, () -> plugins.newPlugin(
            TestPlugin.ALWAYS_THROW_EXCEPTION.className(),
            new AbstractConfig(new ConfigDef(), Collections.emptyMap()),
            Converter.class
        ));
    }

    @Test
    public void shouldShareStaticValuesBetweenSamePlugin() {
        // Plugins are not isolated from other instances of their own class.
        Converter firstPlugin = plugins.newPlugin(
            TestPlugin.ALIASED_STATIC_FIELD.className(),
            new AbstractConfig(new ConfigDef(), Collections.emptyMap()),
            Converter.class
        );

        assertInstanceOf(SamplingTestPlugin.class, firstPlugin, "Cannot collect samples");

        Converter secondPlugin = plugins.newPlugin(
            TestPlugin.ALIASED_STATIC_FIELD.className(),
            new AbstractConfig(new ConfigDef(), Collections.emptyMap()),
            Converter.class
        );

        assertInstanceOf(SamplingTestPlugin.class, secondPlugin, "Cannot collect samples");
        assertSame(
            ((SamplingTestPlugin) firstPlugin).otherSamples(),
            ((SamplingTestPlugin) secondPlugin).otherSamples()
        );
    }

    @Test
    public void newPluginShouldServiceLoadWithPluginClassLoader() {
        Converter plugin = plugins.newPlugin(
            TestPlugin.SERVICE_LOADER.className(),
            new AbstractConfig(new ConfigDef(), Collections.emptyMap()),
            Converter.class
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        // Assert that the service loaded subclass is found in both environments
        assertTrue(samples.containsKey("ServiceLoadedSubclass.static"));
        assertTrue(samples.containsKey("ServiceLoadedSubclass.dynamic"));
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void newPluginShouldInstantiateWithPluginClassLoader() {
        Converter plugin = plugins.newPlugin(
            TestPlugin.ALIASED_STATIC_FIELD.className(),
            new AbstractConfig(new ConfigDef(), Collections.emptyMap()),
            Converter.class
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void shouldFailToFindConverterInCurrentClassloader() {
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestPlugin.SAMPLING_CONVERTER.className());
        assertThrows(ConfigException.class, this::createConfig);
    }

    @Test
    public void newConverterShouldConfigureWithPluginClassLoader() {
        props.put(WorkerConfig.KEY_CONVERTER_CLASS_CONFIG, TestPlugin.SAMPLING_CONVERTER.className());
        ClassLoader classLoader = plugins.delegatingLoader().pluginClassLoader(TestPlugin.SAMPLING_CONVERTER.className());
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            createConfig();
        }

        Converter plugin = plugins.newConverter(
            config,
            WorkerConfig.KEY_CONVERTER_CLASS_CONFIG,
            ClassLoaderUsage.PLUGINS
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        assertTrue(samples.containsKey("configure"));
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void newConfigProviderShouldConfigureWithPluginClassLoader() {
        String providerPrefix = "some.provider";
        props.put(providerPrefix + ".class", TestPlugin.SAMPLING_CONFIG_PROVIDER.className());

        PluginClassLoader classLoader = plugins.delegatingLoader().pluginClassLoader(TestPlugin.SAMPLING_CONFIG_PROVIDER.className());
        assertNotNull(classLoader);
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            createConfig();
        }

        ConfigProvider plugin = plugins.newConfigProvider(
            config,
            providerPrefix,
            ClassLoaderUsage.PLUGINS
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        assertTrue(samples.containsKey("configure"));
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void newHeaderConverterShouldConfigureWithPluginClassLoader() {
        props.put(WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG, TestPlugin.SAMPLING_HEADER_CONVERTER.className());
        ClassLoader classLoader = plugins.delegatingLoader().pluginClassLoader(TestPlugin.SAMPLING_HEADER_CONVERTER.className());
        try (LoaderSwap loaderSwap = plugins.withClassLoader(classLoader)) {
            createConfig();
        }

        HeaderConverter plugin = plugins.newHeaderConverter(
            config,
            WorkerConfig.HEADER_CONVERTER_CLASS_CONFIG,
            ClassLoaderUsage.PLUGINS
        );

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        assertTrue(samples.containsKey("configure")); // HeaderConverter::configure was called
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void newPluginsShouldConfigureWithPluginClassLoader() {
        List<Configurable> configurables = plugins.newPlugins(
            Collections.singletonList(TestPlugin.SAMPLING_CONFIGURABLE.className()),
            config,
            Configurable.class
        );
        assertEquals(1, configurables.size());
        Configurable plugin = configurables.get(0);

        assertInstanceOf(SamplingTestPlugin.class, plugin, "Cannot collect samples");
        Map<String, SamplingTestPlugin> samples = ((SamplingTestPlugin) plugin).flatten();
        assertTrue(samples.containsKey("configure")); // Configurable::configure was called
        assertPluginClassLoaderAlwaysActive(samples);
    }

    @Test
    public void pluginClassLoaderReadVersionFromResourceExistingOnlyInChild() throws Exception {
        assertClassLoaderReadsVersionFromResource(
                TestPlugin.ALIASED_STATIC_FIELD,
                TestPlugin.READ_VERSION_FROM_RESOURCE_V1,
                TestPlugin.READ_VERSION_FROM_RESOURCE_V1.className(),
                "1.0.0");
    }

    @Test
    public void pluginClassLoaderReadVersionFromResourceExistingOnlyInParent() throws Exception {
        assertClassLoaderReadsVersionFromResource(
                TestPlugin.READ_VERSION_FROM_RESOURCE_V1,
                TestPlugin.ALIASED_STATIC_FIELD,
                TestPlugin.READ_VERSION_FROM_RESOURCE_V1.className(),
                "1.0.0");
    }

    @Test
    public void pluginClassLoaderReadVersionFromResourceExistingInParentAndChild() throws Exception {
        assertClassLoaderReadsVersionFromResource(
                TestPlugin.READ_VERSION_FROM_RESOURCE_V1,
                TestPlugin.READ_VERSION_FROM_RESOURCE_V2,
                TestPlugin.READ_VERSION_FROM_RESOURCE_V2.className(),
                "2.0.0", "1.0.0");
    }

    private void assertClassLoaderReadsVersionFromResource(
            TestPlugin parentResource, TestPlugin childResource, String className, String... expectedVersions) throws MalformedURLException {
        URL[] systemPath = TestPlugins.pluginPath(parentResource)
                .stream()
                .map(File::new)
                .map(File::toURI)
                .map(uri -> {
                    try {
                        return uri.toURL();
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                })
                .toArray(URL[]::new);
        URLClassLoader parent = new URLClassLoader(systemPath);

        // Initialize Plugins object with parent class loader in the class loader tree. This is
        // to simulate the situation where jars exist on both system classpath and plugin path.
        Map<String, String> pluginProps = Collections.singletonMap(
                WorkerConfig.PLUGIN_PATH_CONFIG,
                String.join(",", TestPlugins.pluginPath(childResource))
        );
        plugins = new Plugins(pluginProps, parent);

        Converter converter = plugins.newPlugin(
                className,
                new AbstractConfig(new ConfigDef(), Collections.emptyMap()),
                Converter.class
        );
        // Verify the version was read from the correct resource
        assertEquals(expectedVersions[0],
                new String(converter.fromConnectData(null, null, null)));
        // When requesting multiple resources, they should be listed in the correct order
        assertEquals(Arrays.asList(expectedVersions),
                converter.toConnectData(null, null).value());
    }

    public static void assertPluginClassLoaderAlwaysActive(Map<String, SamplingTestPlugin> samples) {
        for (Entry<String, SamplingTestPlugin> e : samples.entrySet()) {
            String sampleName = "\"" + e.getKey() + "\" (" + e.getValue() + ")";
            assertInstanceOf(
                PluginClassLoader.class,
                e.getValue().staticClassloader(),
                sampleName + " has incorrect static classloader"
            );
            assertInstanceOf(
                PluginClassLoader.class,
                e.getValue().classloader(),
                sampleName + " has incorrect dynamic classloader"
            );
        }
    }

    public static void assertInstanceOf(Class<?> expected, Object actual, String message) {
        assertTrue(
            "Expected an instance of " + expected.getSimpleName() + ", found " + actual + " instead: " + message,
            expected.isInstance(actual)
        );
    }

    protected void instantiateAndConfigureConverter(String configPropName, ClassLoaderUsage classLoaderUsage) {
        converter = (TestConverter) plugins.newConverter(config, configPropName, classLoaderUsage);
        assertNotNull(converter);
    }

    protected void instantiateAndConfigureHeaderConverter(String configPropName) {
        headerConverter = (TestHeaderConverter) plugins.newHeaderConverter(config, configPropName, ClassLoaderUsage.CURRENT_CLASSLOADER);
        assertNotNull(headerConverter);
    }

    protected void instantiateAndConfigureInternalConverter(boolean isKey, Map<String, String> config) {
        internalConverter = (TestInternalConverter) plugins.newInternalConverter(isKey, TestInternalConverter.class.getName(), config);
        assertNotNull(internalConverter);
    }

    protected void assertConverterType(ConverterType type, Map<String, ?> props) {
        assertEquals(type.getName(), props.get(ConverterConfig.TYPE_CONFIG));
    }

    public static class TestableWorkerConfig extends WorkerConfig {
        public TestableWorkerConfig(Map<String, String> props) {
            super(WorkerConfig.baseConfigDef(), props);
        }
    }

    public static class TestConverter implements Converter, Configurable {
        public Map<String, ?> configs;

        public ConfigDef config() {
            return JsonConverterConfig.configDef();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            new JsonConverterConfig(configs); // requires the `converter.type` config be set
        }

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            this.configs = configs;
        }

        @Override
        public byte[] fromConnectData(String topic, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectData(String topic, byte[] value) {
            return null;
        }
    }

    public static class TestHeaderConverter implements HeaderConverter {
        public Map<String, ?> configs;

        @Override
        public ConfigDef config() {
            return JsonConverterConfig.configDef();
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            new JsonConverterConfig(configs); // requires the `converter.type` config be set
        }

        @Override
        public byte[] fromConnectHeader(String topic, String headerKey, Schema schema, Object value) {
            return new byte[0];
        }

        @Override
        public SchemaAndValue toConnectHeader(String topic, String headerKey, byte[] value) {
            return null;
        }

        @Override
        public void close() {
        }
    }


    public static class TestConnectRestExtension implements ConnectRestExtension {

        public Map<String, ?> configs;

        @Override
        public void register(ConnectRestExtensionContext restPluginContext) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
        }

        @Override
        public String version() {
            return "test";
        }
    }

    public static class TestInternalConverter extends JsonConverter {
        public Map<String, ?> configs;

        @Override
        public void configure(Map<String, ?> configs) {
            this.configs = configs;
            super.configure(configs);
        }
    }
}
