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
package org.apache.kafka.common.security.ssl;

import java.io.File;
import java.nio.file.Files;
import java.security.KeyStore;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.SecurityConfig;
import org.apache.kafka.common.config.SslConfigs;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.common.security.auth.SslEngineFactory;
import org.apache.kafka.common.security.ssl.mock.TestKeyManagerFactory;
import org.apache.kafka.common.security.ssl.mock.TestProviderCreator;
import org.apache.kafka.common.security.ssl.mock.TestTrustManagerFactory;
import org.apache.kafka.common.utils.Java;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.common.network.Mode;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import java.security.Security;

@RunWith(value = Parameterized.class)
public class SslFactoryTest {

    private final String tlsProtocol;

    @Parameterized.Parameters(name = "tlsProtocol={0}")
    public static Collection<Object[]> data() {
        List<Object[]> values = new ArrayList<>();
        values.add(new Object[] {"TLSv1.2"});
        if (Java.IS_JAVA11_COMPATIBLE) {
            values.add(new Object[] {"TLSv1.3"});
        }
        return values;
    }

    public SslFactoryTest(String tlsProtocol) {
        this.tlsProtocol = tlsProtocol;
    }

    @Test
    public void testSslFactoryConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertNotNull(engine);
        assertEquals(Utils.mkSet(tlsProtocol), Utils.mkSet(engine.getEnabledProtocols()));
        assertEquals(false, engine.getUseClientMode());
    }

    @Test
    public void testSslFactoryWithCustomKeyManagerConfiguration() {
        TestProviderCreator testProviderCreator = new TestProviderCreator();
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM,
                tlsProtocol
        );
        serverSslConfig.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG, testProviderCreator.getClass().getName());
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertNotNull("SslEngineFactory not created", sslFactory.sslEngineFactory());
        Security.removeProvider(testProviderCreator.getProvider().getName());
    }

    @Test(expected = KafkaException.class)
    public void testSslFactoryWithoutProviderClassConfiguration() {
        // An exception is thrown as the algorithm is not registered through a provider
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM,
                tlsProtocol
        );
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
    }

    @Test(expected = KafkaException.class)
    public void testSslFactoryWithIncorrectProviderClassConfiguration() {
        // An exception is thrown as the algorithm is not registered through a provider
        Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(
                TestKeyManagerFactory.ALGORITHM,
                TestTrustManagerFactory.ALGORITHM,
                tlsProtocol
        );
        serverSslConfig.put(SecurityConfig.SECURITY_PROVIDERS_CONFIG,
                "com.fake.ProviderClass1,com.fake.ProviderClass2");
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
    }

    @Test
    public void testSslFactoryWithoutPasswordConfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        // unset the password
        serverSslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        try {
            sslFactory.configure(serverSslConfig);
        } catch (Exception e) {
            fail("An exception was thrown when configuring the truststore without a password: " + e);
        }
    }

    @Test
    public void testClientMode() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        //host and port are hints
        SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
        assertTrue(engine.getUseClientMode());
    }

    @Test
    public void testReconfiguration() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SslEngineFactory sslEngineFactory = sslFactory.sslEngineFactory();
        assertNotNull("SslEngineFactory not created", sslEngineFactory);

        // Verify that SslEngineFactory is not recreated on reconfigure() if config and
        // file are not changed
        sslFactory.reconfigure(sslConfig);
        assertSame("SslEngineFactory recreated unnecessarily",
                sslEngineFactory, sslFactory.sslEngineFactory());

        // Verify that the SslEngineFactory is recreated on reconfigure() if config is changed
        trustStoreFile = File.createTempFile("truststore", ".jks");
        sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineFactory not recreated",
                sslEngineFactory, sslFactory.sslEngineFactory());
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that builder is recreated on reconfigure() if config is not changed, but truststore file was modified
        trustStoreFile.setLastModified(System.currentTimeMillis() + 10000);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineFactory not recreated",
                sslEngineFactory, sslFactory.sslEngineFactory());
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that builder is recreated on reconfigure() if config is not changed, but keystore file was modified
        File keyStoreFile = new File((String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG));
        keyStoreFile.setLastModified(System.currentTimeMillis() + 10000);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineFactory not recreated",
                sslEngineFactory, sslFactory.sslEngineFactory());
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that builder is recreated after validation on reconfigure() if config is not changed, but keystore file was modified
        keyStoreFile.setLastModified(System.currentTimeMillis() + 15000);
        sslFactory.validateReconfiguration(sslConfig);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SslEngineFactory not recreated",
                sslEngineFactory, sslFactory.sslEngineFactory());
        sslEngineFactory = sslFactory.sslEngineFactory();

        // Verify that the builder is not recreated if modification time cannot be determined
        keyStoreFile.setLastModified(System.currentTimeMillis() + 20000);
        Files.delete(keyStoreFile.toPath());
        sslFactory.reconfigure(sslConfig);
        assertSame("SslEngineFactory recreated unnecessarily",
                sslEngineFactory, sslFactory.sslEngineFactory());
    }

    @Test
    public void testReconfigurationWithoutTruststore() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SSLContext sslContext = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext();
        assertNotNull("SSL context not created", sslContext);
        assertSame("SSL context recreated unnecessarily", sslContext,
                ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext());
        assertFalse(sslFactory.createSslEngine("localhost", 0).getUseClientMode());

        Map<String, Object> sslConfig2 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        try {
            sslFactory.validateReconfiguration(sslConfig2);
            fail("Truststore configured dynamically for listener without previous truststore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testReconfigurationWithoutKeystore() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(sslConfig);
        SSLContext sslContext = ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext();
        assertNotNull("SSL context not created", sslContext);
        assertSame("SSL context recreated unnecessarily", sslContext,
                ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext());
        assertFalse(sslFactory.createSslEngine("localhost", 0).getUseClientMode());

        File newTrustStoreFile = File.createTempFile("truststore", ".jks");
        sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(newTrustStoreFile)
                .build();
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        sslConfig.remove(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        sslFactory.reconfigure(sslConfig);
        assertNotSame("SSL context not recreated", sslContext,
                ((DefaultSslEngineFactory) sslFactory.sslEngineFactory()).sslContext());

        sslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(newTrustStoreFile)
                .build();
        try {
            sslFactory.validateReconfiguration(sslConfig);
            fail("Keystore configured dynamically for listener without previous keystore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testKeyStoreTrustStoreValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertNotNull("SslEngineFactory not created", sslFactory.sslEngineFactory());
    }

    @Test
    public void testUntrustedKeyStoreValidationFails() throws Exception {
        File trustStoreFile1 = File.createTempFile("truststore1", ".jks");
        File trustStoreFile2 = File.createTempFile("truststore2", ".jks");
        Map<String, Object> sslConfig1 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile1)
                .build();
        Map<String, Object> sslConfig2 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile2)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER, null, true);
        for (String key : Arrays.asList(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG,
                SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG,
                SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG)) {
            sslConfig1.put(key, sslConfig2.get(key));
        }
        try {
            sslFactory.configure(sslConfig1);
            fail("Validation did not fail with untrusted truststore");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testKeystoreVerifiableUsingTruststore() throws Exception {
        File trustStoreFile1 = File.createTempFile("truststore1", ".jks");
        Map<String, Object> sslConfig1 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile1)
                .build();
        SslFactory sslFactory = new SslFactory(Mode.SERVER, null, true);
        sslFactory.configure(sslConfig1);

        File trustStoreFile2 = File.createTempFile("truststore2", ".jks");
        Map<String, Object> sslConfig2 = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile2)
                .build();
        // Verify that `createSSLContext` fails even if certificate from new keystore is trusted by
        // the new truststore, if certificate is not trusted by the existing truststore on the `SslFactory`.
        // This is to prevent both keystores and truststores to be modified simultaneously on an inter-broker
        // listener to stores that may not work with other brokers where the update hasn't yet been performed.
        try {
            sslFactory.validateReconfiguration(sslConfig2);
            fail("ValidateReconfiguration did not fail as expected");
        } catch (ConfigException e) {
            // Expected exception
        }
    }

    @Test
    public void testCertificateEntriesValidation() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .build();
        Map<String, Object> newCnConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(File.createTempFile("truststore", ".jks"))
                .cn("Another CN")
                .build();
        KeyStore ks1 = sslKeyStore(serverSslConfig).get();
        KeyStore ks2 = sslKeyStore(serverSslConfig).get();
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2));

        // Use different alias name, validation should succeed
        ks2.setCertificateEntry("another", ks1.getCertificate("localhost"));
        assertEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks2));

        KeyStore ks3 = sslKeyStore(newCnConfig).get();
        assertNotEquals(SslFactory.CertificateEntries.create(ks1), SslFactory.CertificateEntries.create(ks3));
    }

    /**
     * Tests client side ssl.engine.factory configuration is used when specified
     */
    @Test
    public void testClientSpecifiedSslEngineFactoryUsed() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
        assertTrue("SslEngineFactory must be of expected type",
                sslFactory.sslEngineFactory() instanceof TestSslUtils.TestSslEngineFactory);
    }

    /**
     * Tests server side ssl.engine.factory configuration is used when specified
     */
    @Test
    public void testServerSpecifiedSslEngineFactoryUsed() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> serverSslConfig = sslConfigsBuilder(Mode.SERVER)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        serverSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, TestSslUtils.TestSslEngineFactory.class);
        SslFactory sslFactory = new SslFactory(Mode.SERVER);
        sslFactory.configure(serverSslConfig);
        assertTrue("SslEngineFactory must be of expected type",
                sslFactory.sslEngineFactory() instanceof TestSslUtils.TestSslEngineFactory);
    }

    /**
     * Tests invalid ssl.engine.factory configuration
     */
    @Test(expected = ClassCastException.class)
    public void testInvalidSslEngineFactory() throws Exception {
        File trustStoreFile = File.createTempFile("truststore", ".jks");
        Map<String, Object> clientSslConfig = sslConfigsBuilder(Mode.CLIENT)
                .createNewTrustStore(trustStoreFile)
                .useClientCert(false)
                .build();
        clientSslConfig.put(SslConfigs.SSL_ENGINE_FACTORY_CLASS_CONFIG, String.class);
        SslFactory sslFactory = new SslFactory(Mode.CLIENT);
        sslFactory.configure(clientSslConfig);
    }

    private DefaultSslEngineFactory.SecurityStore sslKeyStore(Map<String, Object> sslConfig) {
        return new DefaultSslEngineFactory.SecurityStore(
                (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG),
                (String) sslConfig.get(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG),
                (Password) sslConfig.get(SslConfigs.SSL_KEY_PASSWORD_CONFIG)
        );
    }

    private TestSslUtils.SslConfigsBuilder sslConfigsBuilder(Mode mode) {
        return new TestSslUtils.SslConfigsBuilder(mode).tlsProtocol(tlsProtocol);
    }
}
