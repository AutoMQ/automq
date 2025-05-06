/*
 * Copyright 2025, AutoMQ HK Limited.
 *
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

package kafka.automq.table;

import kafka.server.KafkaConfig;

import com.automq.stream.s3.operator.AwsObjectStorage;
import com.automq.stream.s3.operator.BucketURI;
import com.automq.stream.utils.IdURI;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;

public class CatalogFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(CatalogFactory.class);
    private static final String CATALOG_TYPE_CONFIG = "type";

    public Catalog newCatalog(KafkaConfig config) {
        return new Builder(config).build();
    }

    static class Builder {
        final KafkaConfig config;

        String catalogImpl;
        BucketURI bucketURI;
        final Map<String, Object> catalogConfigs;
        final Map<String, Object> hadoopConfigs;
        final Map<String, String> options = new HashMap<>();
        final Configuration hadoopConf = new Configuration();
        UserGroupInformation ugi = null;
        Catalog catalog = null;

        Builder(KafkaConfig config) {
            this.config = config;
            catalogConfigs = config.originalsWithPrefix("automq.table.topic.catalog.");
            hadoopConfigs = config.originalsWithPrefix("automq.table.topic.hadoop.");
            String catalogType = Optional.ofNullable(catalogConfigs.get(CATALOG_TYPE_CONFIG)).map(Object::toString).orElse(null);
            if (StringUtils.isBlank(catalogType)) {
                return;
            }
            bucketURI = config.automq().dataBuckets().get(0);
            CredentialProviderHolder.setup(bucketURI);
            options.put("ref", "main");
            options.put("client.region", bucketURI.region());
            options.put("client.credentials-provider", "kafka.automq.table.CredentialProviderHolder");
            switch (catalogType) {
                case "glue":
                    withGlue();
                    break;
                case "nessie":
                    withNessie();
                    break;
                case "tablebucket":
                    withTableBucket();
                    break;
                case "hive":
                    withHive();
                    break;
                case "rest":
                    withRest();
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported catalog type: " + catalogType);
            }
            catalogConfigs.forEach((k, v) -> options.put(k, v.toString()));
            hadoopConfigs.forEach((k, v) -> hadoopConf.set(k, v.toString()));
            options.remove(CATALOG_TYPE_CONFIG);
            LOGGER.info("[TABLE_MANAGER_START],catalog={},options={},hadoopConfig={}", catalogType, options, hadoopConf);
            this.catalog = runAs(() -> CatalogUtil.loadCatalog(catalogImpl, catalogType, options, hadoopConf));
        }

        public Catalog build() {
            return catalog;
        }

        private void withGlue() {
            catalogImpl = "org.apache.iceberg.aws.glue.GlueCatalog";
            if (StringUtils.isNotBlank(bucketURI.endpoint())) {
                options.put("glue.endpoint", bucketURI.endpoint().replaceFirst("s3", "glue"));
            }
            putDataBucketAsWarehouse(false);
        }

        private void withNessie() {
            // nessie config extension e.g.
            // automq.table.topic.catalog.uri=http://localhost:19120/api/v2
            catalogImpl = "org.apache.iceberg.nessie.NessieCatalog";
            putDataBucketAsWarehouse(false);
        }

        private void withTableBucket() {
            // table bucket config extension e.g.
            // automq.table.topic.catalog.warehouse=table bucket arn
            catalogImpl = "software.amazon.s3tables.iceberg.S3TablesCatalog";
        }

        private void withHive() {
            // hive config extension e.g.
            // automq.table.topic.catalog.uri=thrift://xxx:9083
            // kerberos authentication
            // - automq.table.topic.catalog.auth=kerberos://?principal=base64(clientPrincipal)&keytab=base64(keytabFile)&krb5conf=base64(krb5confFile)
            // - automq.table.topic.hadoop.metastore.kerberos.principal=serverPrincipal

            // simple authentication
            // - automq.table.topic.catalog.auth=simple://?username=xxx
            catalogImpl = "org.apache.iceberg.hive.HiveCatalog";
            putDataBucketAsWarehouse(true);

            IdURI uri = IdURI.parse("0@" + catalogConfigs.getOrDefault("auth", "none://?"));
            try {
                switch (uri.protocol()) {
                    case "kerberos": {
                        System.setProperty("sun.security.krb5.debug", "true");
                        String configBasePath = config.metadataLogDir();
                        System.setProperty(
                            "java.security.krb5.conf",
                            base64Config2file(uri.extensionString("krb5conf"), configBasePath, "krb5.conf")
                        );
                        Configuration configuration = new Configuration();
                        configuration.set("hadoop.security.authentication", "Kerberos");
                        UserGroupInformation.setConfiguration(configuration);
                        UserGroupInformation.loginUserFromKeytab(
                            decodeBase64(uri.extensionString("principal")),
                            base64Config2file(uri.extensionString("keytab"), configBasePath, "keytab")
                        );
                        ugi = UserGroupInformation.getCurrentUser();
                        hadoopConf.set("metastore.sasl.enabled", "true");
                        break;
                    }
                    case "simple": {
                        ugi = UserGroupInformation.createRemoteUser(uri.extensionString("username"));
                        UserGroupInformation.setLoginUser(ugi);
                        hadoopConf.set("metastore.sasl.enabled", "true");
                        break;
                    }
                    default: {
                    }
                }
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        }

        private void withRest() {
            // rest config extension e.g.
            // automq.table.topic.catalog.uri=http://127.0.0.1:9001/iceberg
            // If a token is set, HTTP requests use the value as a bearer token in the HTTP Authorization header.
            // If credential is used, then the key and secret are used to fetch a token using the OAuth2 client credentials flow.
            // The resulting token is used as the bearer token for subsequent requests.
            // config ref. org.apache.iceberg.rest.RESTSessionCatalog#initialize
            // automq.table.topic.catalog.oauth2-server-uri=
            // automq.table.topic.catalog.credential=
            // automq.table.topic.catalog.token=
            // automq.table.topic.catalog.scope=
            catalogImpl = "org.apache.iceberg.rest.RESTCatalog";
            putDataBucketAsWarehouse(false);
        }

        private Catalog runAs(Supplier<Catalog> func) {
            if (ugi != null) {
                return ugi.doAs((PrivilegedAction<Catalog>) func::get);
            } else {
                return func.get();
            }
        }

        private void putDataBucketAsWarehouse(boolean s3a) {
            if (bucketURI.endpoint() != null) {
                options.put("s3.endpoint", bucketURI.endpoint());
            }
            if (bucketURI.extensionBool(AwsObjectStorage.PATH_STYLE_KEY, false)) {
                options.put("s3.path-style-access", "true");
            }
            options.put("io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
            options.put("warehouse", String.format((s3a ? "s3a" : "s3") + "://%s/iceberg", bucketURI.bucket()));
        }

    }

    /**
     * Decode base64 str and save it to file
     *
     * @return the file path
     */
    private static String base64Config2file(String base64, String configPath, String configName) {
        byte[] bytes = Base64.getDecoder().decode(base64);
        try {
            Path dir = Paths.get(configPath);
            if (!Files.exists(dir)) {
                Files.createDirectories(dir);
            }
            Path filePath = Paths.get(configPath + File.separator + configName);
            Files.write(filePath, bytes, CREATE, TRUNCATE_EXISTING);
            return filePath.toAbsolutePath().toString();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    private static String decodeBase64(String base64) {
        return new String(Base64.getDecoder().decode(base64), StandardCharsets.ISO_8859_1);
    }
}
