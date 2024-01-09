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
package org.apache.kafka.tools.automq;

import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.common.Uuid;

import static net.sourceforge.argparse4j.impl.Arguments.store;

/**
 * Generate s3url for user
 */
public class GenerateS3UrlCmd {

    private final GenerateS3UrlCmd.Parameter parameter;

    public GenerateS3UrlCmd(Parameter parameter) {
        this.parameter = parameter;
    }

    enum EndpointProtocol {

        HTTP("http"),
        HTTPS("https");

        EndpointProtocol(String key) {
            this.name = key;
        }

        final String name;

        public static EndpointProtocol getByName(String protocolName) {
            for (EndpointProtocol protocol : EndpointProtocol.values()) {
                if (protocol.name.equals(protocolName)) {
                    return protocol;
                }
            }
            throw new IllegalArgumentException("Invalid protocol: " + protocolName);
        }
    }

    enum AuthMethod {
        KEY_FROM_ENV("key-from-env"),

        KEY_FROM_ARGS("key-from-args"),
        ROLE("role");

        AuthMethod(String key) {
            this.key = key;
        }

        final String key;

        public static AuthMethod getByName(String methodName) {
            for (AuthMethod method : AuthMethod.values()) {
                if (method.key.equals(methodName)) {
                    return method;
                }
            }
            throw new IllegalArgumentException("Invalid auth method: " + methodName);
        }
    }

    static class Parameter {
        final String s3AccessKey;
        final String s3SecretKey;

        final AuthMethod s3AuthMethod;

        final String s3Region;

        final EndpointProtocol endpointProtocol;

        final String s3Endpoint;

        final String s3DataBucket;

        final String s3OpsBucket;

        Parameter(Namespace res) {
            this.s3AccessKey = res.getString("s3-access-key");
            this.s3SecretKey = res.getString("s3-secret-key");
            String authMethodName = res.getString("s3-auth-method");
            if (authMethodName == null || authMethodName.trim().isEmpty()) {
                this.s3AuthMethod = AuthMethod.KEY_FROM_ENV;
            } else {
                this.s3AuthMethod = AuthMethod.getByName(authMethodName);
            }
            this.s3Region = res.getString("s3-region");
            String endpointProtocolStr = res.get("s3-endpoint-protocol");
            this.endpointProtocol = EndpointProtocol.getByName(endpointProtocolStr);
            this.s3Endpoint = res.getString("s3-endpoint");
            this.s3DataBucket = res.getString("s3-data-bucket");
            this.s3OpsBucket = res.getString("s3-ops-bucket");
        }
    }

    static ArgumentParser argumentParser() {
        ArgumentParser parser = ArgumentParsers
            .newArgumentParser("generate-s3-url")
            .defaultHelp(true)
            .description("This cmd is used to generate s3url for AutoMQ that is used to connect to s3 or other cloud object storage service.");
        parser.addArgument("--s3AccessKey")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-access-key")
            .metavar("S3-ACCESS-KEY")
            .help("Your accessKey that used to access S3");
        parser.addArgument("--s3SecretKey")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-secret-key")
            .metavar("S3-SECRET-KEY")
            .help("Your secretKey that used to access S3");
        parser.addArgument("--s3AuthMethod")
            .action(store())
            .required(false)
            .type(String.class)
            .dest("s3-auth-method")
            .metavar("S3-AUTH-METHOD")
            .help("The auth method that used to access S3, default is key-from-env, other options are key-from-args and role");
        parser.addArgument("--s3Region")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-region")
            .metavar("S3-REGION")
            .help("The region of S3");
        parser.addArgument("--s3Endpoint")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-endpoint")
            .metavar("S3-ENDPOINT")
            .help("The endpoint of S3");
        parser.addArgument("--s3DataBucket")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-data-bucket")
            .metavar("S3-DATA-BUCKET")
            .help("The bucket name of S3 that used to store kafka's stream data");
        parser.addArgument("--s3OpsBucket")
            .action(store())
            .required(true)
            .type(String.class)
            .dest("s3-ops-bucket")
            .metavar("S3-OPS-BUCKET")
            .help("The bucket name of S3 that used to store operation data like metric,log,naming service info etc.");

        return parser;
    }

    public String run() {
        String s3Url = buildS3Url();
        System.out.printf(String.format("Your S3 URL is: %s", s3Url));
        return s3Url;
    }

    private String buildS3Url() {
        StringBuilder s3UrlBuilder = new StringBuilder();
        s3UrlBuilder
            .append("s3://")
            .append(parameter.s3Endpoint)
            .append("?").append("s3-access-key=").append(parameter.s3AccessKey)
            .append("&").append("s3-secret-key=").append(parameter.s3SecretKey)
            .append("&").append("s3-region=").append(parameter.s3Region)
            .append("&").append("s3-auth-method=").append(parameter.s3AuthMethod.key)
            .append("&").append("s3-endpoint-protocol=").append(parameter.endpointProtocol.name)
            .append("&").append("s3-data-bucket=").append(parameter.s3DataBucket)
            .append("&").append("s3-ops-bucket=").append(parameter.s3OpsBucket)
            .append("&").append("cluster-id=").append(Uuid.randomUuid());
        return s3UrlBuilder.toString();
    }

}
