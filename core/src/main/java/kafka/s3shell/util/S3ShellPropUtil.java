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
package kafka.s3shell.util;

import com.automq.s3shell.sdk.constant.ServerConfigKey;
import com.automq.s3shell.sdk.model.S3Url;
import com.automq.s3shell.sdk.util.S3PropUtil;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.common.internals.FatalExitError;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.automq.s3shell.sdk.auth.EnvVariableCredentialsProvider.ACCESS_KEY_NAME;
import static com.automq.s3shell.sdk.auth.EnvVariableCredentialsProvider.SECRET_KEY_NAME;

public class S3ShellPropUtil {

    private static OptionParser acceptOption() {
        OptionParser optionParser = new OptionParser();
        optionParser.accepts("override", "Optional property that should override values set in server.properties file")
            .withRequiredArg()
            .ofType(String.class);
        optionParser.accepts("config", "Path to server.properties file")
            .withRequiredArg()
            .ofType(String.class);
        optionParser.accepts("s3-url", "URL for S3 storage")
            .withRequiredArg()
            .ofType(String.class);
        optionParser.accepts("version", "Print version information and exit.");
        return optionParser;
    }

    public static Properties autoGenPropsByCmd(String[] args, String processRole) throws IOException {
        if (args.length < 1) {
            throw new FatalExitError(1);
        }

        Properties props = new Properties();
        switch (processRole) {
            case "broker":
                props.putAll(S3PropUtil.loadTemplateProps(S3PropUtil.BROKER_PROPS_PATH));
                break;
            case "controller":
                props.putAll(S3PropUtil.loadTemplateProps(S3PropUtil.CONTROLLER_PROPS_PATH));
                break;
            case "broker,controller":
            case "controller,broker":
                props.putAll(S3PropUtil.loadTemplateProps(S3PropUtil.SERVER_PROPS_PATH));
                break;
            default:
                throw new IllegalArgumentException("Invalid process role:" + processRole);
        }

        // Handle --override options
        OptionParser optionParser = acceptOption();
        OptionSet options = optionParser.parse(args);
        handleOption(options, props);

        return props;
    }

    private static void handleOption(OptionSet options, Properties props) {
        S3Url s3Url = null;
        if (options.has("s3-url")) {
            String s3UrlStr = (String) options.valueOf("s3-url");
            s3Url = S3Url.parse(s3UrlStr);
            props.put(ServerConfigKey.S3_ENDPOINT.getKeyName(), s3Url.getEndpointProtocol().getName() + "://" + s3Url.getS3Endpoint());
            props.put(ServerConfigKey.S3_REGION.getKeyName(), s3Url.getS3Region());
            props.put(ServerConfigKey.S3_BUCKET.getKeyName(), s3Url.getS3DataBucket());
            props.put(ServerConfigKey.S3_PATH_STYLE.getKeyName(), String.valueOf(s3Url.isS3PathStyle()));

            // override system env
            // TODO: no need to override system env here, just set ak/sk to CredentialsProviderHolder
            EnvUtil.setEnv(ACCESS_KEY_NAME, s3Url.getS3AccessKey());
            EnvUtil.setEnv(SECRET_KEY_NAME, s3Url.getS3SecretKey());
        }

        if (options.has("override")) {
            List<?> overrideOptions = options.valuesOf("override");
            for (Object o : overrideOptions) {
                String option = (String) o;
                String[] keyValue = option.split("=", 2);
                if (keyValue.length == 2) {
                    props.setProperty(keyValue[0], keyValue[1]);
                } else {
                    throw new IllegalArgumentException("Invalid override option format: " + option);
                }
            }
        }

        //format storage
        if (s3Url != null) {
            try {
                KafkaFormatUtil.formatStorage(s3Url.getClusterId(), props);
            } catch (IOException e) {
                throw new RuntimeException(String.format("Format storage failed for cluster:%s", s3Url.getClusterId()), e);
            }
        }
    }
}
