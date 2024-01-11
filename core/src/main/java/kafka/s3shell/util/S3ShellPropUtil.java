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
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import kafka.utils.CommandLineUtils;
import org.apache.kafka.common.internals.FatalExitError;
import org.apache.kafka.common.utils.Utils;

import static kafka.log.stream.s3.ConfigUtils.ACCESS_KEY_NAME;
import static kafka.log.stream.s3.ConfigUtils.SECRET_KEY_NAME;

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

    public static Properties getPropsFromArgs(String[] args) throws IOException {

        OptionParser optionParser = acceptOption();
        if (args.length == 0 || Arrays.asList(args).contains("--help")) {
            CommandLineUtils.printUsageAndDie(optionParser,
                String.format("USAGE: java [options] %s [--config server.properties] [--override property=value]* [--s3-url url]",
                    S3ShellPropUtil.class.getCanonicalName().split("\\$")[0]));
        }

        if (Arrays.asList(args).contains("--version")) {
            CommandLineUtils.printVersionAndDie();
        }

        OptionSet options = optionParser.parse(args);
        Properties props = new Properties();

        List<?> nonOptionArgs = options.nonOptionArguments();
        if (nonOptionArgs.size() > 1) {
            throw new FatalExitError(1);
        }

        String configPath = null;
        if (options.has("config")) {
            configPath = (String) options.valueOf("config");
        } else if (!nonOptionArgs.isEmpty()) {
            configPath = (String) nonOptionArgs.get(0);
        }

        if (configPath != null) {
            props.putAll(Utils.loadProps(configPath));
        }

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

