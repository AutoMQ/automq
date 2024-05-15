/*
 * Copyright 2024, AutoMQ CO.,LTD.
 *
 * Use of this software is governed by the Business Source License
 * included in the file BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */
package kafka.s3shell.util;

import com.automq.shell.constant.ServerConfigKey;
import com.automq.shell.model.S3Url;
import com.automq.shell.util.S3PropUtil;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.apache.kafka.common.internals.FatalExitError;

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
