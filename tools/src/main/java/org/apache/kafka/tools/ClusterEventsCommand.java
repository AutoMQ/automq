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

package org.apache.kafka.tools;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ClusterEventTypeRegistry;
import org.apache.kafka.clients.admin.ClusterEventsReader;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.server.util.CommandDefaultOptions;
import org.apache.kafka.server.util.CommandLineUtils;

import java.io.IOException;
import java.io.PrintStream;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import io.cloudevents.CloudEvent;
import joptsimple.ArgumentAcceptingOptionSpec;

/**
 * CLI tool for querying the {@code __automq_cluster_events} internal topic.
 *
 * <p>When {@code --until} is not specified, runs in tail mode: continuously polls
 * for new events until Ctrl-C or {@code --max-events} is reached.
 *
 * <pre>
 * kafka-cluster-events.sh --bootstrap-server localhost:9092 \
 *   --type com.automq.ops.rebalance.summary --since -24h
 * </pre>
 */
public class ClusterEventsCommand {

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(2);

    public static void main(String[] args) {
        Exit.exit(mainNoExit(args, System.out));
    }

    static int mainNoExit(String[] args, PrintStream out) {
        ClusterEventsCommandOptions opts = new ClusterEventsCommandOptions(args);
        try {
            opts.checkArgs();
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
            try {
                opts.parser.printHelpOn(out);
            } catch (IOException ioe) {
                // ignore
            }
            return 1;
        }

        Properties props = buildAdminProps(opts, out);
        if (props == null) return 1;

        try (Admin admin = Admin.create(props);
             ClusterEventsReader reader = admin.describeClusterEvents(parseSinceMs(opts))) {
            return pollLoop(reader, out, opts);
        } catch (Exception e) {
            out.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static Long parseSinceMs(ClusterEventsCommandOptions opts) {
        return opts.options.has(opts.sinceOpt)
            ? parseTimeArg(opts.options.valueOf(opts.sinceOpt)) : null;
    }

    private static int pollLoop(ClusterEventsReader reader, PrintStream out,
                                ClusterEventsCommandOptions opts) {
        EventFilter filter = EventFilter.from(opts);
        AtomicBoolean running = new AtomicBoolean(true);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> running.set(false)));

        int count = 0;
        while (running.get() && count < filter.maxEvents) {
            List<CloudEvent> events = reader.poll(POLL_TIMEOUT);
            for (CloudEvent event : events) {
                if (filter.isPastUntil(event)) return 0;
                if (filter.matches(event)) {
                    out.println(formatEvent(event));
                    if (++count >= filter.maxEvents) return 0;
                }
            }
            if (!filter.tailMode && reader.isPolledToEnd()) break;
        }
        return 0;
    }

    private static final class EventFilter {
        final Long untilMs;
        final int maxEvents;
        final String eventType;
        final Pattern sourceRegex;
        final Pattern subjectRegex;
        final boolean tailMode;

        EventFilter(Long untilMs, int maxEvents, String eventType,
                    Pattern sourceRegex, Pattern subjectRegex) {
            this.untilMs = untilMs;
            this.maxEvents = maxEvents;
            this.eventType = eventType;
            this.sourceRegex = sourceRegex;
            this.subjectRegex = subjectRegex;
            this.tailMode = untilMs == null;
        }

        static EventFilter from(ClusterEventsCommandOptions opts) {
            Long untilMs = opts.options.has(opts.untilOpt)
                ? parseTimeArg(opts.options.valueOf(opts.untilOpt)) : null;
            int maxEvents = opts.options.has(opts.maxEventsOpt)
                ? opts.options.valueOf(opts.maxEventsOpt) : Integer.MAX_VALUE;
            String eventType = opts.options.has(opts.typeOpt)
                ? opts.options.valueOf(opts.typeOpt) : null;
            Pattern sourceRegex = opts.options.has(opts.sourceOpt)
                ? Pattern.compile(opts.options.valueOf(opts.sourceOpt)) : null;
            Pattern subjectRegex = opts.options.has(opts.subjectOpt)
                ? Pattern.compile(opts.options.valueOf(opts.subjectOpt)) : null;
            return new EventFilter(untilMs, maxEvents, eventType, sourceRegex, subjectRegex);
        }

        boolean isPastUntil(CloudEvent event) {
            if (untilMs == null) return false;
            java.time.OffsetDateTime time = event.getTime();
            return time != null && time.toInstant().toEpochMilli() > untilMs;
        }

        boolean matches(CloudEvent event) {
            if (event == null) return false;
            if (eventType != null && !eventType.equals(event.getType())) return false;
            if (!matchesPattern(sourceRegex, event.getSource() != null ? event.getSource().toString() : null))
                return false;
            return matchesPattern(subjectRegex, event.getSubject());
        }

        private static boolean matchesPattern(Pattern pattern, String value) {
            if (pattern == null) return true;
            return value != null && pattern.matcher(value).find();
        }
    }

    private static Properties buildAdminProps(ClusterEventsCommandOptions opts, PrintStream out) {
        Properties props = new Properties();
        if (opts.options.has(opts.commandConfigOpt)) {
            try {
                props.putAll(Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt)));
            } catch (IOException e) {
                out.println("Error loading command config: " + e.getMessage());
                return null;
            }
        }
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt));
        return props;
    }

    /**
     * Parse a time argument. Accepts:
     * <ul>
     *   <li>ISO-8601 datetime: {@code 2026-04-07T10:00:00Z}</li>
     *   <li>Relative: {@code -1h}, {@code -24h}, {@code -7d}</li>
     * </ul>
     */
    static long parseTimeArg(String value) {
        value = value.trim();
        Pattern relative = Pattern.compile("^-(\\d+)([hHdD])$");
        Matcher m = relative.matcher(value);
        if (m.matches()) {
            long amount = Long.parseLong(m.group(1));
            String unit = m.group(2).toLowerCase(java.util.Locale.ROOT);
            long millis = unit.equals("h") ? amount * 3600_000L : amount * 86400_000L;
            return System.currentTimeMillis() - millis;
        }
        try {
            return OffsetDateTime.parse(value, DateTimeFormatter.ISO_OFFSET_DATE_TIME)
                .toInstant().toEpochMilli();
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Cannot parse time value '" + value
                + "'. Use ISO-8601 (e.g. 2026-04-07T10:00:00Z) or relative (e.g. -1h, -7d).");
        }
    }

    private static String formatEvent(CloudEvent event) {
        StringBuilder sb = new StringBuilder();
        sb.append("type=").append(event.getType());
        sb.append(" source=").append(event.getSource());
        if (event.getSubject() != null)
            sb.append(" subject=").append(event.getSubject());
        if (event.getTime() != null)
            sb.append(" time=").append(event.getTime());
        ClusterEventTypeRegistry.decode(event).ifPresentOrElse(
            decoded -> sb.append(" data=").append(decoded),
            () -> {
                io.cloudevents.CloudEventData data = event.getData();
                if (data != null) {
                    byte[] bytes = data.toBytes();
                    if (bytes != null)
                        sb.append(" data=").append(new String(bytes, java.nio.charset.StandardCharsets.UTF_8));
                }
            }
        );
        return sb.toString();
    }

    static final class ClusterEventsCommandOptions extends CommandDefaultOptions {
        final ArgumentAcceptingOptionSpec<String> bootstrapServerOpt;
        final ArgumentAcceptingOptionSpec<String> commandConfigOpt;
        final ArgumentAcceptingOptionSpec<String> typeOpt;
        final ArgumentAcceptingOptionSpec<String> sinceOpt;
        final ArgumentAcceptingOptionSpec<String> untilOpt;
        final ArgumentAcceptingOptionSpec<String> sourceOpt;
        final ArgumentAcceptingOptionSpec<String> subjectOpt;
        final ArgumentAcceptingOptionSpec<Integer> maxEventsOpt;

        ClusterEventsCommandOptions(String[] args) {
            super(args);
            bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: The Kafka server to connect to.")
                .withRequiredArg().describedAs("server").ofType(String.class);
            commandConfigOpt = parser.accepts("command-config",
                "Property file containing configs to be passed to Admin Client.")
                .withRequiredArg().describedAs("config file").ofType(String.class);
            typeOpt = parser.accepts("type", "Filter by CloudEvent type (e.g. com.automq.ops.rebalance.summary).")
                .withRequiredArg().ofType(String.class);
            sinceOpt = parser.accepts("since",
                "Only show events at or after this time. ISO-8601 or relative (e.g. -1h, -7d).")
                .withRequiredArg().ofType(String.class);
            untilOpt = parser.accepts("until",
                "Only show events at or before this time. ISO-8601 or relative. "
                + "If not set, runs in tail mode (continuously polls until Ctrl-C).")
                .withRequiredArg().ofType(String.class);
            sourceOpt = parser.accepts("source", "Filter by source (regex).")
                .withRequiredArg().ofType(String.class);
            subjectOpt = parser.accepts("subject", "Filter by subject (regex).")
                .withRequiredArg().ofType(String.class);
            maxEventsOpt = parser.accepts("max-events", "Maximum number of events to return.")
                .withRequiredArg().ofType(Integer.class);
            options = parser.parse(args);
        }

        void checkArgs() {
            CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt);
        }
    }
}
