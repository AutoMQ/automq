---
last_updated: 2026-10-15
---

# Automatic JMX metrics in Kafka Connect

Connect workers automatically collect readable numeric attributes from the local platform
`MBeanServer`. A connector does not need a metrics reporter, an ObjectName allowlist, a YAML
mapping, or a plugin-specific profile. MBeans registered after the worker starts are discovered
by the background sampler. Sampling also refreshes the attributes of dynamic MBeans.

The collector records through the worker's existing OpenTelemetry SDK. The existing
`automq.telemetry.exporter.uri` selects all destinations, including `ops://`, Prometheus HTTP,
and enterprise Prometheus Remote Write. Adding a connector does not change that configuration.
Remote Write must already be enabled for the Connect worker; Kafka instance exporter settings
are not automatically inherited by a separately configured Connect cluster.

## Values and identity

Numbers are exported as raw double gauges, Boolean values as `1` or `0`. CompositeData,
TabularData, maps with scalar keys, and arrays are recursively expanded within fixed resource
limits. Strings, arbitrary objects, nulls, and non-finite numbers are not converted into metric
values. Numeric values follow the precision limits of the Prometheus float64 sample model.
Units and counter semantics are not inferred from attribute names.

Metric names have the form:

```text
connect_jmx_<normalized-domain-and-attribute-path>_<stable-hash>_value
```

The stable hash includes the original domain and attribute path, so character normalization and
name truncation do not merge different attributes. ObjectName property values are labels rather
than metric name components. Every point includes:

- `worker_id`: generated once per worker JVM to distinguish replicas and process restarts.
- `jmx_domain`: the original domain.
- `jmx_object_name`: the canonical ObjectName.
- `jmx_attribute`: the attribute path; nested path segments use JSON Pointer escaping.
- `jmx_prop_<key>`: ObjectName properties with collision-safe encoding for label keys.
- Map keys, tabular indices, and array indices as additional `jmx_key_*`, `jmx_row_*`, or
  `jmx_index_*` dimensions.

Worker base labels, including `connect_cluster_id` when supplied by the control plane, are also
attached to data points. This preserves identity through the OPS exporter as well as Remote
Write. The collector retains native plugin identity; it does not invent a connector or task
mapping when the MBean does not expose one.

For example, a Grafana panel can select a Debezium streaming attribute without knowing the
generated hash:

```promql
{
  __name__=~"connect_jmx_.*_value",
  jmx_domain="debezium.mysql",
  jmx_attribute="MilliSecondsBehindSource",
  jmx_prop_context="streaming"
}
```

Use the connector's documentation to choose units and aggregation. A cumulative number is still
exported as a raw gauge; automatic ingestion does not supply a business-specific dashboard.

## Sampling and lifecycle

Sampling runs independently of exporter callbacks. All readers consume the same immutable
snapshot, so a Prometheus scrape and an OPS or Remote Write collection do not execute connector
getters again. Registration notifications track MBean generations; periodic discovery reconciles
the live set. Removed MBeans stop reporting, and reusing an ObjectName does not replay the old
instance's sample. Cached samples expire if the sampler cannot refresh them.

The existing `automq.telemetry.exporter.interval.ms` determines the sampling interval. Visibility
also depends on the existing exporter, bucket upload, and downstream synchronization intervals.
The collector belongs to the worker, not an individual task or Kafka client reporter. Task
rebalances keep it running; worker shutdown closes its readers, listeners, and callbacks.

Discovery and reads have fixed platform budgets for MBeans, attributes, metric families,
series, nesting, and execution time. These are not connector configuration. Slow or unsupported
MBeans must not block OpenTelemetry exporter callbacks. Budget exhaustion and read failures are
observable through `connect_jmx_collector_*` metrics, including reads, errors, timeouts, dropped
values, unsupported values, active series, and scan duration. A getter that ignores interruption
cannot be forcibly terminated safely; its execution remains confined to the bounded reader pool.

This source collects all domains, including JMX views of JVM and Kafka metrics that may also be
available through existing standard metric names. The `connect_jmx_` namespace keeps raw JMX
observations separate from those existing metrics.

## Verification

Focused JVM tests register previously unknown MBeans after startup and verify dynamic discovery,
value updates, structured values, removal, and bounded reads. Export tests compare real OPS JSON
and Prometheus HTTP samples produced from the same SDK. Enterprise tests exercise the existing
Remote Write HTTP endpoint and decode the resulting payload.

The system test
`ConnectAutomaticJmxTest.test_automatic_jmx_metrics` runs a test connector that registers a new
JMX domain from its task. It creates, deletes, and recreates connectors without changing worker
configuration, then checks that worker process IDs remain unchanged.
