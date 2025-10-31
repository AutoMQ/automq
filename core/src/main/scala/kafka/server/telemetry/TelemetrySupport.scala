package kafka.server.telemetry

import com.automq.opentelemetry.{AutoMQTelemetryManager, TelemetryConstants}
import com.automq.stream.s3.metrics.{Metrics, MetricsConfig, MetricsLevel, S3StreamMetricsManager}
import kafka.automq.table.metric.TableTopicMetricsManager
import kafka.server.KafkaConfig
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager
import org.slf4j.LoggerFactory

import io.opentelemetry.api.common.Attributes

import java.net.InetAddress
import java.util.{Locale, Properties}
import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._
import scala.util.Try

/**
 * Helper used by the core module to bootstrap AutoMQ telemetry using the
 * shared {@link com.automq.opentelemetry.AutoMQTelemetryManager} implementation.
 */
object TelemetrySupport {
  private val logger = LoggerFactory.getLogger(TelemetrySupport.getClass)
  private val CommonJmxPath = "/jmx/rules/common.yaml"
  private val BrokerJmxPath = "/jmx/rules/broker.yaml"
  private val ControllerJmxPath = "/jmx/rules/controller.yaml"
  private val KafkaMetricsPrefix = "kafka_stream_"

  def start(config: KafkaConfig, clusterId: String): AutoMQTelemetryManager = {
    val telemetryProps = buildTelemetryProperties(config, clusterId)
    val telemetryManager = new AutoMQTelemetryManager(telemetryProps)
    telemetryManager.init()
    telemetryManager.startYammerMetricsReporter(KafkaYammerMetrics.defaultRegistry())
    initializeMetrics(telemetryManager, config)
    telemetryManager
  }

  private def buildTelemetryProperties(config: KafkaConfig, clusterId: String): Properties = {
    val props = new Properties()
    config.originals().asScala.foreach { case (key, value) =>
      if (value != null) {
        props.setProperty(key, value.toString)
      }
    }

    putIfAbsent(props, TelemetryConstants.SERVICE_NAME_KEY, clusterId)
    putIfAbsent(props, TelemetryConstants.SERVICE_INSTANCE_ID_KEY, config.nodeId.toString)
    putIfAbsent(props, TelemetryConstants.EXPORTER_INTERVAL_MS_KEY, config.s3ExporterReportIntervalMs.toString)
    putIfAbsent(props, TelemetryConstants.S3_CLUSTER_ID_KEY, clusterId)
    putIfAbsent(props, TelemetryConstants.S3_NODE_ID_KEY, config.nodeId.toString)

    val exporterUri = Option(config.automq.metricsExporterURI()).getOrElse("")
    if (StringUtils.isNotBlank(exporterUri) && !props.containsKey(TelemetryConstants.EXPORTER_URI_KEY)) {
      props.setProperty(TelemetryConstants.EXPORTER_URI_KEY, exporterUri)
    }

    if (!props.containsKey(TelemetryConstants.JMX_CONFIG_PATH_KEY)) {
      props.setProperty(TelemetryConstants.JMX_CONFIG_PATH_KEY, buildJmxConfigPaths(config))
    }

    if (!props.containsKey(TelemetryConstants.TELEMETRY_METRICS_BASE_LABELS_CONFIG)) {
      val labels = Option(config.automq.baseLabels()).map(_.asScala.toSeq).getOrElse(Seq.empty)
      if (labels.nonEmpty) {
        val encoded = labels.map(pair => s"${pair.getLeft}=${pair.getRight}").mkString(",")
        props.setProperty(TelemetryConstants.TELEMETRY_METRICS_BASE_LABELS_CONFIG, encoded)
      }
    }

    if (!props.containsKey(TelemetryConstants.S3_BUCKET)) {
      val bucket = Option(config.automq.opsBuckets()).flatMap(_.asScala.headOption).map(_.toString)
      bucket.foreach(value => props.setProperty(TelemetryConstants.S3_BUCKET, value))
    }

    ensureSelectorProps(props, config, clusterId)
    props
  }

  private def initializeMetrics(manager: AutoMQTelemetryManager, config: KafkaConfig): Unit = {
    S3StreamKafkaMetricsManager.setTruststoreCertsSupplier(() => {
      try {
        val password = config.getPassword("ssl.truststore.certificates")
        if (password != null) password.value() else null
      } catch {
        case e: Exception =>
          logger.error("Failed to obtain truststore certificates", e)
          null
      }
    })

    S3StreamKafkaMetricsManager.setCertChainSupplier(() => {
      try {
        val password = config.getPassword("ssl.keystore.certificate.chain")
        if (password != null) password.value() else null
      } catch {
        case e: Exception =>
          logger.error("Failed to obtain certificate chain", e)
          null
      }
    })

    val meter = manager.getMeter
    val metricsLevel = parseMetricsLevel(config.s3MetricsLevel)
    val metricsIntervalMs = config.s3ExporterReportIntervalMs.toLong
    val metricsConfig = new MetricsConfig(metricsLevel, Attributes.empty(), metricsIntervalMs)

    Metrics.instance().setup(meter, metricsConfig)
    S3StreamMetricsManager.configure(new MetricsConfig(metricsLevel, Attributes.empty(), metricsIntervalMs))
    S3StreamMetricsManager.initMetrics(meter, KafkaMetricsPrefix)

    S3StreamKafkaMetricsManager.configure(new MetricsConfig(metricsLevel, Attributes.empty(), metricsIntervalMs))
    S3StreamKafkaMetricsManager.initMetrics(meter, KafkaMetricsPrefix)

    TableTopicMetricsManager.initMetrics(meter)
  }

  private def parseMetricsLevel(rawLevel: String): MetricsLevel = {
    if (StringUtils.isBlank(rawLevel)) {
      return MetricsLevel.INFO
    }
    try {
      MetricsLevel.valueOf(rawLevel.trim.toUpperCase(Locale.ENGLISH))
    } catch {
      case _: IllegalArgumentException =>
        logger.warn("Illegal metrics level '{}', defaulting to INFO", rawLevel)
        MetricsLevel.INFO
    }
  }

  private def buildJmxConfigPaths(config: KafkaConfig): String = {
    val paths = ListBuffer(CommonJmxPath)
    val roles = config.processRoles
    if (roles.contains(ProcessRole.BrokerRole)) {
      paths += BrokerJmxPath
    }
    if (roles.contains(ProcessRole.ControllerRole)) {
      paths += ControllerJmxPath
    }
    paths.mkString(",")
  }

  private def putIfAbsent(props: Properties, key: String, value: String): Unit = {
    if (!props.containsKey(key) && StringUtils.isNotBlank(value)) {
      props.setProperty(key, value)
    }
  }

  private def ensureSelectorProps(props: Properties, config: KafkaConfig, clusterId: String): Unit = {
    val bucket = props.getProperty(TelemetryConstants.S3_BUCKET)
    if (StringUtils.isBlank(bucket)) {
      return
    }

    val selectorTypeKey = s"${TelemetryConstants.S3_SELECTOR_TYPE_KEY}"
    if (!props.containsKey(selectorTypeKey)) {
      props.setProperty(selectorTypeKey, "controller")
    }

    val selectorType = props.getProperty(selectorTypeKey, "controller").toLowerCase(Locale.ROOT)
    if (selectorType == "kafka") {
      val bootstrapKey = s"automq.telemetry.s3.selector.kafka.bootstrap.servers"
      if (!props.containsKey(bootstrapKey)) {
        bootstrapServers(config).foreach(servers => props.setProperty(bootstrapKey, servers))
      }

      val normalizedCluster = Option(clusterId).filter(StringUtils.isNotBlank).getOrElse("default")
      val topicKey = s"automq.telemetry.s3.selector.kafka.topic"
      if (!props.containsKey(topicKey)) {
        props.setProperty(topicKey, s"__automq_telemetry_s3_leader_$normalizedCluster")
      }

      val groupKey = s"automq.telemetry.s3.selector.kafka.group.id"
      if (!props.containsKey(groupKey)) {
        props.setProperty(groupKey, s"automq-telemetry-s3-$normalizedCluster")
      }
    }
  }

  private def bootstrapServers(config: KafkaConfig): Option[String] = {
    val endpoints = {
      val advertised = config.effectiveAdvertisedBrokerListeners
      if (advertised.nonEmpty) advertised else config.listeners
    }

    val hosts = endpoints
      .map(ep => s"${resolveHost(ep.host)}:${ep.port}")
      .filter(_.nonEmpty)

    if (hosts.nonEmpty) Some(hosts.mkString(",")) else None
  }

  private def resolveHost(host: String): String = {
    val value = Option(host).filter(_.nonEmpty).getOrElse("localhost")
    if (value == "0.0.0.0") {
      Try(InetAddress.getLocalHost.getHostAddress).getOrElse("127.0.0.1")
    } else {
      value
    }
  }
}
