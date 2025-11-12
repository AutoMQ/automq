package kafka.server.telemetry

import com.automq.opentelemetry.AutoMQTelemetryManager
import com.automq.stream.s3.metrics.{Metrics, MetricsConfig, MetricsLevel, S3StreamMetricsManager}
import io.opentelemetry.api.common.Attributes
import kafka.automq.table.metric.TableTopicMetricsManager
import kafka.server.KafkaConfig
import org.apache.commons.lang3.StringUtils
import org.apache.kafka.server.ProcessRole
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.metrics.s3stream.S3StreamKafkaMetricsManager
import org.slf4j.LoggerFactory

import java.util.Locale
import scala.collection.mutable.ListBuffer

/**
 * Helper used by the core module to bootstrap AutoMQ telemetry using the AutoMQTelemetryManager implement.
 */
object TelemetrySupport {
  private val logger = LoggerFactory.getLogger(TelemetrySupport.getClass)
  private val CommonJmxPath = "/jmx/rules/common.yaml"
  private val BrokerJmxPath = "/jmx/rules/broker.yaml"
  private val ControllerJmxPath = "/jmx/rules/controller.yaml"
  private val KafkaMetricsPrefix = "kafka_stream_"

  def start(config: KafkaConfig, clusterId: String): AutoMQTelemetryManager = {
    val telemetryManager = new AutoMQTelemetryManager(config.automq.metricsExporterURI(), clusterId, config.nodeId.toString, null)
    telemetryManager.setJmxConfigPaths(buildJmxConfigPaths(config))
    telemetryManager.init()
    telemetryManager.startYammerMetricsReporter(KafkaYammerMetrics.defaultRegistry())
    initializeMetrics(telemetryManager, config)
    telemetryManager
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
}
