package kafka.server.log

import com.automq.log.S3RollingFileAppender
import com.automq.log.uploader.{DefaultS3LogConfig, LogConfigConstants, S3LogConfig, S3LogConfigProvider}
import kafka.server.KafkaConfig
import org.apache.commons.lang3.StringUtils
import org.slf4j.LoggerFactory

import java.util.Properties
import scala.jdk.CollectionConverters._

/**
 * Bridges Kafka configuration to the automq-log-uploader module by providing
 * an {{@link S3LogConfig}} backed by {{@link KafkaConfig}} settings.
 */
class CoreS3LogConfigProvider(kafkaConfig: KafkaConfig) extends S3LogConfigProvider {
  private val logger = LoggerFactory.getLogger(classOf[CoreS3LogConfigProvider])

  private val baseProps: Properties = extractBaseProps()

  @volatile private var clusterId: String = _
  @volatile private var activeConfig: DefaultS3LogConfig = _

  override def get(): S3LogConfig = {
    if (!isEnabled) {
      return null
    }

    val currentClusterId = clusterId
    if (currentClusterId == null) {
      logger.debug("Cluster id not yet available, postponing log uploader initialization")
      return null
    }

    var config = activeConfig
    if (config == null) synchronized {
      config = activeConfig
      if (config == null) {
        buildEffectiveConfig(currentClusterId) match {
          case Some(built) =>
            activeConfig = built
            config = built
          case None =>
            return null
        }
      }
    }
    config
  }

  def updateRuntimeContext(newClusterId: String): Unit = synchronized {
    if (StringUtils.isBlank(newClusterId)) {
      logger.warn("Received blank cluster id when updating log uploader context; ignoring")
      return
    }
    if (!StringUtils.equals(this.clusterId, newClusterId)) {
      this.clusterId = newClusterId
      this.activeConfig = null
      logger.info("Updated log uploader provider with clusterId={} nodeId={}", newClusterId, kafkaConfig.nodeId)
      S3RollingFileAppender.triggerInitialization()
    }
  }

  private def isEnabled: Boolean = {
    java.lang.Boolean.parseBoolean(baseProps.getProperty(LogConfigConstants.LOG_S3_ENABLE_KEY, kafkaConfig.s3OpsTelemetryEnabled.toString))
  }

  private def buildEffectiveConfig(currentClusterId: String): Option[DefaultS3LogConfig] = {
    val bucket = baseProps.getProperty(LogConfigConstants.LOG_S3_BUCKET_KEY)
    if (StringUtils.isBlank(bucket)) {
      logger.warn("log.s3.bucket is not configured; disabling S3 log uploader")
      return None
    }

    val props = new Properties()
    props.putAll(baseProps)
    if (!props.containsKey(LogConfigConstants.LOG_S3_CLUSTER_ID_KEY)) {
      props.setProperty(LogConfigConstants.LOG_S3_CLUSTER_ID_KEY, currentClusterId)
      logger.info(s"Setting missing property ${LogConfigConstants.LOG_S3_CLUSTER_ID_KEY} to $currentClusterId")
    }
    props.setProperty(LogConfigConstants.LOG_S3_NODE_ID_KEY, kafkaConfig.nodeId.toString)

    ensureSelectorProps(props, currentClusterId)
    Some(new DefaultS3LogConfig(props))
  }

  private def extractBaseProps(): Properties = {
    val props = new Properties()
    val originals = kafkaConfig.originals()
    originals.asScala.foreach { case (key, value) =>
      if (key.startsWith("log.s3.")) {
        props.setProperty(key, value.toString)
      } else if (key.startsWith("automq.log.s3.")) {
        val suffix = key.substring("automq.log.s3.".length)
        props.setProperty(s"log.s3.$suffix", value.toString)
      }
    }

    if (!props.containsKey(LogConfigConstants.LOG_S3_ENABLE_KEY)) {
      props.setProperty(LogConfigConstants.LOG_S3_ENABLE_KEY, kafkaConfig.s3OpsTelemetryEnabled.toString)
    }

    if (!props.containsKey(LogConfigConstants.LOG_S3_BUCKET_KEY)) {
      val opsBuckets = Option(kafkaConfig.automq.opsBuckets())
      val bucketUri = opsBuckets.toSeq.flatMap(_.asScala).headOption
      bucketUri.foreach(uri => props.setProperty(LogConfigConstants.LOG_S3_BUCKET_KEY, uri.toString))
    }

    props.setProperty(LogConfigConstants.LOG_S3_NODE_ID_KEY, kafkaConfig.nodeId.toString)

    props
  }

  private def ensureSelectorProps(props: Properties, clusterId: String): Unit = {
    if (!props.containsKey(LogConfigConstants.LOG_S3_SELECTOR_TYPE_KEY)) {
      props.setProperty(LogConfigConstants.LOG_S3_SELECTOR_TYPE_KEY, "controller")
    }
  }
}
