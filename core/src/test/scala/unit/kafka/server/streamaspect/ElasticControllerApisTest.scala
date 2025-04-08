package unit.kafka.server.streamaspect

import kafka.server.streamaspect.ElasticControllerApis
import kafka.server.{ControllerApisTest, KafkaConfig, SimpleApiVersionManager}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.controller.Controller
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{FinalizedFeatures, MetadataVersion}
import org.apache.kafka.server.config.KRaftConfigs
import org.junit.jupiter.api.{Tag, Timeout}

import java.util.Properties

@Timeout(60)
@Tag("S3Unit")
class ElasticControllerApisTest extends ControllerApisTest {

  override def createControllerApis(authorizer: Option[Authorizer],
    controller: Controller,
    props: Properties = new Properties(),
    throttle: Boolean = false): ElasticControllerApis = {

    props.put(KRaftConfigs.NODE_ID_CONFIG, nodeId: java.lang.Integer)
    props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "PLAINTEXT")
    props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:9092")
    new ElasticControllerApis(
      requestChannel,
      authorizer,
      if (throttle) quotasAlwaysThrottleControllerMutations else quotasNeverThrottleControllerMutations,
      time,
      controller,
      raftManager,
      new KafkaConfig(props),
      "JgxuGe9URy-E-ceaL04lEw",
      new ControllerRegistrationsPublisher(),
      new SimpleApiVersionManager(
        ListenerType.CONTROLLER,
        true,
        false,
        () => FinalizedFeatures.fromKRaftVersion(MetadataVersion.latestTesting())),
      metadataCache
    )
  }
}
