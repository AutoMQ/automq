package unit.kafka.server.streamaspect

import kafka.server.streamaspect.ElasticControllerApis
import kafka.server.{ControllerApisTest, KafkaConfig, SimpleApiVersionManager}
import org.apache.kafka.common.message.ApiMessageType.ListenerType
import org.apache.kafka.controller.Controller
import org.apache.kafka.image.publisher.ControllerRegistrationsPublisher
import org.apache.kafka.server.authorizer.Authorizer
import org.apache.kafka.server.common.{Features, MetadataVersion}
import org.junit.jupiter.api.Tag

import java.util.Properties

@Tag("S3Unit")
class ElasticControllerApisTest extends ControllerApisTest {

  override def createControllerApis(authorizer: Option[Authorizer],
    controller: Controller,
    props: Properties = new Properties(),
    throttle: Boolean = false): ElasticControllerApis = {

    props.put(KafkaConfig.NodeIdProp, nodeId: java.lang.Integer)
    props.put(KafkaConfig.ProcessRolesProp, "controller")
    props.put(KafkaConfig.ControllerListenerNamesProp, "PLAINTEXT")
    props.put(KafkaConfig.QuorumVotersProp, s"$nodeId@localhost:9092")
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
        () => Features.fromKRaftVersion(MetadataVersion.latestTesting())),
      metadataCache
    )
  }
}
