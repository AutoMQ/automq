package unit.kafka.server.streamaspect.extension

import kafka.network.RequestChannel
import kafka.server.RequestLocal
import kafka.server.streamaspect.extension.{BrokerExtensionHandle, BrokerExtensionHandleProvider, BrokerHandleOps}
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Test

class BrokerExtensionHandleContractsTest {

  @Test
  def shouldSupportDefaultInitHook(): Unit = {
    var initialized = false
    val handle = new BrokerExtensionHandle {
      override def init(ops: BrokerHandleOps): Unit = {
        initialized = true
      }

      override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
      }
    }

    handle.init(new BrokerHandleOps {
      override def forwardToControllerOrFail(request: RequestChannel.Request): Unit = ()
      override def maybeForward(request: RequestChannel.Request, handler: RequestChannel.Request => Unit,
        cb: Option[org.apache.kafka.common.requests.AbstractResponse] => Unit): Unit = ()
      override def sendForwardedResponse(request: RequestChannel.Request,
        response: org.apache.kafka.common.requests.AbstractResponse): Unit = ()
      override def sendResponseMaybeThrottle(request: RequestChannel.Request,
        responseBuilder: Int => org.apache.kafka.common.requests.AbstractResponse): Unit = ()
      override def handleError(request: RequestChannel.Request, t: Throwable): Unit = ()
      override def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit = ()
    })
    assert(initialized)
  }

  @Test
  def shouldAllowProviderToCreateHandle(): Unit = {
    val provider = new BrokerExtensionHandleProvider {
      override def create(): BrokerExtensionHandle = new BrokerExtensionHandle {
        override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = ()
      }
    }

    assertNotNull(provider.create())
  }
}
