package unit.kafka.server.streamaspect.extension

import kafka.network.RequestChannel
import kafka.server.RequestLocal
import kafka.server.streamaspect.extension.BrokerExtensionHandleDispatcher.{Handled, NotHandled}
import kafka.server.streamaspect.extension.{BrokerExtensionHandle, BrokerExtensionHandleDispatcher, BrokerExtensionHandleProvider, BrokerExtensionContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

class BrokerExtensionHandleDispatcherTest {

  @Test
  def shouldReturnNotHandledWithoutProvider(): Unit = {
    val dispatcher = BrokerExtensionHandleDispatcher.loadFromProviders(noopOps, Seq.empty)
    assertEquals(NotHandled, dispatcher.handle(null, null))
  }

  @Test
  def shouldInvokeProviderHandle(): Unit = {
    var initialized = false
    var handled = false

    val provider = new BrokerExtensionHandleProvider {
      override def create(): BrokerExtensionHandle = new BrokerExtensionHandle {
        override def init(ops: BrokerExtensionContext): Unit = {
          initialized = true
        }

        override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = {
          handled = true
        }
      }
    }

    val dispatcher = BrokerExtensionHandleDispatcher.loadFromProviders(noopOps, Seq(provider))
    assertEquals(Handled, dispatcher.handle(null, null))
    assertTrue(initialized)
    assertTrue(handled)
  }

  @Test
  def shouldRejectMultipleProviders(): Unit = {
    val providerOne = simpleProvider
    val providerTwo = simpleProvider

    assertThrows(classOf[IllegalStateException], () =>
      BrokerExtensionHandleDispatcher.loadFromProviders(noopOps, Seq(providerOne, providerTwo)))
  }

  private def simpleProvider: BrokerExtensionHandleProvider = new BrokerExtensionHandleProvider {
    override def create(): BrokerExtensionHandle = new BrokerExtensionHandle {
      override def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit = ()
    }
  }

  private def noopOps: BrokerExtensionContext = new BrokerExtensionContext {
    override def forwardToControllerOrFail(request: RequestChannel.Request): Unit = ()

    override def maybeForward(request: RequestChannel.Request,
      handler: RequestChannel.Request => Unit,
      responseCallback: Option[org.apache.kafka.common.requests.AbstractResponse] => Unit): Unit = ()

    override def sendForwardedResponse(request: RequestChannel.Request,
      response: org.apache.kafka.common.requests.AbstractResponse): Unit = ()

    override def sendResponseMaybeThrottle(request: RequestChannel.Request,
      responseBuilder: Int => org.apache.kafka.common.requests.AbstractResponse): Unit = ()

    override def handleError(request: RequestChannel.Request, t: Throwable): Unit = ()

    override def handleInvalidVersionsDuringForwarding(request: RequestChannel.Request): Unit = ()
  }
}
