/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.util
import java.util.concurrent.TimeUnit.{MILLISECONDS, NANOSECONDS}
import java.util.concurrent.CompletableFuture
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection
import org.apache.kafka.common.message.{BrokerHeartbeatRequestData, BrokerRegistrationRequestData}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerRegistrationRequest, BrokerRegistrationResponse}
import org.apache.kafka.metadata.{BrokerState, VersionRange}
import org.apache.kafka.queue.EventQueue.DeadlineFunction
import org.apache.kafka.common.utils.{ExponentialBackoff, LogContext, Time}
import org.apache.kafka.queue.{EventQueue, KafkaEventQueue}
import scala.jdk.CollectionConverters._


/**
 * The broker lifecycle manager owns the broker state.
 *
 * Its inputs are messages passed in from other parts of the broker and from the
 * controller: requests to start up, or shut down, for example. Its output are the broker
 * state and various futures that can be used to wait for broker state transitions to
 * occur.
 *
 * The lifecycle manager handles registering the broker with the controller, as described
 * in KIP-631. After registration is complete, it handles sending periodic broker
 * heartbeats and processing the responses.
 *
 * This code uses an event queue paradigm. Modifications get translated into events, which
 * are placed on the queue to be processed sequentially. As described in the JavaDoc for
 * each variable, most mutable state can be accessed only from that event queue thread.
 * In some cases we expose a volatile variable which can be read from any thread, but only
 * written from the event queue thread.
 */
class BrokerLifecycleManager(val config: KafkaConfig,
                             val time: Time,
                             val threadNamePrefix: Option[String]) extends Logging {
  val logContext = new LogContext(s"[BrokerLifecycleManager id=${config.nodeId}] ")

  this.logIdent = logContext.logPrefix()

  /**
   * The broker id.
   */
  private val nodeId = config.nodeId

  /**
   * The broker rack, or null if there is no configured rack.
   */
  private val rack = config.rack

  /**
   * How long to wait for registration to succeed before failing the startup process.
   */
  private val initialTimeoutNs =
    MILLISECONDS.toNanos(config.initialRegistrationTimeoutMs.longValue())

  /**
   * The exponential backoff to use for resending communication.
   */
  private val resendExponentialBackoff =
    new ExponentialBackoff(100, 2, config.brokerSessionTimeoutMs.toLong, 0.02)

  /**
   * The number of times we've tried and failed to communicate.  This variable can only be
   * read or written from the event queue thread.
   */
  private var failedAttempts = 0L

  /**
   * The broker incarnation ID.  This ID uniquely identifies each time we start the broker
   */
  val incarnationId = Uuid.randomUuid()

  /**
   * A future which is completed just as soon as the broker has caught up with the latest
   * metadata offset for the first time.
   */
  val initialCatchUpFuture = new CompletableFuture[Void]()

  /**
   * A future which is completed when controlled shutdown is done.
   */
  val controlledShutdownFuture = new CompletableFuture[Void]()

  /**
   * The broker epoch, or -1 if the broker has not yet registered.  This variable can only
   * be written from the event queue thread.
   */
  @volatile private var _brokerEpoch = -1L

  /**
   * The current broker state.  This variable can only be written from the event queue
   * thread.
   */
  @volatile private var _state = BrokerState.NOT_RUNNING

  /**
   * A thread-safe callback function which gives this manager the current highest metadata
   * offset.  This variable can only be read or written from the event queue thread.
   */
  private var _highestMetadataOffsetProvider: () => Long = _

  /**
   * True only if we are ready to unfence the broker.  This variable can only be read or
   * written from the event queue thread.
   */
  private var readyToUnfence = false

  /**
   * True if we sent a event queue to the active controller requesting controlled
   * shutdown.  This variable can only be read or written from the event queue thread.
   */
  private var gotControlledShutdownResponse = false

  /**
   * Whether or not this broker is registered with the controller quorum.
   * This variable can only be read or written from the event queue thread.
   */
  private var registered = false

  /**
   * True if the initial registration succeeded.  This variable can only be read or
   * written from the event queue thread.
   */
  private var initialRegistrationSucceeded = false

  /**
   * The cluster ID, or null if this manager has not been started yet.  This variable can
   * only be read or written from the event queue thread.
   */
  private var _clusterId: String = _

  /**
   * The listeners which this broker advertises.  This variable can only be read or
   * written from the event queue thread.
   */
  private var _advertisedListeners: ListenerCollection = _

  /**
   * The features supported by this broker.  This variable can only be read or written
   * from the event queue thread.
   */
  private var _supportedFeatures: util.Map[String, VersionRange] = _

  /**
   * The channel manager, or null if this manager has not been started yet.  This variable
   * can only be read or written from the event queue thread.
   */
  private var _channelManager: BrokerToControllerChannelManager = _

  /**
   * The event queue.
   */
  private[server] val eventQueue = new KafkaEventQueue(time, logContext, threadNamePrefix.getOrElse(""))

  /**
   * Start the BrokerLifecycleManager.
   *
   * @param highestMetadataOffsetProvider Provides the current highest metadata offset.
   * @param channelManager                The brokerToControllerChannelManager to use.
   * @param clusterId                     The cluster ID.
   */
  def start(highestMetadataOffsetProvider: () => Long,
            channelManager: BrokerToControllerChannelManager,
            clusterId: String,
            advertisedListeners: ListenerCollection,
            supportedFeatures: util.Map[String, VersionRange]): Unit = {
    eventQueue.append(new StartupEvent(highestMetadataOffsetProvider,
      channelManager, clusterId, advertisedListeners, supportedFeatures))
  }

  def setReadyToUnfence(): Unit = {
    eventQueue.append(new SetReadyToUnfenceEvent())
  }

  def brokerEpoch: Long = _brokerEpoch

  def state: BrokerState = _state

  private class BeginControlledShutdownEvent extends EventQueue.Event {
    override def run(): Unit = {
      _state match {
        case BrokerState.PENDING_CONTROLLED_SHUTDOWN =>
          info("Attempted to enter pending controlled shutdown state, but we are " +
          "already in that state.")
        case BrokerState.RUNNING =>
          info("Beginning controlled shutdown.")
          _state = BrokerState.PENDING_CONTROLLED_SHUTDOWN
          // Send the next heartbeat immediately in order to let the controller
          // begin processing the controlled shutdown as soon as possible.
          scheduleNextCommunicationImmediately()

        case _ =>
          info(s"Skipping controlled shutdown because we are in state ${_state}.")
          beginShutdown()
      }
    }
  }

  /**
   * Enter the controlled shutdown state if we are in RUNNING state.
   * Or, if we're not running, shut down immediately.
   */
  def beginControlledShutdown(): Unit = {
    eventQueue.append(new BeginControlledShutdownEvent())
  }

  /**
   * Start shutting down the BrokerLifecycleManager, but do not block.
   */
  def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown", new ShutdownEvent())
  }

  /**
   * Shut down the BrokerLifecycleManager and block until all threads are joined.
   */
  def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }

  private class SetReadyToUnfenceEvent() extends EventQueue.Event {
    override def run(): Unit = {
      readyToUnfence = true
      scheduleNextCommunicationImmediately()
    }
  }

  private class StartupEvent(highestMetadataOffsetProvider: () => Long,
                     channelManager: BrokerToControllerChannelManager,
                     clusterId: String,
                     advertisedListeners: ListenerCollection,
                     supportedFeatures: util.Map[String, VersionRange]) extends EventQueue.Event {
    override def run(): Unit = {
      _highestMetadataOffsetProvider = highestMetadataOffsetProvider
      _channelManager = channelManager
      _channelManager.start()
      _state = BrokerState.STARTING
      _clusterId = clusterId
      _advertisedListeners = advertisedListeners.duplicate()
      _supportedFeatures = new util.HashMap[String, VersionRange](supportedFeatures)
      eventQueue.scheduleDeferred("initialRegistrationTimeout",
        new DeadlineFunction(time.nanoseconds() + initialTimeoutNs),
        new RegistrationTimeoutEvent())
      sendBrokerRegistration()
      info(s"Incarnation ${incarnationId} of broker ${nodeId} in cluster ${clusterId} " +
        "is now STARTING.")
    }
  }

  private def sendBrokerRegistration(): Unit = {
    val features = new BrokerRegistrationRequestData.FeatureCollection()
    _supportedFeatures.asScala.foreach {
      case (name, range) => features.add(new BrokerRegistrationRequestData.Feature().
        setName(name).
        setMinSupportedVersion(range.min()).
        setMaxSupportedVersion(range.max()))
    }
    val data = new BrokerRegistrationRequestData().
        setBrokerId(nodeId).
        setClusterId(_clusterId).
        setFeatures(features).
        setIncarnationId(incarnationId).
        setListeners(_advertisedListeners).
        setRack(rack.orNull)
    if (isDebugEnabled) {
      debug(s"Sending broker registration ${data}")
    }
    _channelManager.sendRequest(new BrokerRegistrationRequest.Builder(data),
      new BrokerRegistrationResponseHandler())
  }

  private class BrokerRegistrationResponseHandler extends ControllerRequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      if (response.authenticationException() != null) {
        error(s"Unable to register broker ${nodeId} because of an authentication exception.",
          response.authenticationException());
        scheduleNextCommunicationAfterFailure()
      } else if (response.versionMismatch() != null) {
        error(s"Unable to register broker ${nodeId} because of an API version problem.",
          response.versionMismatch());
        scheduleNextCommunicationAfterFailure()
      } else if (response.responseBody() == null) {
        warn(s"Unable to register broker ${nodeId}.")
        scheduleNextCommunicationAfterFailure()
      } else if (!response.responseBody().isInstanceOf[BrokerRegistrationResponse]) {
        error(s"Unable to register broker ${nodeId} because the controller returned an " +
          "invalid response type.")
        scheduleNextCommunicationAfterFailure()
      } else {
        val message = response.responseBody().asInstanceOf[BrokerRegistrationResponse]
        val errorCode = Errors.forCode(message.data().errorCode())
        if (errorCode == Errors.NONE) {
          failedAttempts = 0
          _brokerEpoch = message.data().brokerEpoch()
          registered = true
          initialRegistrationSucceeded = true
          info(s"Successfully registered broker ${nodeId} with broker epoch ${_brokerEpoch}")
          scheduleNextCommunicationImmediately() // Immediately send a heartbeat
        } else {
          info(s"Unable to register broker ${nodeId} because the controller returned " +
            s"error ${errorCode}")
          scheduleNextCommunicationAfterFailure()
        }
      }
    }

    override def onTimeout(): Unit = {
      info(s"Unable to register the broker because the RPC got timed out before it could be sent.")
      scheduleNextCommunicationAfterFailure()
    }
  }

  private def sendBrokerHeartbeat(): Unit = {
    val metadataOffset = _highestMetadataOffsetProvider()
    val data = new BrokerHeartbeatRequestData().
      setBrokerEpoch(_brokerEpoch).
      setBrokerId(nodeId).
      setCurrentMetadataOffset(metadataOffset).
      setWantFence(!readyToUnfence).
      setWantShutDown(_state == BrokerState.PENDING_CONTROLLED_SHUTDOWN)
    if (isTraceEnabled) {
      trace(s"Sending broker heartbeat ${data}")
    }
    _channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data),
      new BrokerHeartbeatResponseHandler())
  }

  private class BrokerHeartbeatResponseHandler extends ControllerRequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      if (response.authenticationException() != null) {
        error(s"Unable to send broker heartbeat for ${nodeId} because of an " +
          "authentication exception.", response.authenticationException());
        scheduleNextCommunicationAfterFailure()
      } else if (response.versionMismatch() != null) {
        error(s"Unable to send broker heartbeat for ${nodeId} because of an API " +
          "version problem.", response.versionMismatch());
        scheduleNextCommunicationAfterFailure()
      } else if (response.responseBody() == null) {
        warn(s"Unable to send broker heartbeat for ${nodeId}. Retrying.")
        scheduleNextCommunicationAfterFailure()
      } else if (!response.responseBody().isInstanceOf[BrokerHeartbeatResponse]) {
        error(s"Unable to send broker heartbeat for ${nodeId} because the controller " +
          "returned an invalid response type.")
        scheduleNextCommunicationAfterFailure()
      } else {
        val message = response.responseBody().asInstanceOf[BrokerHeartbeatResponse]
        val errorCode = Errors.forCode(message.data().errorCode())
        if (errorCode == Errors.NONE) {
          failedAttempts = 0
          _state match {
            case BrokerState.STARTING =>
              if (message.data().isCaughtUp()) {
                info(s"The broker has caught up. Transitioning from STARTING to RECOVERY.")
                _state = BrokerState.RECOVERY
                initialCatchUpFuture.complete(null)
              } else {
                debug(s"The broker is STARTING. Still waiting to catch up with cluster metadata.")
              }
              // Schedule the heartbeat after only 10 ms so that in the case where
              // there is no recovery work to be done, we start up a bit quicker.
              scheduleNextCommunication(NANOSECONDS.convert(10, MILLISECONDS))
            case BrokerState.RECOVERY =>
              if (!message.data().isFenced()) {
                info(s"The broker has been unfenced. Transitioning from RECOVERY to RUNNING.")
                _state = BrokerState.RUNNING
              } else {
                info(s"The broker is in RECOVERY.")
              }
              scheduleNextCommunicationAfterSuccess()
            case BrokerState.RUNNING =>
              debug(s"The broker is RUNNING. Processing heartbeat response.")
              scheduleNextCommunicationAfterSuccess()
            case BrokerState.PENDING_CONTROLLED_SHUTDOWN =>
              if (!message.data().shouldShutDown()) {
                info(s"The broker is in PENDING_CONTROLLED_SHUTDOWN state, still waiting " +
                  "for the active controller.")
                if (!gotControlledShutdownResponse) {
                  // If this is the first pending controlled shutdown response we got,
                  // schedule our next heartbeat a little bit sooner than we usually would.
                  // In the case where controlled shutdown completes quickly, this will
                  // speed things up a little bit.
                  scheduleNextCommunication(NANOSECONDS.convert(50, MILLISECONDS))
                } else {
                  scheduleNextCommunicationAfterSuccess()
                }
              } else {
                info(s"The controller has asked us to exit controlled shutdown.")
                beginShutdown()
              }
              gotControlledShutdownResponse = true
            case BrokerState.SHUTTING_DOWN =>
              info(s"The broker is SHUTTING_DOWN. Ignoring heartbeat response.")
            case _ =>
              error(s"Unexpected broker state ${_state}")
              scheduleNextCommunicationAfterSuccess()
          }
        } else {
          warn(s"Broker ${nodeId} sent a heartbeat request but received error ${errorCode}.")
          scheduleNextCommunicationAfterFailure()
        }
      }
    }

    override def onTimeout(): Unit = {
      info("Unable to send a heartbeat because the RPC got timed out before it could be sent.")
      scheduleNextCommunicationAfterFailure()
    }
  }

  private def scheduleNextCommunicationImmediately(): Unit = scheduleNextCommunication(0)

  private def scheduleNextCommunicationAfterFailure(): Unit = {
    val delayMs = resendExponentialBackoff.backoff(failedAttempts)
    failedAttempts = failedAttempts + 1
    scheduleNextCommunication(NANOSECONDS.convert(delayMs, MILLISECONDS))
  }

  private def scheduleNextCommunicationAfterSuccess(): Unit = {
    scheduleNextCommunication(NANOSECONDS.convert(
      config.brokerHeartbeatIntervalMs.longValue() , MILLISECONDS))
  }

  private def scheduleNextCommunication(intervalNs: Long): Unit = {
    trace(s"Scheduling next communication at ${MILLISECONDS.convert(intervalNs, NANOSECONDS)} " +
      "ms from now.")
    val deadlineNs = time.nanoseconds() + intervalNs
    eventQueue.scheduleDeferred("communication",
      new DeadlineFunction(deadlineNs),
      new CommunicationEvent())
  }

  private class RegistrationTimeoutEvent extends EventQueue.Event {
    override def run(): Unit = {
      if (!initialRegistrationSucceeded) {
        error("Shutting down because we were unable to register with the controller quorum.")
        eventQueue.beginShutdown("registrationTimeout", new ShutdownEvent())
      }
    }
  }

  private class CommunicationEvent extends EventQueue.Event {
    override def run(): Unit = {
      if (registered) {
        sendBrokerHeartbeat()
      } else {
        sendBrokerRegistration()
      }
    }
  }

  private class ShutdownEvent extends EventQueue.Event {
    override def run(): Unit = {
      info(s"Transitioning from ${_state} to ${BrokerState.SHUTTING_DOWN}.")
      _state = BrokerState.SHUTTING_DOWN
      controlledShutdownFuture.complete(null)
      initialCatchUpFuture.cancel(false)
      if (_channelManager != null) {
        _channelManager.shutdown()
        _channelManager = null
      }
    }
  }
}
