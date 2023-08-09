/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.utils

import java.io._
import java.nio._
import java.util.concurrent.locks.{Lock, ReadWriteLock}
import java.lang.management._
import java.util.{Base64, Properties, UUID}
import com.typesafe.scalalogging.Logger

import javax.management._
import scala.collection._
import scala.collection.{Seq, mutable}
import kafka.cluster.EndPoint
import org.apache.commons.validator.routines.InetAddressValidator
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.Utils
import org.slf4j.event.Level

import java.util
import scala.annotation.nowarn
import scala.jdk.CollectionConverters._

/**
 * General helper functions!
 *
 * This is for general helper functions that aren't specific to Kafka logic. Things that should have been included in
 * the standard library etc.
 *
 * If you are making a new helper function and want to add it to this class please ensure the following:
 * 1. It has documentation
 * 2. It is the most general possible utility, not just the thing you needed in one particular place
 * 3. You have tests for it if it is nontrivial in any way
 */
object CoreUtils {
  private val logger = Logger(getClass)

  private val inetAddressValidator = InetAddressValidator.getInstance()

  /**
    * Do the given action and log any exceptions thrown without rethrowing them.
    *
    * @param action The action to execute.
    * @param logging The logging instance to use for logging the thrown exception.
    * @param logLevel The log level to use for logging.
    */
  def swallow(action: => Unit, logging: Logging, logLevel: Level = Level.WARN): Unit = {
    try {
      action
    } catch {
      case e: Throwable => logLevel match {
        case Level.ERROR => logger.error(e.getMessage, e)
        case Level.WARN => logger.warn(e.getMessage, e)
        case Level.INFO => logger.info(e.getMessage, e)
        case Level.DEBUG => logger.debug(e.getMessage, e)
        case Level.TRACE => logger.trace(e.getMessage, e)
      }
    }
  }

  /**
   * Recursively delete the list of files/directories and any subfiles (if any exist)
   * @param files sequence of files to be deleted
   */
  def delete(files: Seq[String]): Unit = files.foreach(f => Utils.delete(new File(f)))

  /**
   * Invokes every function in `all` even if one or more functions throws an exception.
   *
   * If any of the functions throws an exception, the first one will be rethrown at the end with subsequent exceptions
   * added as suppressed exceptions.
   */
  // Note that this is a generalised version of `Utils.closeAll`. We could potentially make it more general by
  // changing the signature to `def tryAll[R](all: Seq[() => R]): Seq[R]`
  def tryAll(all: Seq[() => Unit]): Unit = {
    var exception: Throwable = null
    all.foreach { element =>
      try element.apply()
      catch {
        case e: Throwable =>
          if (exception != null)
            exception.addSuppressed(e)
          else
            exception = e
      }
    }
    if (exception != null)
      throw exception
  }

  /**
   * Register the given mbean with the platform mbean server,
   * unregistering any mbean that was there before. Note,
   * this method will not throw an exception if the registration
   * fails (since there is nothing you can do and it isn't fatal),
   * instead it just returns false indicating the registration failed.
   * @param mbean The object to register as an mbean
   * @param name The name to register this mbean with
   * @return true if the registration succeeded
   */
  def registerMBean(mbean: Object, name: String): Boolean = {
    try {
      val mbs = ManagementFactory.getPlatformMBeanServer()
      mbs synchronized {
        val objName = new ObjectName(name)
        if (mbs.isRegistered(objName))
          mbs.unregisterMBean(objName)
        mbs.registerMBean(mbean, objName)
        true
      }
    } catch {
      case e: Exception =>
        logger.error(s"Failed to register Mbean $name", e)
        false
    }
  }

  /**
   * This method gets comma separated values which contains key,value pairs and returns a map of
   * key value pairs. the format of allCSVal is key1:val1, key2:val2 ....
   * Also supports strings with multiple ":" such as IpV6 addresses, taking the last occurrence
   * of the ":" in the pair as the split, eg a:b:c:val1, d:e:f:val2 => a:b:c -> val1, d:e:f -> val2
   */
  def parseCsvMap(str: String): Map[String, String] = {
    val map = new mutable.HashMap[String, String]
    if ("".equals(str))
      return map
    val keyVals = str.split("\\s*,\\s*").map(s => {
      val lio = s.lastIndexOf(":")
      (s.substring(0,lio).trim, s.substring(lio + 1).trim)
    })
    keyVals.toMap
  }

  /**
   * Parse a comma separated string into a sequence of strings.
   * Whitespace surrounding the comma will be removed.
   */
  def parseCsvList(csvList: String): Seq[String] = {
    if (csvList == null || csvList.isEmpty)
      Seq.empty[String]
    else
      csvList.split("\\s*,\\s*").filter(v => !v.equals(""))
  }

  /**
   * Create an instance of the class with the given class name
   */
  def createObject[T <: AnyRef](className: String, args: AnyRef*): T = {
    val klass = Class.forName(className, true, Utils.getContextOrKafkaClassLoader()).asInstanceOf[Class[T]]
    val constructor = klass.getConstructor(args.map(_.getClass): _*)
    constructor.newInstance(args: _*)
  }

  /**
   * Execute the given function inside the lock
   */
  def inLock[T](lock: Lock)(fun: => T): T = {
    lock.lock()
    try {
      fun
    } finally {
      lock.unlock()
    }
  }

  def inReadLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.readLock)(fun)

  def inWriteLock[T](lock: ReadWriteLock)(fun: => T): T = inLock[T](lock.writeLock)(fun)

  /**
   * Returns a list of duplicated items
   */
  def duplicates[T](s: Iterable[T]): Iterable[T] = {
    s.groupBy(identity)
      .map { case (k, l) => (k, l.size)}
      .filter { case (_, l) => l > 1 }
      .keys
  }

  def listenerListToEndPoints(listeners: String, securityProtocolMap: Map[ListenerName, SecurityProtocol]): Seq[EndPoint] = {
    listenerListToEndPoints(listeners, securityProtocolMap, true)
  }

  def checkDuplicateListenerPorts(endpoints: Seq[EndPoint], listeners: String): Unit = {
    val distinctPorts = endpoints.map(_.port).distinct
    require(distinctPorts.size == endpoints.map(_.port).size, s"Each listener must have a different port, listeners: $listeners")
  }

  def listenerListToEndPoints(listeners: String, securityProtocolMap: Map[ListenerName, SecurityProtocol], requireDistinctPorts: Boolean): Seq[EndPoint] = {
    def validateOneIsIpv4AndOtherIpv6(first: String, second: String): Boolean =
      (inetAddressValidator.isValidInet4Address(first) && inetAddressValidator.isValidInet6Address(second)) ||
        (inetAddressValidator.isValidInet6Address(first) && inetAddressValidator.isValidInet4Address(second))

    def validate(endPoints: Seq[EndPoint]): Unit = {
      val distinctListenerNames = endPoints.map(_.listenerName).distinct
      require(distinctListenerNames.size == endPoints.size, s"Each listener must have a different name, listeners: $listeners")

      val (duplicatePorts, _) = endPoints.filter {
        // filter port 0 for unit tests
        ep => ep.port != 0
      }.groupBy(_.port).partition {
        case (_, endpoints) => endpoints.size > 1
      }

      // Exception case, let's allow duplicate ports if one host is on IPv4 and the other one is on IPv6
      val duplicatePortsPartitionedByValidIps = duplicatePorts.map {
        case (port, eps) =>
          (port, eps.partition(ep =>
            ep.host != null && inetAddressValidator.isValid(ep.host)
          ))
      }

      // Iterate through every grouping of duplicates by port to see if they are valid
      duplicatePortsPartitionedByValidIps.foreach {
        case (port, (duplicatesWithIpHosts, duplicatesWithoutIpHosts)) =>
          if (requireDistinctPorts)
            checkDuplicateListenerPorts(duplicatesWithoutIpHosts, listeners)

          duplicatesWithIpHosts match {
            case eps if eps.isEmpty =>
            case Seq(ep1, ep2) =>
              if (requireDistinctPorts) {
                val errorMessage = "If you have two listeners on " +
                  s"the same port then one needs to be IPv4 and the other IPv6, listeners: $listeners, port: $port"
                require(validateOneIsIpv4AndOtherIpv6(ep1.host, ep2.host), errorMessage)

                // If we reach this point it means that even though duplicatesWithIpHosts in isolation can be valid, if
                // there happens to be ANOTHER listener on this port without an IP host (such as a null host) then its
                // not valid.
                if (duplicatesWithoutIpHosts.nonEmpty)
                  throw new IllegalArgumentException(errorMessage)
              }
            case _ =>
              // Having more than 2 duplicate endpoints doesn't make sense since we only have 2 IP stacks (one is IPv4
              // and the other is IPv6)
              if (requireDistinctPorts)
                throw new IllegalArgumentException("Each listener must have a different port unless exactly one listener has " +
                  s"an IPv4 address and the other IPv6 address, listeners: $listeners, port: $port")
          }
      }
    }

    val endPoints = try {
      val listenerList = parseCsvList(listeners)
      listenerList.map(EndPoint.createEndPoint(_, Some(securityProtocolMap)))
    } catch {
      case e: Exception =>
        throw new IllegalArgumentException(s"Error creating broker listeners from '$listeners': ${e.getMessage}", e)
    }
    validate(endPoints)
    endPoints
  }

  def generateUuidAsBase64(): String = {
    val uuid = UUID.randomUUID()
    Base64.getUrlEncoder.withoutPadding.encodeToString(getBytesFromUuid(uuid))
  }

  def getBytesFromUuid(uuid: UUID): Array[Byte] = {
    // Extract bytes for uuid which is 128 bits (or 16 bytes) long.
    val uuidBytes = ByteBuffer.wrap(new Array[Byte](16))
    uuidBytes.putLong(uuid.getMostSignificantBits)
    uuidBytes.putLong(uuid.getLeastSignificantBits)
    uuidBytes.array
  }

  def propsWith(key: String, value: String): Properties = {
    propsWith((key, value))
  }

  def propsWith(props: (String, String)*): Properties = {
    val properties = new Properties()
    props.foreach { case (k, v) => properties.put(k, v) }
    properties
  }

  /**
   * Atomic `getOrElseUpdate` for concurrent maps. This is optimized for the case where
   * keys often exist in the map, avoiding the need to create a new value. `createValue`
   * may be invoked more than once if multiple threads attempt to insert a key at the same
   * time, but the same inserted value will be returned to all threads.
   *
   * In Scala 2.12, `ConcurrentMap.getOrElse` has the same behaviour as this method, but JConcurrentMapWrapper that
   * wraps Java maps does not.
   */
  def atomicGetOrUpdate[K, V](map: concurrent.Map[K, V], key: K, createValue: => V): V = {
    map.get(key) match {
      case Some(value) => value
      case None =>
        val value = createValue
        map.putIfAbsent(key, value).getOrElse(value)
    }
  }

  @nowarn("cat=unused") // see below for explanation
  def groupMapReduce[T, K, B](elements: Iterable[T])(key: T => K)(f: T => B)(reduce: (B, B) => B): Map[K, B] = {
    // required for Scala 2.12 compatibility, unused in Scala 2.13 and hence we need to suppress the unused warning
    import scala.collection.compat._
    elements.groupMapReduce(key)(f)(reduce)
  }

  def replicaToBrokerAssignmentAsScala(map: util.Map[Integer, util.List[Integer]]): Map[Int, Seq[Int]] = {
    map.asScala.map(e => (e._1.asInstanceOf[Int], e._2.asScala.map(_.asInstanceOf[Int])))
  }
}
