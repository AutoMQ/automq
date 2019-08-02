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

import java.util
import java.util.Locale

import org.apache.log4j.{Level, LogManager, Logger}

import scala.collection.mutable
import scala.collection.JavaConverters._


object Log4jController {
  val ROOT_LOGGER = "root"

  /**
    * Returns a map of the log4j loggers and their assigned log level.
    * If a logger does not have a log level assigned, we return the root logger's log level
    */
  def loggers: mutable.Map[String, String] = {
    val logs = new mutable.HashMap[String, String]()
    val rootLoggerLvl = existingLogger(ROOT_LOGGER).getLevel.toString
    logs.put(ROOT_LOGGER, rootLoggerLvl)

    val loggers = LogManager.getCurrentLoggers
    while (loggers.hasMoreElements) {
      val logger = loggers.nextElement().asInstanceOf[Logger]
      if (logger != null) {
        val level = if (logger.getLevel != null) logger.getLevel.toString else rootLoggerLvl
        logs.put(logger.getName, level)
      }
    }
    logs
  }

  /**
    * Sets the log level of a particular logger
    */
  def logLevel(loggerName: String, logLevel: String): Boolean = {
    val log = existingLogger(loggerName)
    if (!loggerName.trim.isEmpty && !logLevel.trim.isEmpty && log != null) {
      log.setLevel(Level.toLevel(logLevel.toUpperCase(Locale.ROOT)))
      true
    }
    else false
  }

  def unsetLogLevel(loggerName: String): Boolean = {
    val log = existingLogger(loggerName)
    if (!loggerName.trim.isEmpty && log != null) {
      log.setLevel(null)
      true
    }
    else false
  }

  def loggerExists(loggerName: String): Boolean = existingLogger(loggerName) != null

  private def existingLogger(loggerName: String) =
    if (loggerName == ROOT_LOGGER)
      LogManager.getRootLogger
    else LogManager.exists(loggerName)
}

/**
 * An MBean that allows the user to dynamically alter log4j levels at runtime.
 * The companion object contains the singleton instance of this class and
 * registers the MBean. The [[kafka.utils.Logging]] trait forces initialization
 * of the companion object.
 */
class Log4jController extends Log4jControllerMBean {

  def getLoggers: util.List[String] = {
    Log4jController.loggers.map {
      case (logger, level) => s"$logger=$level"
    }.toList.asJava
  }


  def getLogLevel(loggerName: String): String = {
    val log = Log4jController.existingLogger(loggerName)
    if (log != null) {
      val level = log.getLevel
      if (level != null)
        log.getLevel.toString
      else
        Log4jController.existingLogger(Log4jController.ROOT_LOGGER).getLevel.toString
    }
    else "No such logger."
  }

  def setLogLevel(loggerName: String, level: String): Boolean = Log4jController.logLevel(loggerName, level)
}


trait Log4jControllerMBean {
  def getLoggers: java.util.List[String]
  def getLogLevel(logger: String): String
  def setLogLevel(logger: String, level: String): Boolean
}
