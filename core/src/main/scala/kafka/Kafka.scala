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

package kafka

import com.automq.s3shell.sdk.auth.{CredentialsProviderHolder, EnvVariableCredentialsProvider}
import com.automq.s3shell.sdk.model.S3Url
import joptsimple.OptionParser
import kafka.s3shell.util.S3ShellPropUtil
import kafka.server.{KafkaConfig, KafkaRaftServer, KafkaServer, Server}
import kafka.utils.Implicits._
import kafka.utils.{CommandLineUtils, Exit, Logging}
import org.apache.kafka.common.utils._
import software.amazon.awssdk.auth.credentials.{AwsBasicCredentials, StaticCredentialsProvider}

import java.util.Properties
import scala.jdk.CollectionConverters._

object Kafka extends Logging {

  def getPropsFromArgs(args: Array[String]): Properties = {
    // AutoMQ for Kafka inject start
    if (args.exists(_.contains("s3-url"))) {
      val roleInfo = args.find(_.startsWith("process.roles="))
      if (roleInfo.isEmpty) {
        throw new IllegalArgumentException("'--override process.roles=broker|controller' is required")
      }
      if (!args.exists(_.startsWith("node.id"))) {
        throw new IllegalArgumentException(s"'--override node.id= ' is required")
      }
      if (!args.exists(_.startsWith("controller.quorum.voters"))) {
        throw new IllegalArgumentException(s"'--override controller.quorum.voters=''' is required")
      }
      if (!args.exists(_.startsWith("listeners"))) {
        throw new IllegalArgumentException(s"'--override listeners=''' is required")
      }

      roleInfo match {
        case Some("process.roles=broker") =>
          if (!args.exists(_.startsWith("advertised.listeners"))) {
            throw new IllegalArgumentException(s"'--override advertised.listeners=''' is required")
          }
          return S3ShellPropUtil.autoGenPropsByCmd(args, "broker")
        case Some("process.roles=controller") =>
          return S3ShellPropUtil.autoGenPropsByCmd(args, "controller")
        case _ =>
          if (!args.exists(_.startsWith("advertised.listeners"))) {
            throw new IllegalArgumentException(s"'--override advertised.listeners=''' is required")
          }
          return S3ShellPropUtil.autoGenPropsByCmd(args, "broker,controller")
      }
    }
    // AutoMQ for Kafka inject end

    val optionParser = new OptionParser(false)
    val overrideOpt = optionParser.accepts("override", "Optional property that should override values set in server.properties file")
      .withRequiredArg()
      .ofType(classOf[String])
    // This is just to make the parameter show up in the help output, we are not actually using this due the
    // fact that this class ignores the first parameter which is interpreted as positional and mandatory
    // but would not be mandatory if --version is specified
    // This is a bit of an ugly crutch till we get a chance to rework the entire command line parsing
    optionParser.accepts("version", "Print version information and exit.")

    if (args.isEmpty || args.contains("--help")) {
      CommandLineUtils.printUsageAndDie(optionParser,
        "USAGE: java [options] %s server.properties [--override property=value]*".format(this.getClass.getCanonicalName.split('$').head))
    }

    if (args.contains("--version")) {
      CommandLineUtils.printVersionAndDie()
    }

    val props = Utils.loadProps(args(0))

    if (args.length > 1) {
      val options = optionParser.parse(args.slice(1, args.length): _*)

      if (options.nonOptionArguments().size() > 0) {
        CommandLineUtils.printUsageAndDie(optionParser, "Found non argument parameters: " + options.nonOptionArguments().toArray.mkString(","))
      }

      props ++= CommandLineUtils.parseKeyValueArgs(options.valuesOf(overrideOpt).asScala)
    }
    props
  }

  // For Zk mode, the API forwarding is currently enabled only under migration flag. We can
  // directly do a static IBP check to see API forwarding is enabled here because IBP check is
  // static in Zk mode.
  private def enableApiForwarding(config: KafkaConfig) =
    config.migrationEnabled && config.interBrokerProtocolVersion.isApiForwardingEnabled

  private def buildServer(props: Properties): Server = {
    val config = KafkaConfig.fromProps(props, false)
    if (config.requiresZookeeper) {
      new KafkaServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None,
        enableForwarding = enableApiForwarding(config)
      )
    } else {
      new KafkaRaftServer(
        config,
        Time.SYSTEM,
        threadNamePrefix = None
      )
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      // AutoMQ for Kafka inject start
      val serverProps = getPropsFromArgs(args)
      val s3UrlString = S3Url.parseS3UrlValFromArgs(args)
      if (s3UrlString == null ) {
        CredentialsProviderHolder.create(EnvVariableCredentialsProvider.get())
      } else {
        val s3Url = S3Url.parse(s3UrlString)
        CredentialsProviderHolder.create(StaticCredentialsProvider.create(AwsBasicCredentials.create(s3Url.getS3AccessKey, s3Url.getS3SecretKey)))
      }
      val server = buildServer(serverProps)
      // AutoMQ for Kafka inject end

      try {
        if (!OperatingSystem.IS_WINDOWS && !Java.isIbmJdk)
          new LoggingSignalHandler().register()
      } catch {
        case e: ReflectiveOperationException =>
          warn("Failed to register optional signal handler that logs a message when the process is terminated " +
            s"by a signal. Reason for registration failure is: $e", e)
      }

      // attach shutdown handler to catch terminating signals as well as normal termination
      Exit.addShutdownHook("kafka-shutdown-hook", {
        try server.shutdown()
        catch {
          case _: Throwable =>
            fatal("Halting Kafka.")
            // Calling exit() can lead to deadlock as exit() can be called multiple times. Force exit.
            Exit.halt(1)
        }
      })

      try server.startup()
      catch {
        case e: Throwable =>
          // KafkaServer.startup() calls shutdown() in case of exceptions, so we invoke `exit` to set the status code
          fatal("Exiting Kafka due to fatal exception during startup.", e)
          Exit.exit(1)
      }

      server.awaitShutdown()
    }
    catch {
      case e: Throwable =>
        fatal("Exiting Kafka due to fatal exception", e)
        Exit.exit(1)
    }
    Exit.exit(0)
  }
}
