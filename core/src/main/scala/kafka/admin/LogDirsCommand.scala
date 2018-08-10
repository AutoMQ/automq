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

package kafka.admin

import java.io.PrintStream
import java.util.Properties

import org.apache.kafka.clients.admin.{AdminClientConfig, DescribeLogDirsResult, AdminClient => JAdminClient}
import org.apache.kafka.common.requests.DescribeLogDirsResponse.LogDirInfo

import scala.collection.JavaConverters._
import scala.collection.Map
import kafka.utils.{CommandLineUtils, Json}
import joptsimple._
import org.apache.kafka.common.utils.Utils

/**
  * A command for querying log directory usage on the specified brokers
  */
object LogDirsCommand {

    def main(args: Array[String]): Unit = {
        describe(args, System.out)
    }

    def describe(args: Array[String], out: PrintStream): Unit = {
        val opts = new LogDirsCommandOptions(args)
        val adminClient = createAdminClient(opts)
        val topicList = opts.options.valueOf(opts.topicListOpt).split(",").filter(!_.isEmpty)
        val brokerList = Option(opts.options.valueOf(opts.brokerListOpt)) match {
            case Some(brokerListStr) => brokerListStr.split(',').filter(!_.isEmpty).map(_.toInt)
            case None => adminClient.describeCluster().nodes().get().asScala.map(_.id()).toArray
        }

        out.println("Querying brokers for log directories information")
        val describeLogDirsResult: DescribeLogDirsResult = adminClient.describeLogDirs(brokerList.map(Integer.valueOf).toSeq.asJava)
        val logDirInfosByBroker = describeLogDirsResult.all.get().asScala.mapValues(_.asScala)

        out.println(s"Received log directory information from brokers ${brokerList.mkString(",")}")
        out.println(formatAsJson(logDirInfosByBroker, topicList.toSet))
        adminClient.close()
    }

    private def formatAsJson(logDirInfosByBroker: Map[Integer, Map[String, LogDirInfo]], topicSet: Set[String]): String = {
        Json.encodeAsString(Map(
            "version" -> 1,
            "brokers" -> logDirInfosByBroker.map { case (broker, logDirInfos) =>
                Map(
                    "broker" -> broker,
                    "logDirs" -> logDirInfos.map { case (logDir, logDirInfo) =>
                        Map(
                            "logDir" -> logDir,
                            "error" -> logDirInfo.error.exceptionName(),
                            "partitions" -> logDirInfo.replicaInfos.asScala.filter { case (topicPartition, _) =>
                                topicSet.isEmpty || topicSet.contains(topicPartition.topic)
                            }.map { case (topicPartition, replicaInfo) =>
                                Map(
                                    "partition" -> topicPartition.toString,
                                    "size" -> replicaInfo.size,
                                    "offsetLag" -> replicaInfo.offsetLag,
                                    "isFuture" -> replicaInfo.isFuture
                                ).asJava
                            }.asJava
                        ).asJava
                    }.asJava
                ).asJava
            }.asJava
        ).asJava)
    }

    private def createAdminClient(opts: LogDirsCommandOptions): JAdminClient = {
        val props = if (opts.options.has(opts.commandConfigOpt))
            Utils.loadProps(opts.options.valueOf(opts.commandConfigOpt))
        else
            new Properties()
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, opts.options.valueOf(opts.bootstrapServerOpt))
        props.putIfAbsent(AdminClientConfig.CLIENT_ID_CONFIG, "log-dirs-tool")
        JAdminClient.create(props)
    }

    class LogDirsCommandOptions(args: Array[String]) {
        val parser = new OptionParser(false)
        val bootstrapServerOpt = parser.accepts("bootstrap-server", "REQUIRED: the server(s) to use for bootstrapping")
          .withRequiredArg
          .describedAs("The server(s) to use for bootstrapping")
          .ofType(classOf[String])
        val commandConfigOpt = parser.accepts("command-config", "Property file containing configs to be passed to Admin Client.")
          .withRequiredArg
          .describedAs("Admin client property file")
          .ofType(classOf[String])
        val describeOpt = parser.accepts("describe", "Describe the specified log directories on the specified brokers.")
        val topicListOpt = parser.accepts("topic-list", "The list of topics to be queried in the form \"topic1,topic2,topic3\". " +
          "All topics will be queried if no topic list is specified")
          .withRequiredArg
          .describedAs("Topic list")
          .defaultsTo("")
          .ofType(classOf[String])
        val brokerListOpt = parser.accepts("broker-list", "The list of brokers to be queried in the form \"0,1,2\". " +
          "All brokers in the cluster will be queried if no broker list is specified")
          .withRequiredArg
          .describedAs("Broker list")
          .ofType(classOf[String])

        val options = parser.parse(args : _*)
        CommandLineUtils.checkRequiredArgs(parser, options, bootstrapServerOpt, describeOpt)
    }
}
