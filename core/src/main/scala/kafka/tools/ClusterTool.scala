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

package kafka.tools

import java.io.PrintStream
import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.utils.{Exit, Logging}
import net.sourceforge.argparse4j.ArgumentParsers
import net.sourceforge.argparse4j.impl.Arguments.{store, storeTrue}
import org.apache.kafka.clients.admin.{Admin, DescribeNodeIdsOptions}
import org.apache.kafka.common.errors.UnsupportedVersionException
import org.apache.kafka.common.utils.Utils

import java.util.stream.Collectors

object ClusterTool extends Logging {
  def main(args: Array[String]): Unit = {
    try {
      val parser = ArgumentParsers.
        newArgumentParser("kafka-cluster").
        defaultHelp(true).
        description("The Kafka cluster tool.")
      val subparsers = parser.addSubparsers().dest("command")

      val clusterIdParser = subparsers.addParser("cluster-id").
        help("Get information about the ID of a cluster.")
      val unregisterParser = subparsers.addParser("unregister").
        help("Unregister a broker.")
      // Kafka on S3 inject start
      val nodeIdsParser = subparsers.addParser("node-ids")
        .help("Get node ids of the cluster.")
      List(clusterIdParser, unregisterParser, nodeIdsParser).foreach(parser => {
        parser.addArgument("--bootstrap-server", "-b").
          action(store()).
          help("A list of host/port pairs to use for establishing the connection to the kafka cluster.")
        parser.addArgument("--config", "-c").
          action(store()).
          help("A property file containing configs to passed to AdminClient.")
      })
      unregisterParser.addArgument("--id", "-i").
        `type`(classOf[Integer]).
        action(store()).
        required(true).
        help("The ID of the broker to unregister.")
      nodeIdsParser.addArgument("--broker")
        .action(storeTrue())
        .help("Print broker IDs.")
      nodeIdsParser.addArgument("--controller")
        .action(storeTrue())
        .help("Print controller IDs.")
      nodeIdsParser.addArgument("--alive")
        .action(storeTrue())
        .help("Print only alive nodes.")

      val namespace = parser.parseArgsOrFail(args)
      val command = namespace.getString("command")
      val configPath = namespace.getString("config")
      val properties = if (configPath == null) {
        new Properties()
      } else {
        Utils.loadProps(configPath)
      }
      Option(namespace.getString("bootstrap_server")).
        foreach(b => properties.setProperty("bootstrap.servers", b))
      if (properties.getProperty("bootstrap.servers") == null) {
        throw new TerseFailure("Please specify --bootstrap-server.")
      }

      command match {
        case "cluster-id" =>
          val adminClient = Admin.create(properties)
          try {
            clusterIdCommand(System.out, adminClient)
          } finally {
            adminClient.close()
          }
          Exit.exit(0)
        case "unregister" =>
          val adminClient = Admin.create(properties)
          try {
            unregisterCommand(System.out, adminClient, namespace.getInt("id"))
          } finally {
            adminClient.close()
          }
          Exit.exit(0)
        case "node-ids" =>
          val adminClient = Admin.create(properties)
          try {
            val broker = namespace.getBoolean("broker")
            val controller = namespace.getBoolean("controller")
            val alive = namespace.getBoolean("alive")
            if (!broker && !controller) {
              throw new TerseFailure("Please specify --broker or --controller.")
            }
            allNodeIdsCommand(onlyAliveNodes = alive, showControllers = controller, showBrokers = broker, System.out, adminClient)
          } finally {
            adminClient.close()
          }
          // Kafka on S3 inject end
          Exit.exit(0)
        case _ =>
          throw new RuntimeException(s"Unknown command $command")
      }
    } catch {
      case e: TerseFailure =>
        System.err.println(e.getMessage)
        System.exit(1)
    }
  }

  // Kafka on S3 inject start
  def allNodeIdsCommand(onlyAliveNodes: Boolean,
                        showControllers: Boolean,
                        showBrokers: Boolean,
                        stream: PrintStream,
                        adminClient: Admin): Unit = {
    val nodeIdsResult = adminClient.describeNodeIds(new DescribeNodeIdsOptions().onlyAliveNodes(onlyAliveNodes))
    val controllerIds = Option(nodeIdsResult.controllerIds().get())
    val brokerIds = Option(nodeIdsResult.brokerIds().get())
    printNodeIds(showControllers, stream, controllerIds, "controller")
    printNodeIds(showBrokers, stream, brokerIds, "broker")
  }

  private def printNodeIds(showInStream: Boolean,
                           stream: PrintStream,
                           nodeIds: Option[java.util.List[Integer]],
                           role: String): Unit = {
    if (showInStream) {
      nodeIds match {
        case None => stream.println(s"No $role found.")
        case Some(ids) =>
          val sortedIds = ids.stream().sorted().collect(Collectors.toList())
          stream.println(s"node ids with $role role: $sortedIds")
      }
    }
  }
  // Kafka on S3 inject end

  def clusterIdCommand(stream: PrintStream,
                       adminClient: Admin): Unit = {
    val clusterId = Option(adminClient.describeCluster().clusterId().get())
    clusterId match {
      case None => stream.println(s"No cluster ID found. The Kafka version is probably too old.")
      case Some(id) => stream.println(s"Cluster ID: ${id}")
    }
  }

  def unregisterCommand(stream: PrintStream,
                          adminClient: Admin,
                          id: Int): Unit = {
    try {
      Option(adminClient.unregisterBroker(id).all().get())
      stream.println(s"Broker ${id} is no longer registered.")
    } catch {
      case e: ExecutionException => {
        val cause = e.getCause()
        if (cause.isInstanceOf[UnsupportedVersionException]) {
          stream.println(s"The target cluster does not support the broker unregistration API.")
        } else {
          throw e
        }
      }
    }
  }
}
