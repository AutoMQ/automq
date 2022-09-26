KRaft (aka KIP-500) mode
=========================================================

# Introduction
It is now possible to run Apache Kafka without Apache ZooKeeper!  We call this the [Kafka Raft metadata mode](https://cwiki.apache.org/confluence/display/KAFKA/KIP-500%3A+Replace+ZooKeeper+with+a+Self-Managed+Metadata+Quorum), typically shortened to `KRaft mode`.
`KRaft` is intended to be pronounced like `craft` (as in `craftsmanship`).

When the Kafka cluster is in KRaft mode, it does not store its metadata in ZooKeeper.  In fact, you do not have to run ZooKeeper at all, because it stores its metadata in a KRaft quorum of controller nodes.

KRaft mode has many benefits -- some obvious, and some not so obvious.  Clearly, it is nice to manage and configure one service rather than two services.  In addition, you can now run a single process Kafka cluster.
Most important of all, KRaft mode is more scalable.  We expect to be able to [support many more topics and partitions](https://www.confluent.io/kafka-summit-san-francisco-2019/kafka-needs-no-keeper/) in this mode.

# Quickstart

## Generate a cluster ID
The first step is to generate an ID for your new cluster, using the kafka-storage tool:

~~~~
$ ./bin/kafka-storage.sh random-uuid
xtzWWN4bTjitpL3kfd9s5g
~~~~

## Format Storage Directories
The next step is to format your storage directories.  If you are running in single-node mode, you can do this with one command:

~~~~
$ ./bin/kafka-storage.sh format -t <uuid> -c ./config/kraft/server.properties
Formatting /tmp/kraft-combined-logs
~~~~

If you are using multiple nodes, then you should run the format command on each node.  Be sure to use the same cluster ID for each one.

This example configures the node as both a broker and controller (i.e. `process.roles=broker,controller`). It is also possible to run the broker and controller nodes separately.
Please see [here](https://github.com/apache/kafka/blob/trunk/config/kraft/broker.properties) and [here](https://github.com/apache/kafka/blob/trunk/config/kraft/controller.properties) for example configurations.

## Start the Kafka Server
Finally, you are ready to start the Kafka server on each node.

~~~~
$ ./bin/kafka-server-start.sh ./config/kraft/server.properties
[2021-02-26 15:37:11,071] INFO Registered kafka:type=kafka.Log4jController MBean (kafka.utils.Log4jControllerRegistration$)
[2021-02-26 15:37:11,294] INFO Setting -D jdk.tls.rejectClientInitiatedRenegotiation=true to disable client-initiated TLS renegotiation (org.apache.zookeeper.common.X509Util)
[2021-02-26 15:37:11,466] INFO [Log partition=__cluster_metadata-0, dir=/tmp/kraft-combined-logs] Loading producer state till offset 0 with message format version 2 (kafka.log.Log)
[2021-02-26 15:37:11,509] INFO [raft-expiration-reaper]: Starting (kafka.raft.TimingWheelExpirationService$ExpiredOperationReaper)
[2021-02-26 15:37:11,640] INFO [RaftManager nodeId=1] Completed transition to Unattached(epoch=0, voters=[1], electionTimeoutMs=9037) (org.apache.kafka.raft.QuorumState)
...
~~~~

Just like with a ZooKeeper based broker, you can connect to port 9092 (or whatever port you configured) to perform administrative operations or produce or consume data.

~~~~
$ ./bin/kafka-topics.sh --create --topic foo --partitions 1 --replication-factor 1 --bootstrap-server localhost:9092
Created topic foo.
~~~~

# Deployment

## Controller Servers
In KRaft mode, only a small group of specially selected servers can act as controllers (unlike the ZooKeeper-based mode, where any server can become the
Controller).  The specially selected controller servers will participate in the metadata quorum.  Each controller server is either active, or a hot
standby for the current active controller server.

You will typically select 3 or 5 servers for this role, depending on factors like cost and the number of concurrent failures your system should withstand
without availability impact.  Just like with ZooKeeper, you must keep a majority of the controllers alive in order to maintain availability.  So if you have 3
controllers, you can tolerate 1 failure; with 5 controllers, you can tolerate 2 failures.

## Process Roles
Each Kafka server now has a new configuration key called `process.roles` which can have the following values:

* If `process.roles` is set to `broker`, the server acts as a broker in KRaft mode.
* If `process.roles` is set to `controller`, the server acts as a controller in KRaft mode.
* If `process.roles` is set to `broker,controller`, the server acts as both a broker and a controller in KRaft mode.
* If `process.roles` is not set at all then we are assumed to be in ZooKeeper mode.  As mentioned earlier, you can't currently transition back and forth between ZooKeeper mode and KRaft mode without reformatting.

Nodes that act as both brokers and controllers are referred to as "combined" nodes.  Combined nodes are simpler to operate for simple use cases and allow you to avoid
some fixed memory overheads associated with JVMs.  The key disadvantage is that the controller will be less isolated from the rest of the system.  For example, if activity on the broker causes an out of
memory condition, the controller part of the server is not isolated from that OOM condition.

## Quorum Voters
All nodes in the system must set the `controller.quorum.voters` configuration.  This identifies the quorum controller servers that should be used.  All the controllers must be enumerated.
This is similar to how, when using ZooKeeper, the `zookeeper.connect` configuration must contain all the ZooKeeper servers.  Unlike with the ZooKeeper config, however, `controller.quorum.voters`
also has IDs for each node.  The format is id1@host1:port1,id2@host2:port2, etc.

So if you have 10 brokers and 3 controllers named controller1, controller2, controller3, you might have the following configuration on controller1:
```
process.roles=controller
node.id=1
listeners=CONTROLLER://controller1.example.com:9093
controller.quorum.voters=1@controller1.example.com:9093,2@controller2.example.com:9093,3@controller3.example.com:9093
```

Each broker and each controller must set `controller.quorum.voters`.  Note that the node ID supplied in the `controller.quorum.voters` configuration must match that supplied to the server.
So on controller1, node.id must be set to 1, and so forth.  Note that there is no requirement for controller IDs to start at 0 or 1.  However, the easiest and least confusing way to allocate
node IDs is probably just to give each server a numeric ID, starting from 0.  Also note that each node ID must be unique across all the nodes in a particular cluster; no two nodes can have the same node ID regardless of their `process.roles` values.

Note that clients never need to configure `controller.quorum.voters`; only servers do.

## Kafka Storage Tool
As described above in the QuickStart section, you must use the `kafka-storage.sh` tool to generate a cluster ID for your new cluster, and then run the format command on each node before starting the node.

This is different from how Kafka has operated in the past.  Previously, Kafka would format blank storage directories automatically, and also generate a new cluster UUID automatically.  One reason for the change
is that auto-formatting can sometimes obscure an error condition.  For example, under UNIX, if a data directory can't be mounted, it may show up as blank.  In this case, auto-formatting would be the wrong thing to do.

This is particularly important for the metadata log maintained by the controller servers.  If two controllers out of three controllers were able to start with blank logs, a leader might be able to be elected with
nothing in the log, which would cause all metadata to be lost.

# Missing Features

The following features have not yet been fully implemented:

* Configuring SCRAM users via the administrative API
* Supporting JBOD configurations with multiple storage directories
* Modifying certain dynamic configurations on the standalone KRaft controller
* Delegation tokens
* Upgrade from ZooKeeper mode

# Debugging
If you encounter an issue, you might want to take a look at the metadata log.

## kafka-dump-log
One way to view the metadata log is with kafka-dump-log.sh tool, like so:

~~~~
$ ./bin/kafka-dump-log.sh  --cluster-metadata-decoder --skip-record-metadata --files /tmp/kraft-combined-logs/__cluster_metadata-0/*.log
Dumping /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
Starting offset: 0
baseOffset: 0 lastOffset: 0 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 1 isTransactional: false isControl: true position: 0 CreateTime: 1614382631640 size: 89 magic: 2 compresscodec: NONE crc: 1438115474 isvalid: true

baseOffset: 1 lastOffset: 1 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 1 isTransactional: false isControl: false position: 89 CreateTime: 1614382632329 size: 137 magic: 2 compresscodec: NONE crc: 1095855865 isvalid: true
 payload: {"type":"REGISTER_BROKER_RECORD","version":0,"data":{"brokerId":1,"incarnationId":"P3UFsWoNR-erL9PK98YLsA","brokerEpoch":0,"endPoints":[{"name":"PLAINTEXT","host":"localhost","port":9092,"securityProtocol":0}],"features":[],"rack":null}}
baseOffset: 2 lastOffset: 2 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 1 isTransactional: false isControl: false position: 226 CreateTime: 1614382632453 size: 83 magic: 2 compresscodec: NONE crc: 455187130 isvalid: true
 payload: {"type":"UNFENCE_BROKER_RECORD","version":0,"data":{"id":1,"epoch":0}}
baseOffset: 3 lastOffset: 3 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 1 isTransactional: false isControl: false position: 309 CreateTime: 1614382634484 size: 83 magic: 2 compresscodec: NONE crc: 4055692847 isvalid: true
 payload: {"type":"FENCE_BROKER_RECORD","version":0,"data":{"id":1,"epoch":0}}
baseOffset: 4 lastOffset: 4 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 2 isTransactional: false isControl: true position: 392 CreateTime: 1614382671857 size: 89 magic: 2 compresscodec: NONE crc: 1318571838 isvalid: true

baseOffset: 5 lastOffset: 5 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 2 isTransactional: false isControl: false position: 481 CreateTime: 1614382672440 size: 137 magic: 2 compresscodec: NONE crc: 841144615 isvalid: true
 payload: {"type":"REGISTER_BROKER_RECORD","version":0,"data":{"brokerId":1,"incarnationId":"RXRJu7cnScKRZOnWQGs86g","brokerEpoch":4,"endPoints":[{"name":"PLAINTEXT","host":"localhost","port":9092,"securityProtocol":0}],"features":[],"rack":null}}
baseOffset: 6 lastOffset: 6 count: 1 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 2 isTransactional: false isControl: false position: 618 CreateTime: 1614382672544 size: 83 magic: 2 compresscodec: NONE crc: 4155905922 isvalid: true
 payload: {"type":"UNFENCE_BROKER_RECORD","version":0,"data":{"id":1,"epoch":4}}
baseOffset: 7 lastOffset: 8 count: 2 baseSequence: -1 lastSequence: -1 producerId: -1 producerEpoch: -1 partitionLeaderEpoch: 2 isTransactional: false isControl: false position: 701 CreateTime: 1614382712158 size: 159 magic: 2 compresscodec: NONE crc: 3726758683 isvalid: true
 payload: {"type":"TOPIC_RECORD","version":0,"data":{"name":"foo","topicId":"5zoAlv-xEh9xRANKXt1Lbg"}}
 payload: {"type":"PARTITION_RECORD","version":0,"data":{"partitionId":0,"topicId":"5zoAlv-xEh9xRANKXt1Lbg","replicas":[1],"isr":[1],"removingReplicas":null,"addingReplicas":null,"leader":1,"leaderEpoch":0,"partitionEpoch":0}}
~~~~

## The Metadata Shell
Another tool for examining the metadata logs is the Kafka metadata shell.  Just like the ZooKeeper shell, this allows you to inspect the metadata of the cluster.

~~~~
$ ./bin/kafka-metadata-shell.sh  --snapshot /tmp/kraft-combined-logs/__cluster_metadata-0/00000000000000000000.log
>> ls /
brokers  local  metadataQuorum  topicIds  topics
>> ls /topics
foo
>> cat /topics/foo/0/data
{
  "partitionId" : 0,
  "topicId" : "5zoAlv-xEh9xRANKXt1Lbg",
  "replicas" : [ 1 ],
  "isr" : [ 1 ],
  "removingReplicas" : null,
  "addingReplicas" : null,
  "leader" : 1,
  "leaderEpoch" : 0,
  "partitionEpoch" : 0
}
>> exit
~~~~
