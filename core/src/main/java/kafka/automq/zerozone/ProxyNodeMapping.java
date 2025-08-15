/*
 * Copyright 2025, AutoMQ HK Limited.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.automq.zerozone;

import kafka.automq.interceptor.ClientIdMetadata;
import kafka.server.MetadataCache;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.MetadataResponseData;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.metadata.BrokerRegistration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

import software.amazon.awssdk.annotations.NotNull;
import thirdparty.com.github.jaskey.consistenthash.ConsistentHashRouter;

/**
 * Maintain the relationship for main node and proxy node.
 */
class ProxyNodeMapping {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyNodeMapping.class);
    private static final String NOOP_RACK = "";
    private static final int DEFAULT_VIRTUAL_NODE_COUNT = 8;
    private final Node currentNode;
    private final String currentRack;
    private final String interBrokerListenerName;
    private final MetadataCache metadataCache;
    private final List<ProxyTopologyChangeListener> listeners = new CopyOnWriteArrayList<>();

    volatile Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack = new HashMap<>();
    volatile boolean inited = false;

    public ProxyNodeMapping(Node currentNode, String currentRack, String interBrokerListenerName,
        MetadataCache metadataCache) {
        this.interBrokerListenerName = interBrokerListenerName;
        this.currentNode = currentNode;
        this.currentRack = currentRack;
        this.metadataCache = metadataCache;
    }

    /**
     * Get route out node to split the produce request.
     * <p>
     * If return Node.noNode, it means the producer should refresh metadata and send to another node, {@link RouterOut} will return NOT_LEADER_OR_FOLLOWER.
     */
    public Node getRouteOutNode(String topicName, int partition, ClientIdMetadata clientId) {
        String clientRack = clientId.rack();

        BrokerRegistration target = metadataCache.getPartitionLeaderNode(topicName, partition);
        if (target == null) {
            return currentNode;
        }
        if (clientRack == null) {
            // If the client rack isn't set, expect produce send to the real leader.
            if (target.id() == currentNode.id()) {
                return currentNode;
            } else {
                return Node.noNode();
            }
        }

        Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack = this.main2proxyByRack;
        if (Objects.equals(clientRack, currentRack)) {
            if (target.id() == currentNode.id()) {
                return currentNode;
            } else {
                if (Objects.equals(target.rack().orElse(null), currentRack)) {
                    // The producer should refresh metadata and send to another node in the same rack as the producer
                    return Node.noNode();
                } else {
                    // Check whether the current node should proxy the target
                    Map<Integer, BrokerRegistration> main2proxy = main2proxyByRack.get(currentRack);
                    if (main2proxy == null) {
                        // The current node is the last node in the rack, and the current node is shutting down.
                        return Node.noNode();
                    }
                    BrokerRegistration proxyNode = main2proxy.get(target.id());
                    if (proxyNode != null && proxyNode.id() == currentNode.id()) {
                        // Get the target main node.
                        return target.node(interBrokerListenerName).orElse(currentNode);
                    } else {
                        // The producer should refresh metadata and send to another node in the same rack as the current node.
                        return Node.noNode();
                    }

                }
            }
        } else {
            if (main2proxyByRack.containsKey(clientRack)) {
                // The producer should send records to the nodes with the same rack.
                return Node.noNode();
            } else {
                MismatchRecorder.instance().record(topicName, clientId);
                // The cluster doesn't cover the client rack, the producer should directly send records to the partition main node.
                if (target.id() == currentNode.id()) {
                    return currentNode;
                } else {
                    return Node.noNode();
                }
            }
        }
    }

    /**
     * Get the proxy leader node when NOT_LEADER_OR_FOLLOWER happens.
     */
    public Optional<Node> getLeaderNode(int leaderMainNodeId, ClientIdMetadata clientId, String listenerName) {
        BrokerRegistration target = metadataCache.getNode(leaderMainNodeId);
        if (target == null) {
            return Optional.empty();
        }
        String clientRack = clientId.rack();
        if (clientRack == null) {
            // If the client rack isn't set, then return the main node.
            return target.node(listenerName);
        }

        Map<Integer, BrokerRegistration> clientRackMain2proxy = main2proxyByRack.get(clientRack);
        if (clientRackMain2proxy == null) {
            // If the cluster doesn't cover the client rack, the producer should directly send records to the main node.
            return target.node(listenerName);
        }

        // Get the proxy node.
        BrokerRegistration proxy = clientRackMain2proxy.get(target.id());
        if (proxy == null) {
            // The producer rack is the same as the leader rack.
            return target.node(listenerName);
        }
        return proxy.node(listenerName);
    }

    public List<MetadataResponseData.MetadataResponseTopic> handleMetadataResponse(ClientIdMetadata clientIdMetadata,
        List<MetadataResponseData.MetadataResponseTopic> topics) {
        String clientRack = clientIdMetadata.rack();
        if (clientRack == null) {
            return withSnapshotReadFollowers(topics);
        }
        Map<Integer, BrokerRegistration> clientRackMain2proxy = main2proxyByRack.get(clientRack);
        if (clientRackMain2proxy == null) {
            // If the cluster doesn't cover the client rack, the producer should directly send records to the main node.
            return withSnapshotReadFollowers(topics);
        }
        // If the client config rack in clientId, we need to replace the leader id with the proxy leader id.
        topics.forEach(metadataResponseTopic -> {
            metadataResponseTopic.partitions().forEach(metadataResponsePartition -> {
                int leaderMainNodeId = metadataResponsePartition.leaderId();
                if (leaderMainNodeId != -1) {
                    BrokerRegistration proxy = clientRackMain2proxy.get(leaderMainNodeId);
                    if (proxy != null) {
                        int proxyLeaderId = proxy.id();
                        if (proxyLeaderId != leaderMainNodeId) {
                            metadataResponsePartition.setLeaderId(proxyLeaderId);
                            metadataResponsePartition.setIsrNodes(List.of(proxyLeaderId));
                            metadataResponsePartition.setReplicaNodes(List.of(proxyLeaderId));
                        }
                    }
                }
            });
        });
        return topics;
    }

    public void onChange(MetadataDelta delta, MetadataImage image) {
        if (!inited) {
            // When the main2proxyByRack is un-inited, we should force update.
            inited = true;
        } else {
            if (delta.clusterDelta() == null || delta.clusterDelta().changedBrokers().isEmpty()) {
                return;
            }
        }
        // categorize the brokers by rack
        Map<String, List<BrokerRegistration>> rack2brokers = new HashMap<>();
        image.cluster().brokers().forEach((nodeId, node) -> {
            if (node.fenced() || node.inControlledShutdown()) {
                return;
            }
            rack2brokers.compute(node.rack().orElse(NOOP_RACK), (rack, list) -> {
                if (list == null) {
                    list = new ArrayList<>();
                }
                list.add(node);
                return list;
            });
        });
        this.main2proxyByRack = calMain2proxyByRack(rack2brokers);
        notifyListeners(this.main2proxyByRack);
    }

    public void registerListener(ProxyTopologyChangeListener listener) {
        listeners.add(listener);
        listener.onChange(this.main2proxyByRack);
    }

    private void notifyListeners(Map<String, Map<Integer, BrokerRegistration>> main2proxyByRack) {
        listeners.forEach(listener -> {
            try {
                listener.onChange(main2proxyByRack);
            } catch (Throwable e) {
                LOGGER.error("fail to notify listener {}", listener, e);
            }
        });
    }

    private List<MetadataResponseData.MetadataResponseTopic> withSnapshotReadFollowers(List<MetadataResponseData.MetadataResponseTopic> topics) {
        topics.forEach(metadataResponseTopic -> {
            metadataResponseTopic.partitions().forEach(metadataResponsePartition -> {
                int leaderMainNodeId = metadataResponsePartition.leaderId();
                if (leaderMainNodeId != -1) {
                    List<Integer> replicas = new ArrayList<>(main2proxyByRack.size());
                    replicas.add(leaderMainNodeId);
                    main2proxyByRack.forEach((rack, main2proxy) -> {
                        BrokerRegistration proxy = main2proxy.get(leaderMainNodeId);
                        if (proxy != null && proxy.id() != leaderMainNodeId) {
                            replicas.add(proxy.id());
                        }
                    });
                    metadataResponsePartition.setIsrNodes(replicas);
                    metadataResponsePartition.setReplicaNodes(replicas);
                }
            });
        });
        return topics;
    }

    static Map<String, Map<Integer, BrokerRegistration>> calMain2proxyByRack(
        Map<String, List<BrokerRegistration>> rack2brokers) {
        rack2brokers.forEach((rack, brokers) -> brokers.sort(Comparator.comparingInt(BrokerRegistration::id)));

        Map<String, Map<Integer, BrokerRegistration>> newMain2proxyByRack = new HashMap<>();
        List<String> racks = rack2brokers.keySet().stream().sorted().collect(Collectors.toList());
        racks.forEach(proxyRack -> {
            Map<Integer, BrokerRegistration> newMain2proxy = new HashMap<>();
            List<ProxyNode> proxyNodes = new ArrayList<>();
            ConsistentHashRouter<ProxyNode> router = new ConsistentHashRouter<>();
            List<BrokerRegistration> proxyRackBrokers = rack2brokers.get(proxyRack);
            proxyRackBrokers.forEach(node -> {
                ProxyNode proxyNode = new ProxyNode(node);
                router.addNode(proxyNode, DEFAULT_VIRTUAL_NODE_COUNT);
                proxyNodes.add(proxyNode);
            });

            // allocate the proxy node by consistent hash
            int proxyNodeCount = 0;
            for (String rack : racks) {
                List<BrokerRegistration> brokers = rack2brokers.get(rack);
                if (Objects.equals(rack, proxyRack)) {
                    continue;
                }
                for (BrokerRegistration node : brokers) {
                    ProxyNode proxyNode = router.routeNode(Integer.toString(node.id()));
                    newMain2proxy.put(node.id(), proxyNode.node);
                    proxyNode.mainNodeIds.add(node.id());
                    proxyNodeCount++;
                }
            }
            // balance the proxy node count
            double avg = Math.ceil((double) proxyNodeCount / proxyNodes.size());
            proxyNodes.sort(Comparator.reverseOrder());
            for (ProxyNode overloadNode : proxyNodes) {
                if (overloadNode.mainNodeIds.size() <= avg) {
                    break;
                }
                // move overload node's proxied node to free node
                for (int i = proxyNodes.size() - 1; i >= 0 && overloadNode.mainNodeIds.size() > avg; i--) {
                    ProxyNode freeNode = proxyNodes.get(i);
                    if (freeNode.mainNodeIds.size() > avg - 1) {
                        continue;
                    }
                    Integer mainNodeId = overloadNode.mainNodeIds.remove(overloadNode.mainNodeIds.size() - 1);
                    newMain2proxy.put(mainNodeId, freeNode.node);
                    freeNode.mainNodeIds.add(mainNodeId);
                }
            }
            // try let controller only proxy controller
            tryFreeController(proxyNodes, avg);

            newMain2proxyByRack.put(proxyRack, newMain2proxy);
        });
        return newMain2proxyByRack;
    }

    /**
     * Try to move the traffic from controller to broker.
     * - Let main node(controller) proxied by proxy node(controller).
     * - Let proxy node(controller) proxy less main node if possible.
     */
    static void tryFreeController(List<ProxyNode> proxyNodes, double avg) {
        for (ProxyNode controller : proxyNodes) {
            if (!isController(controller.node.id())) {
                continue;
            }
            for (int i = 0; i < controller.mainNodeIds.size(); i++) {
                int mainNodeId = controller.mainNodeIds.get(i);
                if (isController(mainNodeId)) {
                    continue;
                }
                L1:
                for (ProxyNode switchNode : proxyNodes) {
                    if (switchNode.node.id() == controller.node.id() || isController(switchNode.node.id())) {
                        continue;
                    }
                    // move the main node to the switch node
                    if (switchNode.mainNodeIds.size() < avg) {
                        controller.mainNodeIds.remove(i);
                        switchNode.mainNodeIds.add(mainNodeId);
                        i--;
                        break;
                    } else {
                        // swap the main node with the switch node's main node(controller)
                        for (int j = 0; j < switchNode.mainNodeIds.size(); j++) {
                            int switchNodeMainNodeId = switchNode.mainNodeIds.get(j);
                            if (!isController(switchNodeMainNodeId)) {
                                continue;
                            }
                            controller.mainNodeIds.set(i, switchNodeMainNodeId);
                            switchNode.mainNodeIds.set(j, mainNodeId);
                            break L1;
                        }
                    }
                }
            }
        }
    }

    static class ProxyNode implements thirdparty.com.github.jaskey.consistenthash.Node, Comparable<ProxyNode> {
        final BrokerRegistration node;
        final List<Integer> mainNodeIds = new ArrayList<>();

        private final String key;

        public ProxyNode(BrokerRegistration node) {
            this.node = node;
            this.key = Integer.toString(node.id());
        }

        @Override
        public String getKey() {
            return key;
        }

        @Override
        public int compareTo(@NotNull ProxyNode o) {
            int rst = Integer.compare(mainNodeIds.size(), o.mainNodeIds.size());
            if (rst != 0) {
                return rst;
            }
            return Integer.compare(node.id(), o.node.id());
        }
    }

    static boolean isController(int nodeId) {
        return nodeId < 100;
    }

}
