/*
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

package kafka.zk

import java.nio.charset.StandardCharsets.UTF_8
import org.apache.kafka.common.resource.PatternType.{LITERAL, PREFIXED}
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.{GROUP, TOPIC}
import org.apache.kafka.security.authorizer.AclEntry
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows}
import org.junit.jupiter.api.Test

class LiteralAclStoreTest {
  private val literalResource = new ResourcePattern(TOPIC, "some-topic", LITERAL)
  private val prefixedResource = new ResourcePattern(TOPIC, "some-topic", PREFIXED)
  private val store = LiteralAclStore

  @Test
  def shouldHaveCorrectPaths(): Unit = {
    assertEquals("/kafka-acl", store.aclPath)
    assertEquals("/kafka-acl/Topic", store.path(TOPIC))
    assertEquals("/kafka-acl-changes", store.changeStore.aclChangePath)
  }

  @Test
  def shouldHaveCorrectPatternType(): Unit = {
    assertEquals(LITERAL, store.patternType)
  }

  @Test
  def shouldThrowFromEncodeOnNoneLiteral(): Unit = {
    assertThrows(classOf[IllegalArgumentException], () => store.changeStore.createChangeNode(prefixedResource))
  }

  @Test
  def shouldWriteChangesToTheWritePath(): Unit = {
    val changeNode = store.changeStore.createChangeNode(literalResource)

    assertEquals("/kafka-acl-changes/acl_changes_", changeNode.path)
  }

  @Test
  def shouldRoundTripChangeNode(): Unit = {
    val changeNode = store.changeStore.createChangeNode(literalResource)

    val actual = store.changeStore.decode(changeNode.bytes)

    assertEquals(literalResource, actual)
  }

  @Test
  def shouldDecodeResourceUsingTwoPartLogic(): Unit = {
    val resource = new ResourcePattern(GROUP, "PREFIXED:this, including the PREFIXED part, is a valid two part group name", LITERAL)
    val encoded = (resource.resourceType.toString + AclEntry.RESOURCE_SEPARATOR + resource.name).getBytes(UTF_8)

    val actual = store.changeStore.decode(encoded)

    assertEquals(resource, actual)
  }
}
