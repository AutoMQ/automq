/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
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
package org.apache.kafka.streams.scala
package kstream

import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams.kstream.{Suppressed, ValueTransformerWithKeySupplier, KTable => KTableJ}
import org.apache.kafka.streams.scala.FunctionsCompatConversions._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.state.KeyValueStore

/**
 * Wraps the Java class [[org.apache.kafka.streams.kstream.KTable]] and delegates method calls to the underlying Java object.
 *
 * @param [K] Type of keys
 * @param [V] Type of values
 * @param inner The underlying Java abstraction for KTable
 *
 * @see `org.apache.kafka.streams.kstream.KTable`
 */
class KTable[K, V](val inner: KTableJ[K, V]) {

  /**
   * Create a new [[KTable]] that consists all records of this [[KTable]] which satisfies the given
   * predicate
   *
   * @param predicate a filter that is applied to each record
   * @return a [[KTable]] that contains only those records that satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KTable#filter`
   */
  def filter(predicate: (K, V) => Boolean): KTable[K, V] =
    inner.filter(predicate.asPredicate)

  /**
   * Create a new [[KTable]] that consists all records of this [[KTable]] which satisfies the given
   * predicate
   *
   * @param predicate a filter that is applied to each record
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains only those records that satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KTable#filter`
   */
  def filter(predicate: (K, V) => Boolean, materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    inner.filter(predicate.asPredicate, materialized)

  /**
   * Create a new [[KTable]] that consists all records of this [[KTable]] which do <em>not</em> satisfy the given
   * predicate
   *
   * @param predicate a filter that is applied to each record
   * @return a [[KTable]] that contains only those records that do <em>not</em> satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KTable#filterNot`
   */
  def filterNot(predicate: (K, V) => Boolean): KTable[K, V] =
    inner.filterNot(predicate.asPredicate)

  /**
   * Create a new [[KTable]] that consists all records of this [[KTable]] which do <em>not</em> satisfy the given
   * predicate
   *
   * @param predicate a filter that is applied to each record
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains only those records that do <em>not</em> satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KTable#filterNot`
   */
  def filterNot(predicate: (K, V) => Boolean, materialized: Materialized[K, V, ByteArrayKeyValueStore]): KTable[K, V] =
    inner.filterNot(predicate.asPredicate, materialized)

  /**
   * Create a new [[KTable]] by transforming the value of each record in this [[KTable]] into a new value
   * (with possible new type) in the new [[KTable]].
   * <p>
   * The provided `mapper`, a function `V => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper, a function `V => VR` that computes a new output value
   * @return a [[KTable]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KTable#mapValues`
   */
  def mapValues[VR](mapper: V => VR): KTable[K, VR] =
    inner.mapValues[VR](mapper.asValueMapper)

  /**
   * Create a new [[KTable]] by transforming the value of each record in this [[KTable]] into a new value
   * (with possible new type) in the new [[KTable]].
   * <p>
   * The provided `mapper`, a function `V => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper, a function `V => VR` that computes a new output value
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KTable#mapValues`
   */
  def mapValues[VR](mapper: V => VR, materialized: Materialized[K, VR, ByteArrayKeyValueStore]): KTable[K, VR] =
    inner.mapValues[VR](mapper.asValueMapper, materialized)

  /**
   * Create a new [[KTable]] by transforming the value of each record in this [[KTable]] into a new value
   * (with possible new type) in the new [[KTable]].
   * <p>
   * The provided `mapper`, a function `(K, V) => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper, a function `(K, V) => VR` that computes a new output value
   * @return a [[KTable]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KTable#mapValues`
   */
  def mapValues[VR](mapper: (K, V) => VR): KTable[K, VR] =
    inner.mapValues[VR](mapper.asValueMapperWithKey)

  /**
   * Create a new [[KTable]] by transforming the value of each record in this [[KTable]] into a new value
   * (with possible new type) in the new [[KTable]].
   * <p>
   * The provided `mapper`, a function `(K, V) => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper, a function `(K, V) => VR` that computes a new output value
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KTable#mapValues`
   */
  def mapValues[VR](mapper: (K, V) => VR, materialized: Materialized[K, VR, ByteArrayKeyValueStore]): KTable[K, VR] =
    inner.mapValues[VR](mapper.asValueMapperWithKey)

  /**
   * Convert this changelog stream to a [[KStream]].
   *
   * @return a [[KStream]] that contains the same records as this [[KTable]]
   * @see `org.apache.kafka.streams.kstream.KTable#toStream`
   */
  def toStream: KStream[K, V] = inner.toStream

  /**
   * Convert this changelog stream to a [[KStream]] using the given key/value mapper to select the new key
   *
   * @param mapper a function that computes a new key for each record
   * @return a [[KStream]] that contains the same records as this [[KTable]]
   * @see `org.apache.kafka.streams.kstream.KTable#toStream`
   */
  def toStream[KR](mapper: (K, V) => KR): KStream[KR, V] =
    inner.toStream[KR](mapper.asKeyValueMapper)

  /**
   * Suppress some updates from this changelog stream, determined by the supplied [[Suppressed]] configuration.
   *
   * This controls what updates downstream table and stream operations will receive.
   *
   * @param suppressed Configuration object determining what, if any, updates to suppress.
   * @return A new KTable with the desired suppression characteristics.
   * @see `org.apache.kafka.streams.kstream.KTable#suppress`
   */
  def suppress(suppressed: Suppressed[_ >: K]): KTable[K, V] =
    inner.suppress(suppressed)

  /**
   * Create a new `KTable` by transforming the value of each record in this `KTable` into a new value, (with possibly new type).
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformerWithKey` (provided by the given `ValueTransformerWithKeySupplier`) is applied to each input
   * record value and computes a new value for it.
   * This is similar to `#mapValues(ValueMapperWithKey)`, but more flexible, allowing access to additional state-stores,
   * and to the `ProcessorContext`.
   * If the downstream topology uses aggregation functions, (e.g. `KGroupedTable#reduce`, `KGroupedTable#aggregate`, etc),
   * care must be taken when dealing with state, (either held in state-stores or transformer instances), to ensure correct
   * aggregate results.
   * In contrast, if the resulting KTable is materialized, (cf. `#transformValues(ValueTransformerWithKeySupplier, Materialized, String...)`),
   * such concerns are handled for you.
   * In order to assign a state, the state must be created and registered
   * beforehand via stores added via `addStateStore` or `addGlobalStore` before they can be connected to the `Transformer`
   *
   * @param valueTransformerWithKeySupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`.
   *                                 At least one transformer instance will be created per streaming task.
   *                                 Transformer implementations doe not need to be thread-safe.
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](valueTransformerWithKeySupplier: ValueTransformerWithKeySupplier[K, V, VR],
                          stateStoreNames: String*): KTable[K, VR] =
    inner.transformValues[VR](valueTransformerWithKeySupplier, stateStoreNames: _*)

  /**
   * Create a new `KTable` by transforming the value of each record in this `KTable` into a new value, (with possibly new type).
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * This is similar to `#mapValues(ValueMapperWithKey)`, but more flexible, allowing stateful, rather than stateless,
   * record-by-record operation, access to additional state-stores, and access to the `ProcessorContext`.
   * In order to assign a state, the state must be created and registered
   * beforehand via stores added via `addStateStore` or `addGlobalStore` before they can be connected to the `Transformer`
   * The resulting `KTable` is materialized into another state store (additional to the provided state store names)
   * as specified by the user via `Materialized` parameter, and is queryable through its given name.
   *
   * @param valueTransformerWithKeySupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   *                                 At least one transformer instance will be created per streaming task.
   *                                 Transformer implementations doe not need to be thread-safe.
   * @param materialized             an instance of `Materialized` used to describe how the state store of the
   *                                 resulting table should be materialized.
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](valueTransformerWithKeySupplier: ValueTransformerWithKeySupplier[K, V, VR],
                          materialized: Materialized[K, VR, KeyValueStore[Bytes, Array[Byte]]],
                          stateStoreNames: String*): KTable[K, VR] =
    inner.transformValues[VR](valueTransformerWithKeySupplier, materialized, stateStoreNames: _*)

  /**
   * Re-groups the records of this [[KTable]] using the provided key/value mapper
   * and `Serde`s as specified by `Grouped`.
   *
   * @param selector      a function that computes a new grouping key and value to be aggregated
   * @param grouped       the `Grouped` instance used to specify `Serdes`
   * @return a [[KGroupedTable]] that contains the re-grouped records of the original [[KTable]]
   * @see `org.apache.kafka.streams.kstream.KTable#groupBy`
   */
  def groupBy[KR, VR](selector: (K, V) => (KR, VR))(implicit grouped: Grouped[KR, VR]): KGroupedTable[KR, VR] =
    inner.groupBy(selector.asKeyValueMapper, grouped)

  /**
   * Join records of this [[KTable]] with another [[KTable]]'s records using non-windowed inner equi join.
   *
   * @param other  the other [[KTable]] to be joined with this [[KTable]]
   * @param joiner a function that computes the join result for a pair of matching records
   * @return a [[KTable]] that contains join-records for each key and values computed by the given joiner,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KTable#join`
   */
  def join[VO, VR](other: KTable[K, VO])(joiner: (V, VO) => VR): KTable[K, VR] =
    inner.join[VO, VR](other.inner, joiner.asValueJoiner)

  /**
   * Join records of this [[KTable]] with another [[KTable]]'s records using non-windowed inner equi join.
   *
   * @param other  the other [[KTable]] to be joined with this [[KTable]]
   * @param joiner a function that computes the join result for a pair of matching records
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains join-records for each key and values computed by the given joiner,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KTable#join`
   */
  def join[VO, VR](other: KTable[K, VO], materialized: Materialized[K, VR, ByteArrayKeyValueStore])(
    joiner: (V, VO) => VR
  ): KTable[K, VR] =
    inner.join[VO, VR](other.inner, joiner.asValueJoiner, materialized)

  /**
   * Join records of this [[KTable]] with another [[KTable]]'s records using non-windowed left equi join.
   *
   * @param other  the other [[KTable]] to be joined with this [[KTable]]
   * @param joiner a function that computes the join result for a pair of matching records
   * @return a [[KTable]] that contains join-records for each key and values computed by the given joiner,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KTable#leftJoin`
   */
  def leftJoin[VO, VR](other: KTable[K, VO])(joiner: (V, VO) => VR): KTable[K, VR] =
    inner.leftJoin[VO, VR](other.inner, joiner.asValueJoiner)

  /**
   * Join records of this [[KTable]] with another [[KTable]]'s records using non-windowed left equi join.
   *
   * @param other  the other [[KTable]] to be joined with this [[KTable]]
   * @param joiner a function that computes the join result for a pair of matching records
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains join-records for each key and values computed by the given joiner,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KTable#leftJoin`
   */
  def leftJoin[VO, VR](other: KTable[K, VO], materialized: Materialized[K, VR, ByteArrayKeyValueStore])(
    joiner: (V, VO) => VR
  ): KTable[K, VR] =
    inner.leftJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)

  /**
   * Join records of this [[KTable]] with another [[KTable]]'s records using non-windowed outer equi join.
   *
   * @param other  the other [[KTable]] to be joined with this [[KTable]]
   * @param joiner a function that computes the join result for a pair of matching records
   * @return a [[KTable]] that contains join-records for each key and values computed by the given joiner,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KTable#leftJoin`
   */
  def outerJoin[VO, VR](other: KTable[K, VO])(joiner: (V, VO) => VR): KTable[K, VR] =
    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner)

  /**
   * Join records of this [[KTable]] with another [[KTable]]'s records using non-windowed outer equi join.
   *
   * @param other  the other [[KTable]] to be joined with this [[KTable]]
   * @param joiner a function that computes the join result for a pair of matching records
   * @param materialized  a `Materialized` that describes how the `StateStore` for the resulting [[KTable]]
   *                      should be materialized.
   * @return a [[KTable]] that contains join-records for each key and values computed by the given joiner,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KTable#leftJoin`
   */
  def outerJoin[VO, VR](other: KTable[K, VO], materialized: Materialized[K, VR, ByteArrayKeyValueStore])(
    joiner: (V, VO) => VR
  ): KTable[K, VR] =
    inner.outerJoin[VO, VR](other.inner, joiner.asValueJoiner, materialized)

  /**
   * Get the name of the local state store used that can be used to query this [[KTable]].
   *
   * @return the underlying state store name, or `null` if this [[KTable]] cannot be queried.
   */
  def queryableStoreName: String = inner.queryableStoreName
}
