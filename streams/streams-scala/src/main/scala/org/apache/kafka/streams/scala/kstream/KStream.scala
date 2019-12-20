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

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.kstream.{
  GlobalKTable,
  JoinWindows,
  Printed,
  TransformerSupplier,
  ValueTransformerSupplier,
  ValueTransformerWithKeySupplier,
  KStream => KStreamJ
}
import org.apache.kafka.streams.processor.{Processor, ProcessorSupplier, TopicNameExtractor}
import org.apache.kafka.streams.scala.FunctionsCompatConversions.{
  FlatValueMapperFromFunction,
  FlatValueMapperWithKeyFromFunction,
  ForeachActionFromFunction,
  KeyValueMapperFromFunction,
  MapperFromFunction,
  PredicateFromFunction,
  TransformerSupplierAsJava,
  ValueMapperFromFunction,
  ValueMapperWithKeyFromFunction,
  ValueTransformerSupplierAsJava,
  ValueTransformerSupplierWithKeyAsJava
}

import scala.collection.JavaConverters._

/**
 * Wraps the Java class [[org.apache.kafka.streams.kstream.KStream KStream]] and delegates method calls to the
 * underlying Java object.
 *
 * @tparam K Type of keys
 * @tparam V Type of values
 * @param inner The underlying Java abstraction for KStream
 *
 * @see `org.apache.kafka.streams.kstream.KStream`
 */
class KStream[K, V](val inner: KStreamJ[K, V]) {

  /**
   * Create a new [[KStream]] that consists all records of this stream which satisfies the given predicate.
   *
   * @param predicate a filter that is applied to each record
   * @return a [[KStream]] that contains only those records that satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KStream#filter`
   */
  def filter(predicate: (K, V) => Boolean): KStream[K, V] =
    new KStream(inner.filter(predicate.asPredicate))

  /**
   * Create a new [[KStream]] that consists all records of this stream which do <em>not</em> satisfy the given
   * predicate.
   *
   * @param predicate a filter that is applied to each record
   * @return a [[KStream]] that contains only those records that do <em>not</em> satisfy the given predicate
   * @see `org.apache.kafka.streams.kstream.KStream#filterNot`
   */
  def filterNot(predicate: (K, V) => Boolean): KStream[K, V] =
    new KStream(inner.filterNot(predicate.asPredicate))

  /**
   * Set a new key (with possibly new type) for each input record.
   * <p>
   * The function `mapper` passed is applied to every record and results in the generation of a new
   * key `KR`. The function outputs a new [[KStream]] where each record has this new key.
   *
   * @param mapper a function `(K, V) => KR` that computes a new key for each record
   * @return a [[KStream]] that contains records with new key (possibly of different type) and unmodified value
   * @see `org.apache.kafka.streams.kstream.KStream#selectKey`
   */
  def selectKey[KR](mapper: (K, V) => KR): KStream[KR, V] =
    new KStream(inner.selectKey[KR](mapper.asKeyValueMapper))

  /**
   * Transform each record of the input stream into a new record in the output stream (both key and value type can be
   * altered arbitrarily).
   * <p>
   * The provided `mapper`, a function `(K, V) => (KR, VR)` is applied to each input record and computes a new output record.
   *
   * @param mapper a function `(K, V) => (KR, VR)` that computes a new output record
   * @return a [[KStream]] that contains records with new key and value (possibly both of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#map`
   */
  def map[KR, VR](mapper: (K, V) => (KR, VR)): KStream[KR, VR] =
    new KStream(inner.map[KR, VR](mapper.asKeyValueMapper))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * <p>
   * The provided `mapper`, a function `V => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper, a function `V => VR` that computes a new output value
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#mapValues`
   */
  def mapValues[VR](mapper: V => VR): KStream[K, VR] =
    new KStream(inner.mapValues[VR](mapper.asValueMapper))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * <p>
   * The provided `mapper`, a function `(K, V) => VR` is applied to each input record value and computes a new value for it
   *
   * @param mapper, a function `(K, V) => VR` that computes a new output value
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#mapValues`
   */
  def mapValues[VR](mapper: (K, V) => VR): KStream[K, VR] =
    new KStream(inner.mapValues[VR](mapper.asValueMapperWithKey))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * <p>
   * The provided `mapper`, function `(K, V) => Iterable[(KR, VR)]` is applied to each input record and computes zero or more output records.
   *
   * @param mapper function `(K, V) => Iterable[(KR, VR)]` that computes the new output records
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#flatMap`
   */
  def flatMap[KR, VR](mapper: (K, V) => Iterable[(KR, VR)]): KStream[KR, VR] = {
    val kvMapper = mapper.tupled.andThen(_.map(ImplicitConversions.tuple2ToKeyValue).asJava)
    new KStream(inner.flatMap[KR, VR](((k: K, v: V) => kvMapper(k, v)).asKeyValueMapper))
  }

  /**
   * Create a new [[KStream]] by transforming the value of each record in this stream into zero or more values
   * with the same key in the new stream.
   * <p>
   * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
   * stream (value type can be altered arbitrarily).
   * The provided `mapper`, a function `V => Iterable[VR]` is applied to each input record and computes zero or more output values.
   *
   * @param mapper a function `V => Iterable[VR]` that computes the new output values
   * @return a [[KStream]] that contains more or less records with unmodified keys and new values of different type
   * @see `org.apache.kafka.streams.kstream.KStream#flatMapValues`
   */
  def flatMapValues[VR](mapper: V => Iterable[VR]): KStream[K, VR] =
    new KStream(inner.flatMapValues[VR](mapper.asValueMapper))

  /**
   * Create a new [[KStream]] by transforming the value of each record in this stream into zero or more values
   * with the same key in the new stream.
   * <p>
   * Transform the value of each input record into zero or more records with the same (unmodified) key in the output
   * stream (value type can be altered arbitrarily).
   * The provided `mapper`, a function `(K, V) => Iterable[VR]` is applied to each input record and computes zero or more output values.
   *
   * @param mapper a function `(K, V) => Iterable[VR]` that computes the new output values
   * @return a [[KStream]] that contains more or less records with unmodified keys and new values of different type
   * @see `org.apache.kafka.streams.kstream.KStream#flatMapValues`
   */
  def flatMapValues[VR](mapper: (K, V) => Iterable[VR]): KStream[K, VR] =
    new KStream(inner.flatMapValues[VR](mapper.asValueMapperWithKey))

  /**
   * Print the records of this KStream using the options provided by `Printed`
   *
   * @param printed options for printing
   * @see `org.apache.kafka.streams.kstream.KStream#print`
   */
  def print(printed: Printed[K, V]): Unit = inner.print(printed)

  /**
   * Perform an action on each record of `KStream`
   *
   * @param action an action to perform on each record
   * @see `org.apache.kafka.streams.kstream.KStream#foreach`
   */
  def foreach(action: (K, V) => Unit): Unit =
    inner.foreach(action.asForeachAction)

  /**
   * Creates an array of `KStream` from this stream by branching the records in the original stream based on
   * the supplied predicates.
   *
   * @param predicates the ordered list of functions that return a Boolean
   * @return multiple distinct substreams of this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#branch`
   */
  //noinspection ScalaUnnecessaryParentheses
  def branch(predicates: ((K, V) => Boolean)*): Array[KStream[K, V]] =
    inner.branch(predicates.map(_.asPredicate): _*).map(kstream => new KStream(kstream))

  /**
   * Materialize this stream to a topic and creates a new [[KStream]] from the topic using the `Produced` instance for
   * configuration of the `Serde key serde`, `Serde value serde`, and `StreamPartitioner`
   * <p>
   * The user can either supply the `Produced` instance as an implicit in scope or she can also provide implicit
   * key and value serdes that will be converted to a `Produced` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KTable[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.through(topic)
   *
   * // Similarly you can create an implicit Produced and it will be passed implicitly
   * // to the through call
   * }}}
   *
   * @param topic the topic name
   * @param produced the instance of Produced that gives the serdes and `StreamPartitioner`
   * @return a [[KStream]] that contains the exact same (and potentially repartitioned) records as this [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#through`
   */
  def through(topic: String)(implicit produced: Produced[K, V]): KStream[K, V] =
    new KStream(inner.through(topic, produced))

  /**
   * Materialize this stream to a topic using the `Produced` instance for
   * configuration of the `Serde key serde`, `Serde value serde`, and `StreamPartitioner`
   * <p>
   * The user can either supply the `Produced` instance as an implicit in scope or she can also provide implicit
   * key and value serdes that will be converted to a `Produced` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KTable[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.to(topic)
   *
   * // Similarly you can create an implicit Produced and it will be passed implicitly
   * // to the through call
   * }}}
   *
   * @param topic the topic name
   * @param produced the instance of Produced that gives the serdes and `StreamPartitioner`
   * @see `org.apache.kafka.streams.kstream.KStream#to`
   */
  def to(topic: String)(implicit produced: Produced[K, V]): Unit =
    inner.to(topic, produced)

  /**
   * Dynamically materialize this stream to topics using the `Produced` instance for
   * configuration of the `Serde key serde`, `Serde value serde`, and `StreamPartitioner`.
   * The topic names for each record to send to is dynamically determined based on the given mapper.
   * <p>
   * The user can either supply the `Produced` instance as an implicit in scope or she can also provide implicit
   * key and value serdes that will be converted to a `Produced` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * //..
   * val clicksPerRegion: KTable[String, Long] = //..
   *
   * // Implicit serdes in scope will generate an implicit Produced instance, which
   * // will be passed automatically to the call of through below
   * clicksPerRegion.to(topicChooser)
   *
   * // Similarly you can create an implicit Produced and it will be passed implicitly
   * // to the through call
   * }}}
   *
   * @param extractor the extractor to determine the name of the Kafka topic to write to for reach record
   * @param produced the instance of Produced that gives the serdes and `StreamPartitioner`
   * @see `org.apache.kafka.streams.kstream.KStream#to`
   */
  def to(extractor: TopicNameExtractor[K, V])(implicit produced: Produced[K, V]): Unit =
    inner.to(extractor, produced)

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * A `Transformer` (provided by the given `TransformerSupplier`) is applied to each input record
   * and computes zero or more output records.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Transformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param transformerSupplier the `TransformerSuplier` that generates `Transformer`
   * @param stateStoreNames     the names of the state stores used by the processor
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transform`
   */
  def transform[K1, V1](transformerSupplier: TransformerSupplier[K, V, KeyValue[K1, V1]],
                        stateStoreNames: String*): KStream[K1, V1] =
    new KStream(inner.transform(transformerSupplier, stateStoreNames: _*))

  /**
   * Transform each record of the input stream into zero or more records in the output stream (both key and value type
   * can be altered arbitrarily).
   * A `Transformer` (provided by the given `TransformerSupplier`) is applied to each input record
   * and computes zero or more output records.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Transformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param transformerSupplier the `TransformerSuplier` that generates `Transformer`
   * @param stateStoreNames     the names of the state stores used by the processor
   * @return a [[KStream]] that contains more or less records with new key and value (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transform`
   */
  def flatTransform[K1, V1](transformerSupplier: TransformerSupplier[K, V, Iterable[KeyValue[K1, V1]]],
                            stateStoreNames: String*): KStream[K1, V1] =
    new KStream(inner.flatTransform(transformerSupplier.asJava, stateStoreNames: _*))

  /**
   * Transform the value of each input record into zero or more records (with possible new type) in the
   * output stream.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerSupplier` that generates a `ValueTransformer`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def flatTransformValues[VR](valueTransformerSupplier: ValueTransformerSupplier[V, Iterable[VR]],
                              stateStoreNames: String*): KStream[K, VR] =
    new KStream(inner.flatTransformValues[VR](valueTransformerSupplier.asJava, stateStoreNames: _*))

  /**
   * Transform the value of each input record into zero or more records (with possible new type) in the
   * output stream.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def flatTransformValues[VR](valueTransformerSupplier: ValueTransformerWithKeySupplier[K, V, Iterable[VR]],
                              stateStoreNames: String*): KStream[K, VR] =
    new KStream(inner.flatTransformValues[VR](valueTransformerSupplier.asJava, stateStoreNames: _*))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerSupplier` that generates a `ValueTransformer`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](valueTransformerSupplier: ValueTransformerSupplier[V, VR],
                          stateStoreNames: String*): KStream[K, VR] =
    new KStream(inner.transformValues[VR](valueTransformerSupplier, stateStoreNames: _*))

  /**
   * Transform the value of each input record into a new value (with possible new type) of the output record.
   * A `ValueTransformer` (provided by the given `ValueTransformerSupplier`) is applied to each input
   * record value and computes a new value for it.
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `ValueTransformer`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param valueTransformerSupplier a instance of `ValueTransformerWithKeySupplier` that generates a `ValueTransformerWithKey`
   * @param stateStoreNames          the names of the state stores used by the processor
   * @return a [[KStream]] that contains records with unmodified key and new values (possibly of different type)
   * @see `org.apache.kafka.streams.kstream.KStream#transformValues`
   */
  def transformValues[VR](valueTransformerSupplier: ValueTransformerWithKeySupplier[K, V, VR],
                          stateStoreNames: String*): KStream[K, VR] =
    new KStream(inner.transformValues[VR](valueTransformerSupplier, stateStoreNames: _*))

  /**
   * Process all records in this stream, one record at a time, by applying a `Processor` (provided by the given
   * `processorSupplier`).
   * In order to assign a state, the state must be created and added via `addStateStore` before they can be connected
   * to the `Processor`.
   * It's not required to connect global state stores that are added via `addGlobalStore`;
   * read-only access to global state stores is available by default.
   *
   * @param processorSupplier a function that generates a [[org.apache.kafka.streams.processor.Processor]]
   * @param stateStoreNames   the names of the state store used by the processor
   * @see `org.apache.kafka.streams.kstream.KStream#process`
   */
  def process(processorSupplier: () => Processor[K, V], stateStoreNames: String*): Unit = {
    //noinspection ConvertExpressionToSAM // because of the 2.11 build
    val processorSupplierJ: ProcessorSupplier[K, V] = new ProcessorSupplier[K, V] {
      override def get(): Processor[K, V] = processorSupplier()
    }
    inner.process(processorSupplierJ, stateStoreNames: _*)
  }

  /**
   * Group the records by their current key into a [[KGroupedStream]]
   * <p>
   * The user can either supply the `Grouped` instance as an implicit in scope or she can also provide an implicit
   * serdes that will be converted to a `Grouped` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * val clicksPerRegion: KTable[String, Long] =
   *   userClicksStream
   *     .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))
   *     .map((_, regionWithClicks) => regionWithClicks)
   *
   *     // the groupByKey gets the Grouped instance through an implicit conversion of the
   *     // serdes brought into scope through the import Serdes._ above
   *     .groupByKey
   *     .reduce(_ + _)
   *
   * // Similarly you can create an implicit Grouped and it will be passed implicitly
   * // to the groupByKey call
   * }}}
   *
   * @param grouped the instance of Grouped that gives the serdes
   * @return a [[KGroupedStream]] that contains the grouped records of the original [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#groupByKey`
   */
  def groupByKey(implicit grouped: Grouped[K, V]): KGroupedStream[K, V] =
    new KGroupedStream(inner.groupByKey(grouped))

  /**
   * Group the records of this [[KStream]] on a new key that is selected using the provided key transformation function
   * and the `Grouped` instance.
   * <p>
   * The user can either supply the `Grouped` instance as an implicit in scope or she can also provide an implicit
   * serdes that will be converted to a `Grouped` instance implicitly.
   * <p>
   * {{{
   * Example:
   *
   * // brings implicit serdes in scope
   * import Serdes._
   *
   * val textLines = streamBuilder.stream[String, String](inputTopic)
   *
   * val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)
   *
   * val wordCounts: KTable[String, Long] =
   *   textLines.flatMapValues(v => pattern.split(v.toLowerCase))
   *
   *     // the groupBy gets the Grouped instance through an implicit conversion of the
   *     // serdes brought into scope through the import Serdes._ above
   *     .groupBy((k, v) => v)
   *
   *     .count()
   * }}}
   *
   * @param selector a function that computes a new key for grouping
   * @return a [[KGroupedStream]] that contains the grouped records of the original [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#groupBy`
   */
  def groupBy[KR](selector: (K, V) => KR)(implicit grouped: Grouped[KR, V]): KGroupedStream[KR, V] =
    new KGroupedStream(inner.groupBy(selector.asKeyValueMapper, grouped))

  /**
   * Join records of this stream with another [[KStream]]'s records using windowed inner equi join with
   * serializers and deserializers supplied by the implicit `StreamJoined` instance.
   *
   * @param otherStream the [[KStream]] to be joined with this stream
   * @param joiner      a function that computes the join result for a pair of matching records
   * @param windows     the specification of the `JoinWindows`
   * @param streamJoin  an implicit `StreamJoin` instance that defines the serdes to be used to serialize/deserialize
   *                    inputs and outputs of the joined streams. Instead of `StreamJoin`, the user can also supply
   *                    key serde, value serde and other value serde in implicit scope and they will be
   *                    converted to the instance of `Stream` through implicit conversion.  The `StreamJoin` instance can
   *                    also name the repartition topic (if required), the state stores for the join, and the join
   *                    processor node.
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   * one for each matched record-pair with the same key and within the joining window intervals
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[VO, VR](otherStream: KStream[K, VO])(
    joiner: (V, VO) => VR,
    windows: JoinWindows
  )(implicit streamJoin: StreamJoined[K, V, VO]): KStream[K, VR] =
    new KStream(inner.join[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, streamJoin))

  /**
   * Join records of this stream with another [[KStream]]'s records using windowed left equi join with
   * serializers and deserializers supplied by the implicit `StreamJoined` instance.
   *
   * @param otherStream the [[KStream]] to be joined with this stream
   * @param joiner      a function that computes the join result for a pair of matching records
   * @param windows     the specification of the `JoinWindows`
   * @param streamJoin  an implicit `StreamJoin` instance that defines the serdes to be used to serialize/deserialize
   *                    inputs and outputs of the joined streams. Instead of `StreamJoin`, the user can also supply
   *                    key serde, value serde and other value serde in implicit scope and they will be
   *                    converted to the instance of `Stream` through implicit conversion.  The `StreamJoin` instance can
   *                    also name the repartition topic (if required), the state stores for the join, and the join
   *                    processor node.
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *                    one for each matched record-pair with the same key and within the joining window intervals
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[VO, VR](otherStream: KStream[K, VO])(
    joiner: (V, VO) => VR,
    windows: JoinWindows
  )(implicit streamJoin: StreamJoined[K, V, VO]): KStream[K, VR] =
    new KStream(inner.leftJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, streamJoin))

  /**
   * Join records of this stream with another [[KStream]]'s records using windowed outer equi join with
   * serializers and deserializers supplied by the implicit `Joined` instance.
   *
   * @param otherStream the [[KStream]] to be joined with this stream
   * @param joiner      a function that computes the join result for a pair of matching records
   * @param windows     the specification of the `JoinWindows`
   * @param streamJoin  an implicit `StreamJoin` instance that defines the serdes to be used to serialize/deserialize
   *                    inputs and outputs of the joined streams. Instead of `StreamJoin`, the user can also supply
   *                    key serde, value serde and other value serde in implicit scope and they will be
   *                    converted to the instance of `Stream` through implicit conversion.  The `StreamJoin` instance can
   *                    also name the repartition topic (if required), the state stores for the join, and the join
   *                    processor node.
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   * one for each matched record-pair with the same key and within the joining window intervals
   * @see `org.apache.kafka.streams.kstream.KStream#outerJoin`
   */
  def outerJoin[VO, VR](otherStream: KStream[K, VO])(
    joiner: (V, VO) => VR,
    windows: JoinWindows
  )(implicit streamJoin: StreamJoined[K, V, VO]): KStream[K, VR] =
    new KStream(inner.outerJoin[VO, VR](otherStream.inner, joiner.asValueJoiner, windows, streamJoin))

  /**
   * Join records of this stream with another [[KTable]]'s records using inner equi join with
   * serializers and deserializers supplied by the implicit `Joined` instance.
   *
   * @param table    the [[KTable]] to be joined with this stream
   * @param joiner   a function that computes the join result for a pair of matching records
   * @param joined   an implicit `Joined` instance that defines the serdes to be used to serialize/deserialize
   *                 inputs and outputs of the joined streams. Instead of `Joined`, the user can also supply
   *                 key serde, value serde and other value serde in implicit scope and they will be
   *                 converted to the instance of `Joined` through implicit conversion
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   * one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[VT, VR](table: KTable[K, VT])(joiner: (V, VT) => VR)(implicit joined: Joined[K, V, VT]): KStream[K, VR] =
    new KStream(inner.join[VT, VR](table.inner, joiner.asValueJoiner, joined))

  /**
   * Join records of this stream with another [[KTable]]'s records using left equi join with
   * serializers and deserializers supplied by the implicit `Joined` instance.
   *
   * @param table    the [[KTable]] to be joined with this stream
   * @param joiner   a function that computes the join result for a pair of matching records
   * @param joined   an implicit `Joined` instance that defines the serdes to be used to serialize/deserialize
   *                 inputs and outputs of the joined streams. Instead of `Joined`, the user can also supply
   *                 key serde, value serde and other value serde in implicit scope and they will be
   *                 converted to the instance of `Joined` through implicit conversion
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *                 one for each matched record-pair with the same key
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[VT, VR](table: KTable[K, VT])(joiner: (V, VT) => VR)(implicit joined: Joined[K, V, VT]): KStream[K, VR] =
    new KStream(inner.leftJoin[VT, VR](table.inner, joiner.asValueJoiner, joined))

  /**
   * Join records of this stream with `GlobalKTable`'s records using non-windowed inner equi join.
   *
   * @param globalKTable   the `GlobalKTable` to be joined with this stream
   * @param keyValueMapper a function used to map from the (key, value) of this stream
   *                       to the key of the `GlobalKTable`
   * @param joiner         a function that computes the join result for a pair of matching records
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *                       one output for each input [[KStream]] record
   * @see `org.apache.kafka.streams.kstream.KStream#join`
   */
  def join[GK, GV, RV](globalKTable: GlobalKTable[GK, GV])(
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV
  ): KStream[K, RV] =
    new KStream(
      inner.join[GK, GV, RV](
        globalKTable,
        ((k: K, v: V) => keyValueMapper(k, v)).asKeyValueMapper,
        ((v: V, gv: GV) => joiner(v, gv)).asValueJoiner
      )
    )

  /**
   * Join records of this stream with `GlobalKTable`'s records using non-windowed left equi join.
   *
   * @param globalKTable   the `GlobalKTable` to be joined with this stream
   * @param keyValueMapper a function used to map from the (key, value) of this stream
   *                       to the key of the `GlobalKTable`
   * @param joiner         a function that computes the join result for a pair of matching records
   * @return a [[KStream]] that contains join-records for each key and values computed by the given `joiner`,
   *                       one output for each input [[KStream]] record
   * @see `org.apache.kafka.streams.kstream.KStream#leftJoin`
   */
  def leftJoin[GK, GV, RV](globalKTable: GlobalKTable[GK, GV])(
    keyValueMapper: (K, V) => GK,
    joiner: (V, GV) => RV
  ): KStream[K, RV] =
    new KStream(inner.leftJoin[GK, GV, RV](globalKTable, keyValueMapper.asKeyValueMapper, joiner.asValueJoiner))

  /**
   * Merge this stream and the given stream into one larger stream.
   * <p>
   * There is no ordering guarantee between records from this `KStream` and records from the provided `KStream`
   * in the merged stream. Relative order is preserved within each input stream though (ie, records within
   * one input stream are processed in order).
   *
   * @param stream a stream which is to be merged into this stream
   * @return a merged stream containing all records from this and the provided [[KStream]]
   * @see `org.apache.kafka.streams.kstream.KStream#merge`
   */
  def merge(stream: KStream[K, V]): KStream[K, V] =
    new KStream(inner.merge(stream.inner))

  /**
   * Perform an action on each record of `KStream`.
   * <p>
   * Peek is a non-terminal operation that triggers a side effect (such as logging or statistics collection)
   * and returns an unchanged stream.
   *
   * @param action an action to perform on each record
   * @see `org.apache.kafka.streams.kstream.KStream#peek`
   */
  def peek(action: (K, V) => Unit): KStream[K, V] =
    new KStream(inner.peek(action.asForeachAction))
}
