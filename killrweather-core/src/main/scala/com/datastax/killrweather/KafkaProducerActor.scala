/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.killrweather

import java.util.Properties

import akka.actor.{ActorLogging, Actor}
import kafka.serializer.StringEncoder
import kafka.server.KafkaConfig
import kafka.producer._

/** Simple producer for an Akka Actor using string encoder and default partitioner. */
abstract class KafkaProducerActor[K, V](brokers: Set[String], settings: Settings)
  extends Actor with ActorLogging {
  import KafkaEvent._

  import settings._

  private val producerConfig: ProducerConfig = KafkaProducer.createConfig(
    brokers, KafkaBatchSendSize, "async", KafkaEncoderFqcn)

  private val producer = new KafkaProducer[K, V](producerConfig)

  override def postStop(): Unit = {
    log.info("Shutting down producer.")
    producer.close()
  }

  def receive = {
    case e: KafkaMessageEnvelope[K,V] => producer.send(e)
  }
}

/** Simple producer using string encoder and default partitioner. */
class KafkaProducer[K, V](producerConfig: ProducerConfig) {

  def this(brokers: Set[String], batchSize: Int, producerType: String, serializerFqcn: String) =
    this(KafkaProducer.createConfig(brokers, batchSize, producerType, serializerFqcn))

  def this(config: KafkaConfig) =
    this(KafkaProducer.defaultConfig(config))

  import KafkaEvent._

  private val producer = new Producer[K, V](producerConfig)

  /** Sends the data, partitioned by key to the topic. */
  def send(e: KafkaMessageEnvelope[K,V]): Unit =
    batchSend(e.topic, e.key, e.messages)

  /* Sends a single message. */
  def send(topic : String, key : K, message : V): Unit =
    batchSend(topic, key, Seq(message))

  def batchSend(topic: String, key: K, batch: Seq[V]): Unit = {
    val messages = batch map (msg => new KeyedMessage[K, V](topic, key, msg))
    producer.send(messages.toArray: _*)
  }

  def close(): Unit = producer.close()

}

object KafkaProducer {

  def createConfig(brokers: Set[String], batchSize: Int, producerType: String, serializerFqcn: String): ProducerConfig = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("serializer.class", serializerFqcn)
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("producer.type", producerType)
    props.put("request.required.acks", "1")
    props.put("batch.num.messages", batchSize.toString)
    new ProducerConfig(props)
  }

  def defaultConfig(config: KafkaConfig): ProducerConfig =
    createConfig(Set(s"${config.hostName}:${config.port}"), 100, "async", classOf[StringEncoder].getName)
}