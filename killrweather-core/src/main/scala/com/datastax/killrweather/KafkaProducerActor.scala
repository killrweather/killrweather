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
import scalaz.concurrent.Task
import kafka.producer._

class KafkaProducerActor[K, V](brokers: Set[String], settings: Settings)
  extends Actor with ActorLogging {

  import KafkaEvent._
  import settings._

  val props: Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokers.mkString(","))
    props.put("serializer.class", KafkaEncoderFqcn)
    props.put("partitioner.class", KafkaPartitioner)
    props.put("producer.type", "async")
    props.put("request.required.acks", "1")
    props.put("batch.num.messages", KafkaBatchSendSize.toString)
    props
  }

  val config = new ProducerConfig(props)

  val producer = new Producer[K,V](config)

  override def postStop(): Unit = close()

  def receive = {
    case e: KafkaMessageEnvelope[K,V] => send(e)
  }

  /**
   * Sends the data, partitioned by key to the topic using an async producer.
   */
  def send(e: KafkaMessageEnvelope[K,V]): Unit = {
    val messages = e.messages map (m => KeyedMessage(e.topic, e.key, m))
    producer.send(messages: _*)
  }

  /**
   * Close API to close the producer pool connections to all Kafka brokers. Also closes
   * the zookeeper client connection if one exists
   */
  def close(): Task[Unit] = Task.delay {
   log.info("Shutting down producer.")
   producer.close()
  }
}