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
package com.datastax.killrweather.actor

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.KafkaConfig

trait KafkaProducer extends WeatherActor {

  def config: KafkaConfig

  private val producer = {
    val props = new Properties()
    props.put("metadata.broker.list", config.hostName + ":" + config.port)
    /** Just using String Encoder to keep things simple for now. */
    props.put("serializer.class", classOf[StringEncoder].getName)
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    props.put("batch.num.messages", "100")

    new Producer[String, String](new ProducerConfig(props))
  }

  def send(topic : String, key : String, message : String): Unit =
    producer.send(KeyedMessage(topic, key, message))

  def batchSend(topic: String, group: String, batchSize: Int, lines: Seq[String]): Unit =
    if (lines.nonEmpty) {
      val (send, unsent) = lines.toSeq.splitAt(batchSize)
      val messages = send map (KeyedMessage(topic, group, _))
      producer.send(messages.toArray: _*)
      log.debug(s"Published messages to kafka topic '$topic'. Batching remaining ${unsent.size}")
      batchSend(topic, group, batchSize, unsent)
    }

  override def postStop(): Unit =
    producer.close()

}
