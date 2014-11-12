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
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicInteger

import com.datastax.spark.connector.util.Logging
import kafka.consumer.{Consumer, ConsumerConfig}
import kafka.serializer.StringDecoder

class KafkaConsumer(zookeeper: String, topic: String, groupId: String, partitions: Int, numThreads: Int, count: AtomicInteger)
  extends Logging {

  val connector = Consumer.create(createConsumerConfig)

  // create n partitions of the stream for topic “test”, to allow n threads to consume
  val streams = connector
    .createMessageStreams(Map(topic -> partitions), new StringDecoder(), new StringDecoder())
    .get(topic)

  // launch all the threads
  val executor = Executors.newFixedThreadPool(numThreads)

  // consume the messages in the threads
  for(stream <- streams) {
    executor.submit(new Runnable() {
      def run() {
        for(s <- stream) {
          while(s.iterator.hasNext) {
            count.getAndIncrement
            log.debug(s"Consumer [count=$count] received: ${new String(s.iterator.next.message)}")
          }
        }
      }
    })
  }

  def createConsumerConfig: ConsumerConfig = {
    val props = new Properties()
    props.put("consumer.timeout.ms", "2000")
    props.put("zookeeper.connect", zookeeper)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "10")
    props.put("auto.commit.interval.ms", "1000")

    new ConsumerConfig(props)
  }

  def shutdown() {
    println("Consumer shutting down.")
    Option(connector) map (_.shutdown())
    Option(executor) map (_.shutdown())
  }
}