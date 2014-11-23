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

import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._
import akka.actor.Props
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.embedded.{KafkaConsumer, EmbeddedKafka}
import com.datastax.spark.connector.streaming._

class KafkaStreamingActorSpec extends ActorSparkSpec {
  import WeatherEvent._
  import settings._

  // === Tweak as needed ===
  val year = 2008

  // The first test waits for at least n-messages published to kafka from file data
  val publishedToKafka = 8000

  val duration = 60.seconds

  // === Don't modify ===
  val atomic = new AtomicInteger(0)

  val kafka = new EmbeddedKafka

  kafka.createTopic(KafkaTopicRaw)

  val ssc = new StreamingContext(sc, Seconds(SparkStreamingBatchInterval))

  override val kafkaActor = Some(system.actorOf(Props(new KafkaStreamingActor(
    kafka.kafkaParams, ssc, settings, self)), "kafka-stream")) //.withDispatcher("killrweather.kafka-dispatcher")

  val consumer = new KafkaConsumer(kafka.kafkaConfig.zkConnect, KafkaTopicRaw, KafkaGroupId, 1, 10, atomic)

  start(clean = true)

  expectMsgPF(20.seconds) {
    case OutputStreamInitialized => ssc.start()
  }

  "KafkaStreamingActor" must {
    "transforms annual weather .gz files to line data and publish to a Kafka topic" in {
      awaitCond(atomic.get >= publishedToKafka, duration) // assert process of publishing has started, continues to consume
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableRaw" in {
      val tableData = ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw)
      awaitCond(tableData.count >= publishedToKafka, duration)
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableDailyPrecip" in {
      val tableData = ssc.cassandraTable(CassandraKeyspace, CassandraTableDailyPrecip)
      awaitCond(tableData.toLocalIterator.size > 500, duration)
    }
    "consecutive reads on a stream after different computations writing to different tables should still have the raw data" in {
      val tableData = ssc.cassandraTable(CassandraKeyspace, CassandraTableDailyPrecip)
      awaitCond(tableData.toLocalIterator.size > 500, duration)
    }
  }

  override def afterAll() {
    super.afterAll()
    ssc.stop(true, false)
    consumer.shutdown()
    kafka.shutdown()
    Thread.sleep(2000) // hrm, no clean shutdown found yet that doesn't throw
  }
}