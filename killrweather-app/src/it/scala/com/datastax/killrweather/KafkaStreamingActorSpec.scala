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

import java.util.concurrent.CountDownLatch

import scala.concurrent.duration._
import akka.actor.Props
import com.datastax.spark.connector.embedded._
import com.datastax.spark.connector.streaming._

class KafkaStreamingActorSpec extends ActorSparkSpec { 
  import WeatherEvent._
  import settings._

  /* Must be updated with different /data/load files */
  val expected = 8220

  val kafka = new EmbeddedKafka

  // AdminUtils.createTopic(kafka.client, KafkaTopicRaw, partitions = 1, replicationFactor = 1, new Properties())
  kafka.createTopic(KafkaTopicRaw)

  val brokers = Set(s"${kafka.kafkaConfig.hostName}:${kafka.kafkaConfig.port}")

  override val kafkaActor = Some(system.actorOf(Props(new KafkaStreamingActor(
    kafka.kafkaParams, brokers, ssc, settings, self)), "kafka"))

  val latch = new CountDownLatch(expected)
  val consumer = new KafkaTestConsumer(kafka.kafkaConfig.zkConnect, KafkaTopicRaw, KafkaGroupId, 1, latch)

  expectMsgPF(20.seconds) {
    case OutputStreamInitialized => start()
  }

  "KafkaStreamingActor" must {
    "transforms annual weather .gz files to line data and publish to a Kafka topic" in {
      awaitCond(latch.getCount > 500, 3.seconds) // assert process of publishing has started
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableRaw" in {
      val tableData = ssc.cassandraTable(keyspace, CassandraTableRaw)
      awaitCond(tableData.toLocalIterator.size == expected, 10.seconds)
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableDailyPrecip" in {
      val tableData = ssc.cassandraTable(keyspace, CassandraTableDailyPrecip)
      awaitCond(tableData.toLocalIterator.size > 100, 10.seconds)
      tableData.saveAsTextFile(s"./test-output/daily-precipitation-${now.toMillis}")
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableDailyTemp" in {
      val tableData = ssc.cassandraTable(keyspace, CassandraTableDailyTemp)
      awaitCond(tableData.toLocalIterator.size > 100, 10.seconds)
      tableData.saveAsTextFile(s"./test-output/daily-temperature-${now.toMillis}")
    }
  }

  override def afterAll() {
    super.afterAll()
    consumer.shutdown()
    kafka.shutdown()
    Thread.sleep(2000) // hrm, no clean shutdown found yet that doesn't throw
  }
}
