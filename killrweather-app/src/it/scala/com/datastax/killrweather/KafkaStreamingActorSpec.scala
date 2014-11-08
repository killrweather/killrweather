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

import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.concurrent.duration._
import akka.actor.Props
import com.datastax.spark.connector.embedded.{EmbeddedKafka, KafkaConsumer}
import com.datastax.spark.connector.streaming._

class KafkaStreamingActorSpec extends ActorSparkSpec {
  import WeatherEvent._
  import settings._

  /* Must be updated with different /data/load files */
  val expected = 8220

  val kafka = new EmbeddedKafka

  // AdminUtils.createTopic(kafka.client, KafkaTopicRaw, partitions = 1, replicationFactor = 1, new Properties())
  kafka.createTopic(KafkaTopicRaw)

  val ssc = new StreamingContext(sc, Seconds(SparkStreamingBatchInterval))

  override val kafkaActor = Some(system.actorOf(Props(new KafkaStreamingActor(
    kafka.kafkaParams, kafka.kafkaConfig, ssc, settings, self)), "kafka-stream")) //.withDispatcher("killrweather.kafka-dispatcher")

  val latch = new CountDownLatch(expected)

  val consumer = new KafkaConsumer(kafka.kafkaConfig.zkConnect, KafkaTopicRaw, KafkaGroupId, 1, latch)

  override def start(): Unit = {
    ssc.start()
    super.start()
  }

  expectMsgPF(20.seconds) {
    case OutputStreamInitialized => start()
  }

  "KafkaStreamingActor" must {
    "transforms annual weather .gz files to line data and publish to a Kafka topic" in {
      awaitCond(latch.getCount >= 5000, 30.seconds) // assert process of publishing has started, continues to consume
      println(s"Found ${latch.getCount} entries in kafka")
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableRaw" in {
      val tableData = ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw)
      awaitCond(tableData.toLocalIterator.size == expected, 30.seconds)
    }
    s"streams in data from kafka, transforms it, and saves it to $CassandraTableDailyPrecip" in {
      val tableData = ssc.cassandraTable(CassandraKeyspace, CassandraTableDailyPrecip)
      awaitCond(tableData.toLocalIterator.size > 100, 10.seconds)
      tableData.saveAsTextFile(s"./test-output/daily-precipitation-${now.toMillis}")
    }
    "consecutive reads on a stream after different computations writing to different tables should still have the raw data" in {
      val year = HistoricDataYearRange.head
      val tableData = ssc.cassandraTable(CassandraKeyspace, CassandraTableDailyPrecip)
      val january = tableData.where("wsid = ? and year = ? and month = ?", wsid, year, 1).collect()
      january.size should be (31)
      val december = tableData.where("wsid = ? and year = ? and month = ?", wsid, year, 12).collect()
      december.size should be (31)
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
