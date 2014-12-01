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
import org.joda.time.{DateTimeZone, DateTime}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector._
import com.datastax.spark.connector.embedded.{KafkaConsumer, EmbeddedKafka}

class NodeGuardianSpec extends ActorSparkSpec {
  import WeatherEvent._
  import Weather._
  import settings._

  // === Tweak as needed ===
  val year = 2008

  // The first test waits for at least n-messages published to kafka from file data
  val publishedToKafka = 8000

  val duration = 60.seconds

  // === Don't modify ===
  system.eventStream.subscribe(self, classOf[NodeInitialized])

  val atomic = new AtomicInteger(0)

  val kafka = new EmbeddedKafka
  kafka.createTopic(KafkaTopicRaw)

  val ssc = new StreamingContext(sc, Seconds(SparkStreamingBatchInterval))

  val consumer = new KafkaConsumer(kafka.kafkaConfig.zkConnect, KafkaTopicRaw, KafkaGroupId, 1, 10, atomic)

  val guardian = system.actorOf(Props(
    new NodeGuardian(ssc, kafka, settings)), "node-guardian")

  "NodeGuardian" must {
    "publish a NodeInitialized to the event stream on initialization" in {
      expectMsgPF(10.seconds) {
        case NodeInitialized =>
      }
    }
    "return a weather station" in {
      guardian ! GetWeatherStation(sample.wsid)
      expectMsgPF() {
        case e: WeatherStation =>
          e.id should be(sample.wsid)
      }
    }
    "get the current weather for a given weather station, based on UTC" in {
      val timstamp = new DateTime(DateTimeZone.UTC).withYear(sample.year).withMonthOfYear(sample.month).withDayOfMonth(sample.day)
      guardian ! GetCurrentWeather(sample.wsid, Some(timstamp))
      expectMsgPF() {
        case Some(e) =>
          e.asInstanceOf[RawWeatherData].wsid should be(sample.wsid)
      }
    }
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
    "computes and return a monthly aggregation to the requester." in {
      guardian ! GetPrecipitation(sample.wsid, sample.year)
      expectMsgPF(timeout.duration) {
        case a: AnnualPrecipitation =>
          a.wsid should be (sample.wsid)
          a.year should be (sample.year)
      }
    }
    "Return the top k temps for any station in a given year" in {
      guardian ! GetTopKPrecipitation(sample.wsid, sample.year, k = 10)
      expectMsgPF(timeout.duration) {
        case a: TopKPrecipitation =>
          a.wsid should be (sample.wsid)
          a.year should be (sample.year)
          a.top.size should be (10)
      }
    }
    "aggregate hourly wsid temperatures for a given day and year" in {
      guardian ! GetDailyTemperature(sample)
      expectMsgPF(timeout.duration) {
        case aggregate: DailyTemperature =>
          validate(Day(aggregate.wsid, aggregate.year, aggregate.month, aggregate.day))
      }
    }
    s"asynchronously store DailyTemperature data in $CassandraTableDailyTemp" in {
      val tableData = sc.cassandraTable[DailyTemperature](CassandraKeyspace, CassandraTableDailyTemp)
        .where("wsid = ? AND year = ? AND month = ? AND day = ?",
          sample.wsid, sample.year, sample.month, sample.day)

      awaitCond(tableData.toLocalIterator.toSeq.headOption.nonEmpty, 10.seconds)
      val aggregate = tableData.toLocalIterator.toSeq.head
      validate(Day(aggregate.wsid, aggregate.year, aggregate.month, aggregate.day))
    }
    "compute daily temperature rollups per weather station to monthly statistics." in {
      guardian ! GetMonthlyHiLowTemperature(sample.wsid, sample.year, sample.month)
      expectMsgPF(timeout.duration) {
        case aggregate: MonthlyTemperature =>
          validate(Day(aggregate.wsid, aggregate.year, aggregate.month, sample.day))
      }
    }
  }

  override def afterAll() {
    super.afterAll()
    ssc.stop(true, false)
    consumer.shutdown()
    kafka.shutdown()
    Thread.sleep(2000) // hrm, no clean shutdown found yet that doesn't throw
  }

  def validate(aggregate: Day): Unit = {
    aggregate.wsid should be(sample.wsid)
    aggregate.year should be(sample.year)
    aggregate.month should be(sample.month)
    aggregate.day should be(sample.day)
  }
}