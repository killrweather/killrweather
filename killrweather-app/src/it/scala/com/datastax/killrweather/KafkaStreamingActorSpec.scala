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

import java.util.concurrent.{TimeUnit, CountDownLatch}

import com.datastax.spark.connector.cql.CassandraConnector

import scala.concurrent.duration._
import akka.actor.Props
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.spark.connector.streaming._

class KafkaStreamingActorSpec extends ActorSparkSpec {
  import WeatherEvent._
  import settings._

  implicit val ec = system.dispatcher

  val sid = "010010:99999"

  val year = 2005

  lazy val kafka = new EmbeddedKafka

  kafka.createTopic(settings.KafkaTopicRaw)

  // fail fast if you have not run the create and load cql scripts yet :)
  CassandraConnector(conf).withSessionDo { session =>
    // insure for test we are not going to look at existing data, but new from the kafka actor processes
    session.execute(s"DROP TABLE IF EXISTS $CassandraKeyspace.raw_weather_data")
    session.execute(s"""CREATE TABLE IF NOT EXISTS $CassandraKeyspace.raw_weather_data (
      weather_station text, year int, month int, day int, hour int,
      temperature double, dewpoint double, pressure double, wind_direction int, wind_speed double,
      sky_condition int, sky_condition_text text, one_hour_precip double, six_hour_precip double,
      PRIMARY KEY ((weather_station), year, month, day, hour)
     ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC)""")
  }

  system.actorOf(Props(new KafkaStreamingActor(
    kafka.kafkaConfig, kafka.kafkaParams, ssc, settings, self)), "kafka")

  expectMsgPF(1.minute) {
    case OutputStreamInitialized => start()
  }

  "KafkaStreamingActor" must {
    "transforms annual weather .gz files to line data and publish to a Kafka topic" in {
      val latch = new CountDownLatch(1000)
      val consumer = new KafkaTestConsumer(kafka.kafkaConfig.zkConnect, KafkaGroupId, KafkaTopicRaw, 2, latch)
      latch.await(5.minutes.toMillis, TimeUnit.MILLISECONDS)
      // asserts raw data has started to be published to kafka
      awaitCond(latch.getCount == 0)
      log.info(s"\n\nSuccessfully read all data from Kafka.")
      consumer.shutdown()
    }
    "streams in data from kafka, transforms it, and saves it to Cassandra" in {
      awaitCond(ssc.cassandraTable(
        settings.CassandraKeyspace, settings.CassandraTableDailyTemp)
        .toLocalIterator.size > 100)

      log.debug(s"\n\nSuccessfully read all data from $CassandraTableRaw.")

      awaitCond(ssc.cassandraTable(
        settings.CassandraKeyspace, settings.CassandraTableDailyPrecip)
        .toLocalIterator.size > 100)

      log.debug(s"\n\nSuccessfully read all data from $CassandraTableDailyPrecip")

    }
  }

  override def afterAll() {
    super.afterAll()
    kafka.shutdown()
    Thread.sleep(2000) // hrm, no clean shutdown found yet that doesn't throw
  }
}