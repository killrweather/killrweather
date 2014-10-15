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

class KafkaSupervisorSpec extends ActorSparkSpec {
  import WeatherEvent._
  import settings._

  implicit val ec = system.dispatcher

  val sid = "010010:99999"

  val year = 2005

  lazy val kafkaServer = new EmbeddedKafka

  kafkaServer.createTopic(settings.KafkaTopicRaw)

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

  val latch = new CountDownLatch(1000)

  val kafkaActor = system.actorOf(Props(new KafkaSupervisor(kafkaServer, ssc, settings, self)), "kafka")

  expectMsgPF(1.minute) {
    case OutputStreamInitialized => start()
  }

  val consumer = new KafkaTestConsumer(kafkaServer.kafkaConfig.zkConnect, KafkaGroupId, KafkaTopicRaw, 2, latch)

  "KafkaSupervisor" must {
    ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw).collect.size should be (0)

    "RawDataPublisher: transforms annual weather .gz files to line data and publish to a Kafka topic" in {
      kafkaActor ! PublishFeed

      latch.await(5.minutes.toMillis, TimeUnit.MILLISECONDS)
      awaitCond(latch.getCount == 0) // we have at least 100 messages in kafka
    }
    "KafkaStreamActor: streams in data from kafka, transforms it, and saves it to Cassandra" in {
      // while streaming gets going
      awaitCond(ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw).count() > 0, 60.seconds)

      // assert there is now a day of data for the given wsid, for year, month, day, from kafka stream
      val rows = ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw)
        .where("weather_station = ? AND year = ? and month = ? and day = ?", sid, year, 1, 1)
        .toLocalIterator

      rows.size should be >= (20)
    }
  }

  override def afterAll() {
    super.afterAll()
    consumer.shutdown()
    kafkaServer.shutdown()
    Thread.sleep(2000) // hrm, no clean shutdown found yet that doesn't throw
  }
}