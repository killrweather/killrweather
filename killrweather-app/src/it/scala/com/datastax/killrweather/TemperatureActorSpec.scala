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

import akka.actor.{ActorLogging, Actor, ActorRef, Props}
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.embedded.EmbeddedKafka

class TemperatureActorSpec extends ActorSparkSpec {

  import WeatherEvent._
  import settings._

  val year = 2005

  val sid = "252860:99999"

  val expected = 19703 // the total count stations

  lazy val kafka = new EmbeddedKafka
  Thread.sleep(1000)
  kafka.createTopic(settings.KafkaTopicRaw)
  ssc.checkpoint(SparkCheckpointDir)

  var wsids: Set[String] = Set.empty

  // transforms raw data from files and publishes to kafka topic
  // system.actorOf(Props(new RawDataPublisher(kafka.kafkaConfig, ssc, settings))) ! PublishFeed(DataYearRange)

  // reads from kafka stream, writes raw data to cassandra
  //system.actorOf(Props(new KafkaStreamActor(kafka, ssc, settings)))

  override def beforeAll() {
    val wsa = system.actorOf(Props(new WeatherStationActor(ssc, settings)))
    wsa ! GetWeatherStationIds
    expectMsgPF() { case e: WeatherStationIds => wsids = e.sids.toSet}
    system stop wsa
  }

  "DailyTemperatureActor" must {
    "transform raw data from cassandra to daily temperatures and persist in new daily temp table" in {
      val dailyTemperatures = system.actorOf(Props(new DailyTemperatureActor(ssc, settings)))
      dailyTemperatures ! ComputeDailyTemperature(sid, year)

      ssc.cassandraTable[Temperature](CassandraKeyspace, CassandraTableDailyTemp)
        .toLocalIterator foreach println
    }
  }
  /*
  "TemperatureActor" must {
    //val precipitation = system.actorOf(Props(new PrecipitationActor(ssc, settings)), "precipitation")

     "compute daily temperature rollups per weather station to monthly statistics." in {
       val temperature = system.actorOf(Props(new DailyTemperatureActor(ssc, settings)), "temperature")
         temperature ! GetTemperature(sid, 10, 2005)
           expectMsgPF(timeout.duration) {
             case Temperature(id, temp) =>
               id should be (sid)
           }
        }
     }
  }
 */
}

class TemporaryReceiver(year: Int, temperature: ActorRef, precipitation: ActorRef, weatherStation: ActorRef)
  extends Actor with ActorLogging {

  import com.datastax.killrweather.WeatherEvent._

  var received = 0
  val expected = 3

  def receive: Actor.Receive = {
    case StartValidation =>
      temperature ! GetTemperature("010010:99999", 10, year)
      precipitation ! GetPrecipitation("010000:99999", year)
      weatherStation ! GetWeatherStation("13280:99999")

    case e: WeatherEvent.WeatherAggregate =>
      log.info(s"\n\nReceived $e"); received += 1; test()
    case e: Weather.WeatherStation =>
      log.info(s"\n\nReceived $e"); received += 1; test()
  }

  def test(): Unit =
    if (received == expected) context.parent ! ValidationCompleted
}