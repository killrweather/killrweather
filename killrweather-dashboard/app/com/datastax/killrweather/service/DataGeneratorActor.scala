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
package com.datastax.killrweather.service

import java.util.concurrent.TimeUnit

import scala.util.Random
import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Actor, ActorRef}
import play.api.Logger
import play.api.libs.concurrent.Execution.Implicits._
import com.datastax.killrweather.WeatherEvent.{LoadSpec, WeatherStationId}
import com.datastax.killrweather.DashboardProperties
import com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope

class DataGeneratorActor(kafkaProducer: ActorRef, weatherStation: WeatherStationId, random: Random) extends Actor with DashboardProperties {

  private val MaxHourlyPrecipitation = 5

  override def receive: Receive = {
    case loadSpec: LoadSpec =>
      val currentDate = loadSpec.startDate
      // this is the one the daily aggregate table is calculated from
      val hourlyPrecipitation = random.nextDouble() * MaxHourlyPrecipitation
      val rawData = s"$weatherStation,${currentDate.year().get()},${currentDate.monthOfYear().get()},${currentDate.dayOfMonth().get()},00,5.0,-3.9,1020.4,270,4.6,2,$hourlyPrecipitation,5.0"
      Logger.info(s"Sending message to Kafka, $loadSpec AND message $rawData")
      kafkaProducer ! KafkaMessageEnvelope[String, String](KafkaTopic, KafkaKey, rawData)
      if (currentDate.isAfter(loadSpec.endDate)) {
        Logger.info("Finished load generation. Closing down")
        context.stop(kafkaProducer)
        context.stop(self)
      } else {
        context.system.scheduler.scheduleOnce(FiniteDuration(loadSpec.interval.getMillis, TimeUnit.MILLISECONDS), self, loadSpec.copy(startDate = loadSpec.startDate.plusHours(1)))
      }
  }
}