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
package com.datastax.killrweather.compute

import com.datastax.killrweather.WeatherEvent.GetMonthlyTemperature

import scala.concurrent.duration._
import akka.actor._
import org.joda.time.{DateTime, DateTimeZone}
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather._

trait TemperatureSpec extends ActorSparkSpec {

  val year = 2005

  val sid = "010010:99999"

  val expected = 19703 // the total count stations

  val startAt = new DateTime(DateTimeZone.UTC).withMonthOfYear(12).withDayOfMonth(1).getDayOfYear
}

class DailyTemperatureActorSpec extends TemperatureSpec {
  import WeatherEvent._
  import settings._

  "DailyTemperatureActor" must {
    "transform raw data from cassandra to daily temperatures and persist in new daily temp table" in {
      system.eventStream.subscribe(self, classOf[DailyTemperatureTaskCompleted]) // for test purposes

      val dailyTemperatures = system.actorOf(Props(new DailyTemperatureActor(ssc, settings)))
      dailyTemperatures ! ComputeDailyTemperature(sid, year, constraint = Some(startAt))

      expectMsgPF(3.minutes) {
        case DailyTemperatureTaskCompleted(actor, yr) =>
          actor.path should be (dailyTemperatures.path)
          yr should be (year)
          val rows = ssc.cassandraTable(CassandraKeyspace, CassandraTableDailyTemp)
            .where("weather_station = ?", sid).collect
          rows.size should be > 0
          rows foreach println
      }
    }
  }
}

class TemperatureActorSpec extends TemperatureSpec {
  import WeatherEvent._

  "TemperatureActor" must {
    "compute daily temperature rollups per weather station to monthly statistics." in {
      val temperature = system.actorOf(Props(new TemperatureActor(ssc, settings)))
      temperature ! GetMonthlyTemperature(sid, 12, year)
      expectMsgPF(timeout.duration) {
        case e =>
          val temps = e.asInstanceOf[Seq[Temperature]]
          temps.forall(t => t.month == 12 && t.year == year && t.weather_station == sid)
          temps.map(_.day).min should be (1)
          temps.map(_.day).max should be (31)
          temps foreach println
      }
    }//temperature ! GetTemperature(sid, 10, 2005)
  }
}


class TemporaryReceiver(year: Int, temperature: ActorRef, precipitation: ActorRef, weatherStation: ActorRef)
  extends Actor with ActorLogging {
  import com.datastax.killrweather.Weather
  import WeatherEvent._

  var received = 0
  val expected = 3

  def receive: Actor.Receive = {
    case StartValidation =>
      temperature ! GetTemperature("010010:99999", 10, year)
      precipitation ! GetPrecipitation("010000:99999", year)
      weatherStation ! GetWeatherStation("13280:99999")

    case e: WeatherEvent.WeatherAggregate =>
      log.info(s"Received $e"); received += 1; test()
    case e: Weather.WeatherStation =>
      log.info(s"Received $e"); received += 1; test()
  }

  def test(): Unit =
    if (received == expected) context.parent ! ValidationCompleted
}

