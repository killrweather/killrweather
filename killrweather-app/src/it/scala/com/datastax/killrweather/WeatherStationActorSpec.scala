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

import org.joda.time.{DateTimeZone, DateTime}

import akka.actor._

/** This test requires that you have already run these in the cql shell:
  * cqlsh> source 'create-timeseries.cql';
  * cqlsh> source 'load-timeseries.cql';
  *
  * See: https://github.com/killrweather/killrweather/wiki/2.%20Code%20and%20Data%20Setup#data-setup
  */
class WeatherStationActorSpec extends ActorSparkSpec {
  import WeatherEvent._
  import Weather._

  start(clean = false)

  val weatherStations = system.actorOf(Props(new WeatherStationActor(sc, settings)), "weather-station")

  "WeatherStationActor" must {
    "return a weather station" in {
      weatherStations ! GetWeatherStation(sample.wsid)
      expectMsgPF() {
        case e: WeatherStation =>
          e.id should be(sample.wsid)
      }
    }
    "get the current weather for a given weather station, based on UTC" in {
      val timstamp = new DateTime(DateTimeZone.UTC).withYear(sample.year).withMonthOfYear(sample.month).withDayOfMonth(sample.day)
      weatherStations ! GetCurrentWeather(sample.wsid, Some(timstamp))
      expectMsgPF() {
        case Some(e) =>
          e.asInstanceOf[RawWeatherData].wsid should be(sample.wsid)
      }
    }
  }
}