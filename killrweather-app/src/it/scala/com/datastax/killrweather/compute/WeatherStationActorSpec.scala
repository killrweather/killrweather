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

import akka.testkit.TestProbe

import scala.concurrent.duration._
import akka.actor._
import com.datastax.killrweather._

class WeatherStationActorSpec extends ActorSparkSpec {
  import com.datastax.killrweather.WeatherEvent._

  val sid = "252860:99999"

  val expected = 19703 // the total count stations

  val temp = TestProbe().ref
  val precip = TestProbe().ref
  val station = system.actorOf(Props(new WeatherStationActor(ssc, settings, temp, precip)), "weather-station")

  start()

  "WeatherStationActor" must {
   "future.collectAsync: return a  weather station id" in {
      station ! PublishWeatherStationIds
      val start = System.currentTimeMillis()
      expectMsgPF() {
        case e: WeatherStationIds => e.sids.size should be(expected)
      }
      println((System.currentTimeMillis() - start).seconds)
    }
   "streamWeatherStationIds: return weather station ids" in {
      val start = System.currentTimeMillis()
      station ! StreamWeatherStationIds
      val wstations = receiveWhile(messages = expected) {
        case e: WeatherStationIds => e
      }
      println((System.currentTimeMillis() - start).seconds)
      wstations.size should be(expected)
    }
    "return a weather station" in {
      station ! GetWeatherStation(sid)
      expectMsgPF() {
        case e: Weather.WeatherStation =>
          log.info(s"Received weather station: $e")
          e.id should be(sid)
      }
    }
  }
}