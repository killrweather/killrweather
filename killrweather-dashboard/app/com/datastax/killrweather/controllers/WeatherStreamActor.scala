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
package com.datastax.killrweather.controllers

import scala.concurrent.duration._
import scala.language.postfixOps
import akka.actor.{Props, ActorLogging, Actor, ActorRef}
import akka.util.Timeout
import akka.pattern.ask
import play.Logger
import play.api.libs.json.Json
import play.api.libs.concurrent.Execution.Implicits.defaultContext
import com.datastax.killrweather.WeatherEvent.{WeatherUpdate, WeatherStationId, GetWeatherStationWithPrecipitation}
import com.datastax.killrweather.service.WeatherStationInfo

class WeatherStreamActor(apiActor: ActorRef, out: ActorRef, weatherStationId: WeatherStationId) extends Actor with ActorLogging {
  context.system.scheduler.schedule(5 seconds, 5 seconds, self, WeatherUpdate)

  import Implicits._

  implicit val timeout = Timeout(5 seconds)

  override def receive: Receive = {
    case WeatherUpdate =>
      weatherUpdate()

  }

  def weatherUpdate() = {
    val newPrecipitation = apiActor ? GetWeatherStationWithPrecipitation(weatherStationId)
    newPrecipitation.map({
      case weatherStationInfo@Some(station) =>
        val newWeatherData = Json.toJson(station.asInstanceOf[WeatherStationInfo]).toString()
        val eventMsg: String = s"""{"event":"weatherUpdate","data":$newWeatherData}"""
        log.info(s"Sending updated weather data $eventMsg")
        out ! eventMsg
      case None => log.warning(s"Looks like weather station has vanished $weatherStationId")
    })
  }

  override def postStop() = {
    Logger.info("WebSocket connection closed")
  }
}

object WeatherStreamActor {
  def props(api: ActorRef, out: ActorRef, weatherStationId: WeatherStationId) = Props(new WeatherStreamActor(api, out, weatherStationId))
}
