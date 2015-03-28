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

import com.datastax.killrweather.WeatherStationId
import com.datastax.killrweather.infrastructure.WeatherStationDao
import com.datastax.killrweather.service.WeatherService
import play.Logger
import play.api.Play.current
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller, WebSocket}

import scala.language.postfixOps

class WeatherController(weatherStationDao: WeatherStationDao,
                        weatherService: WeatherService) extends Controller {

  import play.api.libs.concurrent.Execution.Implicits.defaultContext
  import Implicits._

  def index = Action {
    Logger.info("Index")
    Redirect("/assets/index.html")
  }

  def stations = Action.async {
    Logger.info("Stations")
    weatherStationDao.retrieveWeatherStations()
      .map(weatherStations => Ok(Json.toJson(weatherStations)))
  }

  def station(id: String) = Action.async {
    weatherService.getWeatherStation(WeatherStationId(id)).map {
        case weatherStationInfo @ Some(station) => Ok(Json.toJson(weatherStationInfo))
        case None => NotFound
      }
  }

  def stream(id: String) = WebSocket.acceptWithActor[String, String] { request => out =>
    WeatherStreamActor.props(weatherService, out, WeatherStationId(id))
  }
}