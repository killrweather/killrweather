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
import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import play.Logger
import play.api.Play.current
import play.api.libs.json.Json
import play.api.mvc.{Action, Controller, WebSocket}
import com.datastax.killrweather.Weather.WeatherStation
import com.datastax.killrweather.WeatherEvent.{GetWeatherStationWithPrecipitation, GetWeatherStations}
import com.datastax.killrweather.service.WeatherStationInfo


class WeatherController(dashboardApi: ActorRef) extends Controller {

  import com.datastax.killrweather.controllers.Implicits._
  import play.api.libs.concurrent.Execution.Implicits.defaultContext

  implicit val timeout = Timeout(5 seconds)

  def index = Action {
    Logger.info("Index")
    Redirect("/assets/index.html")
  }

  def stations = Action.async {
    Logger.info("Stations")
    (dashboardApi ? GetWeatherStations)
      .map(weatherStations => Ok(Json.toJson(weatherStations.asInstanceOf[Seq[WeatherStation]])))
  }

  def stream(id: String) = WebSocket.acceptWithActor[String, String] { request => out =>
    WeatherStreamActor.props(dashboardApi, out, id)
  }

  def station(id: String) = Action.async {
    val response = dashboardApi ? GetWeatherStationWithPrecipitation(id)
    val station = response.mapTo[Option[WeatherStationInfo]]
    station.map {
      case Some(s) => Ok(Json.toJson(s))
      case None => NotFound
    }
  }
}