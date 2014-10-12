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
package com.datastax.killrweather.api

import akka.actor.{ActorRef, ActorSystem}
import org.json4s.Formats
import com.datastax.killrweather._
import com.datastax.killrweather.syntax.future._
import com.datastax.killrweather.syntax.json._

class WeatherCenterServlet(api: WeatherDataActorApi) extends WeatherServlet {
  import WeatherEvent._

  override def jsonFormats: Formats = apiFormats

  get("/v1/weather/climatology/temperature") {
    val sid = stationIdOrHalt(request)
    val month = monthParam(params)
    val year = yearParam(params)
    api.temperature(GetTemperature(sid.value, month, year)).run.valueOrThrow
  }

  get("/v1/weather/stations") {
    val sid = stationIdOrHalt(request)
    api.weatherStation(GetWeatherStation(sid.value)).run.valueOrThrow
  }
}

class WeatherDataActorApi(system: ActorSystem, guardian: ActorRef) {
  import scala.concurrent.duration._
  import akka.pattern.ask
  import akka.util.Timeout
  import WeatherEvent._
  import Weather._
  import system.dispatcher

  implicit val timeout = Timeout(5.seconds)

  /** Returns a summary of the weather for the next 3 days.
    * This includes high and low temperatures, a string text forecast and the conditions.
    * @param param the paramaters for high-low forecast by location
    */
  def temperature(param: GetTemperature): FutureT[Temperature] =
    (guardian ? param).mapTo[Temperature].eitherT


  def weatherStation(station: GetWeatherStation): FutureT[Seq[WeatherStation]] =
    (guardian ? station).mapTo[Seq[WeatherStation]].eitherT
}