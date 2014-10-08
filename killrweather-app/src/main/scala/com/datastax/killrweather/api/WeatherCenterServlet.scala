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

/* not using yet
import scalaz.contrib.std.scalaFuture._
import scalaz.std.list._
import scalaz.syntax.traverse._
import scalaz.syntax.monad._
import scalaz._
import akka.pattern.ask
import org.json4s._
import org.json4s.native.JsonParser
*/

import akka.actor.{ActorRef, ActorSystem}
import com.datastax.killrweather.api.WeatherApi._

class WeatherCenterServlet(api: WeatherDataActorApi) extends TimeseriesServlet {

  /** Sample: /v1/weather/climatology/10023?dayofyear=92 */
  get("/v1/weather/climatology/high-low/:zipcode") {
    val zipcode = zipcodeParam(params) getOrElse halt(status = 400, body = "No Zipcode was provided")
    val dayofyear = dayOfYearParam(params)
    api.hilow(GetTemperatureAggregate(zipcode, dayofyear)).run.valueOrThrow
  }

  /** Sample: /v1/weather/stations/s/010010:99999 */
  get("/v1/weather/stations") {
    val stationId = stationIdOrHalt(request)
    api.weatherStation(GetWeatherStation(stationId)).run.valueOrThrow
  }
}

class WeatherDataActorApi(system: ActorSystem, guardian: ActorRef) {

  import scala.concurrent.duration._
  import akka.pattern.ask
  import akka.util.Timeout
  import com.datastax.killrweather._
  import Weather._
  import system.dispatcher

  implicit val timeout = Timeout(5.seconds)

  /** Returns a summary of the weather for the next 3 days.
    * This includes high and low temperatures, a string text forecast and the conditions.
    * @param hiLow the paramaters for high-low forecast by location
    */
  def hilow(hiLow: GetTemperatureAggregate): FutureT[HiLowForecast] =
    (guardian ? hiLow).mapTo[HiLowForecast].eitherT


  def weatherStation(station: GetWeatherStation): FutureT[WeatherStation] =
    (guardian ? station).mapTo[WeatherStation].eitherT
}