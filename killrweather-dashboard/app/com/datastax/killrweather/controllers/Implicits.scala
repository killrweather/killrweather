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

import play.api.libs.functional.syntax._
import play.api.libs.json.{Json, JsPath, Writes}
import com.datastax.killrweather.Weather.WeatherStation
import com.datastax.killrweather.service.{WeatherStationInfo, DayPrecipitation}

object Implicits {
  implicit val weatherStationWrites: Writes[WeatherStation] = Json.format[WeatherStation]

  implicit val dailyPrecipitationWrites: Writes[DayPrecipitation] = Json.format[DayPrecipitation]

  implicit val weatherStationInfoWrites: Writes[WeatherStationInfo] = (
    (JsPath \ "weatherStation").write[WeatherStation] and
    (JsPath \ "dailyPrecipitation").write[Seq[DayPrecipitation]])(unlift(WeatherStationInfo.unapply))
}
