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

import com.datastax.killrweather.Weather.{DataRequest, WeatherModel}

object WeatherEvents {
  sealed trait LifeCycleEvent extends Serializable
  case class PublishFeed(years: Range) extends LifeCycleEvent
  case object Shutdown extends LifeCycleEvent
  case object TaskCompleted extends LifeCycleEvent
  case object StartValidation extends LifeCycleEvent
  case object ValidationCompleted extends LifeCycleEvent

  sealed trait WeatherEvent extends Serializable
  case class WordCount(word: String, count: Int)
  case class StreamingWordCount(time: Long, word: String, count: Int)

  /**
   * Quick access lookup table for sky_condition. Useful for potential analytics.
   * See http://en.wikipedia.org/wiki/Okta
   */
  case class SkyConditionLookup(code: Int, condition: String) extends WeatherModel

  /** Composite of Air Force Datsav3 station number and NCDC WBAN number
    * @param sid uses the composite key format: stationNum:wbanNum
    */
  case class GetWeatherStation(sid: String) extends DataRequest
  case class GetRawWeatherData(perPage: Int) extends DataRequest
  case class GetSkyConditionLookup(code: Int) extends DataRequest
  case class GetTemperature(sid: String, month: Int, year: Int) extends DataRequest
  case class GetPrecipitation(sid: String, year: Int) extends DataRequest

}

