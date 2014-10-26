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

import akka.actor.ActorRef
import org.apache.spark.util.StatCounter

object WeatherEvent {
  import Weather._

  /** Base marker trait. */
  @SerialVersionUID(1L)
  sealed trait WeatherEvent extends Serializable

  sealed trait LifeCycleEvent extends WeatherEvent
  case object OutputStreamInitialized extends LifeCycleEvent
  case class NodeInitialized(root: ActorRef) extends LifeCycleEvent
  case object PublishFeed extends LifeCycleEvent
  case object Shutdown extends LifeCycleEvent
  case object TaskCompleted extends LifeCycleEvent

  sealed trait WeatherRequest extends WeatherEvent
  sealed trait WeatherResponse extends WeatherEvent

  /** Composite of Air Force Datsav3 station number and NCDC WBAN number
    * @param sid uses the composite key format: stationNum:wbanNum
    */
  case class GetWeatherStation(sid: String) extends WeatherRequest
  case class GetRawWeatherData(perPage: Int) extends WeatherRequest

  case class WeatherStationIds(sids: String*) extends WeatherResponse

  /** @param constraint allows testing a subsection vs doing full year */
  case class ComputeDailyTemperature(wsid: String, year: Int, constraint: Option[Int] = None) extends WeatherRequest
  case class GetTemperature(wsid: String, doy: Int, year: Int) extends WeatherRequest
  case class GetMonthlyTemperature(wsid: String, month: Int, year: Int) extends WeatherRequest

  case class Temperature(wsid: String, year: Int, month: Int, day: Int,
    high: Double, low: Double, mean: Double, variance: Double, stdev: Double) extends WeatherAggregate

  case class ComputeDailyPrecipitation(wsid: String, year: Int, constraint: Option[Int] = None) extends WeatherRequest
  case class GetPrecipitation(sid: String, year: Int) extends WeatherRequest
  case class GetTopKPrecipitation(year: Int) extends WeatherRequest
  case class TopKPrecipitation(seq: Seq[(String, Double)]) extends WeatherAggregate

  /**
   * Quick access lookup table for sky_condition. Useful for potential analytics.
   * See http://en.wikipedia.org/wiki/Okta
   */
  case class GetSkyConditionLookup(code: Int) extends WeatherRequest

}

object KafkaEvent {
  case class KafkaMessageEnvelope[K,V](topic: String, key: K, messages: V*)
}