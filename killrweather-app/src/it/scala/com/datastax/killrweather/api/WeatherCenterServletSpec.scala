/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.datastax.killrweather.api

import com.datastax.killrweather.TimeseriesBlueprint
import com.datastax.killrweather.api.WeatherApi.HiLowForecast
import org.json4s.Extraction._
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.JsonParser
import org.scalatest.WordSpecLike
import org.scalatra.test.scalatest._
import com.datastax.killrweather._

class WeatherCenterServletSpec extends ScalatraSuite with WordSpecLike
  with TimeseriesBlueprint with TimeseriesFixture {
  import com.datastax.killrweather._

  val api = new WeatherDataActorApi(system, guardian)
  
  addServlet(new WeatherCenterServlet(api), "/*")

  "WeatherCenterServlet" should {
    "GET v1/high-low with a valid uid" in {
      get("/v1/weather/climatology/high-low/10023?dayofyear=92", headers = testHeaders) {
        response.status should be(200)
        val alerts = JsonParser.parse(response.body).extract[HiLowForecast]
        println(pretty(render(decompose(alerts))))
        // TODO validate
      }
    }
    "response with 400 if no uid is passed in the header" in {
      get("/v1/high-low") {
        response.status should be(400)
      }
    }
  }
}

// ?perPage=20&size=400
trait TimeseriesFixture {

  val testHeaders = Map("content-type" -> "application/json")

  val userId = "9784dkfu387669eb2936d1b1fdd858a"

  val weatherStationId = "010010:99999"

  val weatherStationHeaders = Map("X-BLUEPRINTS-STATION-ID" -> weatherStationId) ++ testHeaders


}