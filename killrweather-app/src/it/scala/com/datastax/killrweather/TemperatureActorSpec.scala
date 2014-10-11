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

import akka.actor.Props
import com.datastax.killrweather.api.WeatherApi.WeatherStationId
import com.datastax.killrweather._

class TemperatureActorSpec extends AkkaSpec with SparkSpec {
  import com.datastax.killrweather.WeatherEvent._

  var received = 0
  val expected = 3

  "DailyTemperatureActor" must {
    val temperature = system.actorOf(Props(new DailyTemperatureActor(ssc, settings)), "temperature")

    "transform raw data from cassandra to daily temperatures and " in {
      temperature ! GetTemperature("010010:99999", 10, 2005)
      expectMsgPF(timeout.duration) {
        case Temperature(sid, temp) =>
          sid should be ("010010:99999")
          WeatherStationId("010010:99999").toOption.get.value should be ("010010:99999")


      }
    }
  }
}
