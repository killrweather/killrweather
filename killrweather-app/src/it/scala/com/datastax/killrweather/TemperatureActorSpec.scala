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

import akka.actor._

class TemperatureActorSpec extends ActorSparkSpec {
  import WeatherEvent._
  import Weather._

  val year = 2005

  val sid = "010010:99999"

  val expected = 19703 // the total count stations

  "TemperatureActor" must {
    start()
    "compute daily temperature rollups per weather station to monthly statistics." in {
      val temperature = system.actorOf(Props(new TemperatureActor(ssc, settings)))
      temperature ! GetMonthlyTemperature(sid, year, 12)
      expectMsgPF(timeout.duration) {
        case e =>
          val temp = e.asInstanceOf[Option[MonthlyTemperature]].get
          temp.wsid should be (sid)
          temp.year should be (year)
          temp.month should be (12)
          println(s"For month: low=${temp.low} high=${temp.high}")
      }
    }
  }
}