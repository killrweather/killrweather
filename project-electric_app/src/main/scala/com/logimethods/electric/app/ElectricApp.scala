/*
 * Copyright 2016 Logimethods - Laurent Magnin
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.logimethods.electric.app

import akka.actor._
import com.datastax.killrweather.{Application, Settings}

/** Runnable. Requires running these in cqlsh
  * {{{
  *   cqlsh> source 'create-timeseries.cql';
  *   cqlsh> source 'load-timeseries.cql';
  * }}}
  *
  * Run with SBT: sbt app/weather_run
  *
 import com.logimethods.electric.app.ElectricNodeGuardianComponentImpl
 * See: https://github.com/killrweather/killrweather/wiki/2.%20Code%20and%20Data%20Setup#data-setup
  */
object ElectricApp extends App {

  val settings = new Settings
  import settings._

  /** Creates the ActorSystem. */
  val system = ActorSystem(AppName)

  val killrWeather = ElectricApplication(system)

}

object ElectricApplication extends ExtensionId[ElectricApplication] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = ElectricApplication

  override def createExtension(system: ExtendedActorSystem) = new ElectricApplication(system)

}

class ElectricApplication(system: ExtendedActorSystem) extends Application(system: ExtendedActorSystem) with ElectricNodeGuardianComponentImpl {
}

