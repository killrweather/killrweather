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

import java.util.concurrent.atomic.AtomicBoolean
import akka.actor._
import akka.cluster.Cluster
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkConf
import com.datastax.spark.connector.embedded.EmbeddedKafka
import scala.concurrent.Future
import com.datastax.killrweather.Application

/** Runnable. Requires running these in cqlsh
  * {{{
  *   cqlsh> source 'create-timeseries.cql';
  *   cqlsh> source 'load-timeseries.cql';
  * }}}
  *
  * Run with SBT: sbt app/weather_run
  *
  * See: https://github.com/killrweather/killrweather/wiki/2.%20Code%20and%20Data%20Setup#data-setup
  */
object WeatherApp extends App {

  val settings = new Settings
  import settings._

  /** Creates the ActorSystem. */
  val system = ActorSystem(AppName)

  val killrWeather = WeatherApplication(system)

}

object WeatherApplication extends ExtensionId[WeatherApplication] with ExtensionIdProvider {

  override def lookup: ExtensionId[_ <: Extension] = WeatherApplication

  override def createExtension(system: ExtendedActorSystem) = new WeatherApplication(system)

}

class WeatherApplication(system: ExtendedActorSystem) extends Application(system: ExtendedActorSystem) with WeatherNodeGuardianComponentImpl {
}

