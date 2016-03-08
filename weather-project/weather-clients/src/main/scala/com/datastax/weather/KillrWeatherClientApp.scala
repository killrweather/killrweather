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
package com.datastax.weather

import com.datastax.killrweather.cluster.ClusterAwareNodeGuardian
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import akka.cluster.Cluster
import akka.actor._
import org.joda.time.{DateTime, DateTimeZone}
import com.datastax.spark.connector.embedded.Event
import com.datastax.killrweather._

/** Automates demo activity every 2 seconds for demos by sending requests to `KillrWeatherApp` instances. */
object KillrWeatherClientApp extends App with ClientHelper {

  /** Creates the ActorSystem. */
  val system = ActorSystem("KillrWeather", ConfigFactory.parseString("akka.remote.netty.tcp.port = 2552"))

  /* The root supervisor and fault tolerance handler of the data ingestion nodes. */
  val guardian = system.actorOf(Props[WeatherAutomatedApiNodeGuardian], "node-guardian")

  system.registerOnTermination {
    guardian ! PoisonPill
  }

}
