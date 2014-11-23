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

import scala.collection.immutable
import akka.actor._
import akka.cluster.Cluster
import com.typesafe.config.ConfigFactory

object KillrWeatherClientApp extends App {

  val settings = new WeatherSettings()
  import settings._

  val port1 = ConfigFactory.load.getInt("akka.remote.netty.tcp.port")

  /** Creates the client's ActorSystem on another port. */
  val system = ActorSystem(AppName,
    ConfigFactory.parseString(s"akka.remote.netty.tcp.port = ${port1+1}").withFallback(rootConfig))

  protected val log = akka.event.Logging(system, system.name)

  val cluster = Cluster(system)
  cluster.joinSeedNodes(immutable.Seq(cluster.selfAddress))

  val guardian = system.actorSelection(cluster.selfAddress.copy(port = Some(2550)) + "/user/node-guardian")

  /** Drives demo activity by sending requests to the [[NodeGuardian]] actor. */
  val queryClient = system.actorOf(Props(new WeatherApiQueries(settings, guardian)), "api-client")

}
