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

import akka.actor.{Props, ActorSystem}
import play.api._
import com.datastax.killrweather.DashboardApiActor
import com.datastax.killrweather.controllers.{LoadGenerationController, WeatherController}
import com.datastax.killrweather.service.LoadGenerationService
import com.typesafe.config.ConfigFactory


object Global extends GlobalSettings {

  // not using Play's system as you can't override the name
  val system = ActorSystem("KillrWeather", ConfigFactory.load("cluster.conf"))
  val dashboardApi = system.actorOf(Props[DashboardApiActor])

  // poor man's DI
  val weatherDataGenerator = new LoadGenerationService
  val weatherController = new WeatherController(dashboardApi)
  val loadController = new LoadGenerationController(weatherDataGenerator)

  val controllers = Map[Class[_], AnyRef](
    weatherController.getClass -> weatherController,
    loadController.getClass -> loadController
  )

  override def getControllerInstance[A](controllerClass: Class[A]): A = {
    controllers(controllerClass).asInstanceOf[A]
  }
}
