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
import com.datastax.driver.core.Cluster
import com.datastax.killrweather.controllers.{LoadGenerationController, WeatherController}
import com.datastax.killrweather.infrastructure.WeatherStationDao
import com.datastax.killrweather.service.{LoadGenerationService, WeatherService}
import play.api._

object Global extends GlobalSettings {

  // poor man's DI
  val cluster = Cluster.builder().addContactPoint("localhost").build()
  val weatherStationDao = new WeatherStationDao(cluster.connect("isd_weather_data"))
  val weatherService = new WeatherService(weatherStationDao)
  val weatherDataGenerator = new LoadGenerationService

  val weatherController = new WeatherController(weatherStationDao, weatherService)
  val loadController = new LoadGenerationController(weatherDataGenerator)

  val controllers = Map[Class[_], AnyRef](
    weatherController.getClass -> weatherController,
    loadController.getClass -> loadController
  )

  override def getControllerInstance[A](controllerClass: Class[A]): A = {
    controllers(controllerClass).asInstanceOf[A]
  }
}
