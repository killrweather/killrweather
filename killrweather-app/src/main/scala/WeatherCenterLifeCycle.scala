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
import javax.servlet.ServletContext

import akka.actor.Props
import com.datastax.killrweather.{WeatherSettings, NodeGuardian}
import com.datastax.killrweather.api.WeatherLifeCycle
import com.datastax.killrweather.api.{WeatherDataActorApi, WeatherCenterServlet}

/** For running from Tomcat. */
class WeatherCenterLifeCycle extends WeatherLifeCycle {

  protected def initServer(implicit context: ServletContext): Unit = {
    val guardian = actorSystem.actorOf(Props(new NodeGuardian(ssc, kafka, settings)), "node-guardian")
    val api = new WeatherDataActorApi(actorSystem, guardian)
    context.mount(new WeatherCenterServlet(api), "/time-series/*")
  }

  protected override def settings(implicit context: ServletContext): WeatherSettings =
    context("settings").asInstanceOf[WeatherSettings]

  protected def destroyServer(implicit context: ServletContext): Unit = {}
}