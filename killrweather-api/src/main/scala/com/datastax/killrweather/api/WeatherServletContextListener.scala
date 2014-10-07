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
package com.datastax.killrweather.api

import javax.servlet.{ServletContext, ServletContextEvent, ServletContextListener}

import scala.util.Try
import org.scalatra.servlet.ServletApiImplicits
import com.datastax.spark.connector.util.Logging

class WeatherServletContextListener extends ServletContextListener with ServletApiImplicits with Logging {
  private var lifeCycle: WeatherLifeCycle = _

  def contextInitialized(sce: ServletContextEvent) {
    initLifeCycle(sce.getServletContext)
  }

  private def initLifeCycle(context: ServletContext): Unit = {
    lifeCycle = loadLifeCycle()
    lifeCycle.init(context)
  }

  private def loadLifeCycle(): WeatherLifeCycle = Try {
    val bootstrapClass = Class.forName("Bootstrap")
    bootstrapClass.newInstance().asInstanceOf[WeatherLifeCycle]
  } getOrElse defaultBootstrap

  private lazy val defaultBootstrap = new WeatherLifeCycle {
    protected def initServer(implicit context: ServletContext) {}
    protected def destroyServer(implicit context: ServletContext) {}
  }

  def contextDestroyed(sce: ServletContextEvent) {
    destroyLifeCycle(sce.getServletContext)
  }

  def destroyLifeCycle(context: ServletContext): Unit = {
    lifeCycle.destroy(context)
  }
}