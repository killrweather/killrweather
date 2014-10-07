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

import javax.servlet.ServletContext

import akka.actor.ActorSystem
import org.apache.spark.streaming.StreamingContext
import org.scalatra.LifeCycle
import com.datastax.killrweather.Settings
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.spark.connector.util.Logging

/** Custom Scalatra LifeCycle. */
trait WeatherLifeCycle extends LifeCycle with Logging {

  protected def actorSystem(implicit context: ServletContext): ActorSystem =
    context("system").asInstanceOf[ActorSystem]

  protected def settings(implicit context: ServletContext): Settings =
    context("settings").asInstanceOf[Settings]

  protected def ssc(implicit context: ServletContext): StreamingContext =
    context("streaming-context").asInstanceOf[StreamingContext]

  protected def kafka(implicit context: ServletContext): EmbeddedKafka =
    context("kafka").asInstanceOf[EmbeddedKafka]

  override def init(context: ServletContext): Unit = {
    super.init(context)
    log.info("Initializing servlet context.")
    initServer(context)
  }

  // Does nothing today.  Exists for symmetry with init and futureproofing.
  override def destroy(context: ServletContext): Unit = {
    log.info("Destroying servlet context.")
    destroyServer(context)
    super.destroy(context)
  }

  protected def initServer(implicit context: ServletContext): Unit

  protected def destroyServer(implicit context: ServletContext): Unit
}
