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

import com.datastax.killrweather.cluster.ClusterAwareNodeGuardian
import com.typesafe.config.ConfigFactory

import scala.concurrent.duration._
import akka.cluster.Cluster
import akka.actor._
import org.joda.time.{DateTime, DateTimeZone}
import com.datastax.spark.connector.embedded.Event

/** Automates demo activity every 2 seconds for demos by sending requests to `KillrWeatherApp` instances.
 *  
 * All Domain Specific code should be provided by a dedicated subclass of ApiNodeGuardian
 * (like [[com.datastax.killrweather.WeatherApiNodeGuardian]]) through an extension of AutomatedApiActorComponent
 * (like [[com.datastax.killrweather.WeatherAutomatedApiActorComponentImpl]]).
 */
abstract class ApiNodeGuardian extends ClusterAwareNodeGuardian with ClientHelper with AutomatedApiActorComponent {
  import context.dispatcher

  val props = automatedApiActorProps  
  val api = context.actorOf(props, "automated-api")

  var task: Option[Cancellable] = None
  
  cluster.joinSeedNodes(Vector(cluster.selfAddress))

  Cluster(context.system).registerOnMemberUp {
    task = Some(context.system.scheduler.schedule(Duration.Zero, 2.seconds) {
      api ! Event.QueryTask
    })
    
    log.info("Starting sending requests on {}.", cluster.selfAddress)
  }

  override def postStop(): Unit = {
    task.map(_.cancel())
    super.postStop()
  }

  def initialized: Actor.Receive = {
    case e =>
  }
}

/** For simplicity, these just go through Akka.
 *  
 * All Domain Specific code should be define by a dedicated subclass of AutomatedApiActor
 * (like [[com.datastax.killrweather.WeatherAutomatedApiActor]]), 
 * which class should be referenced by an extension of AutomatedApiActorComponent
 * (like [[com.datastax.killrweather.WeatherAutomatedApiActorComponentImpl]]).
*/
abstract class AutomatedApiActor extends Actor with ActorLogging with ClientHelper {
  
  val guardian = context.actorSelection(Cluster(context.system).selfAddress
    .copy(port = Some(BasePort)) + "/user/node-guardian")

}

// @see http://www.warski.org/blog/2010/12/di-in-scala-cake-pattern/
// Interface
trait AutomatedApiActorComponent { // For expressing dependencies
  def automatedApiActorProps: Props // Way to obtain the dependency
}
