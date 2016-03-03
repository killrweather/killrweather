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

import com.datastax.killrweather._
import scala.concurrent.duration._
import akka.actor.{Actor, Props}
import org.apache.spark.streaming.StreamingContext
import com.datastax.spark.connector.embedded._
import com.logimethods.electric.ElectricEvent._

/** A `NodeGuardian` manages the worker actors at the root of each KillrWeather
 * deployed application, where any special application logic is handled in the
 * implementer here, but the cluster work, node lifecycle and supervision events
 * are handled in [[ClusterAwareNodeGuardian]], in `killrweather/killrweather-core`.
 *
 * This `NodeGuardian` creates the [[KafkaStreamingActor]] which creates a streaming
 * pipeline from Kafka to Cassandra, via Spark, which streams the raw data from Kafka,
 * transforms data to [[com.datastax.killrweather.Weather.RawWeatherData]] (hourly per
 * weather station), and saves the new data to the cassandra raw data table on arrival.
 */
class ElectricNodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: Settings)
                        extends NodeGuardian(ssc, kafka, settings) with ElectricKafkaStreamingActorComponentImpl {
  import BusinessEvent._
  import settings._

  /** The Spark/Cassandra computation actors: For the tutorial we just use 2005 for now. */
  val voltage = context.actorOf(Props(new VoltageActor(ssc.sparkContext, settings)), "voltage")

  /** This node guardian's customer behavior once initialized. */
  override def initialized: Actor.Receive = {
    case e: VoltageRequest    => voltage forward e
    case GracefulShutdown => gracefulShutdown(sender())
  }

}

// @see http://www.warski.org/blog/2010/12/di-in-scala-cake-pattern/
// Implementation
trait ElectricNodeGuardianComponentImpl extends NodeGuardianComponent { // For expressing dependencies
  // Dependencies
  this: NodeGuardianComponent =>
  
  def nodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: Settings): NodeGuardian
    = new ElectricNodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: Settings)
}
