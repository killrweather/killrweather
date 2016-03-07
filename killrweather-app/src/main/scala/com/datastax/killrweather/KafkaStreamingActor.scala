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

import akka.actor.{ActorLogging, Actor, ActorRef}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._

/** The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  * 
  * All Domain Specific code should be define by a dedicated subclass of KafkaStreamingActor
  * (like [[com.datastax.killrweather.WeatherKafkaStreamingActor]]), 
  * which class should be referenced by an extension of NodeGuardianComponent
  * (like [[com.datastax.killrweather.WeatherKafkaStreamingActorComponentImpl]]).
  */
abstract class KafkaStreamingActor(kafkaParams: Map[String, String],
                          ssc: StreamingContext,
                          listener: ActorRef) extends AggregationActor with ActorLogging {
  
  def receive : Actor.Receive = {
    case e => // ignore
  }
}

// @see http://www.warski.org/blog/2010/12/di-in-scala-cake-pattern/
// Interface
trait KafkaStreamingActorComponent { // For expressing dependencies
  def kafkaStreamingActor(kafkaParams: Map[String, String],
                          ssc: StreamingContext,
                          listener: ActorRef): KafkaStreamingActor // Way to obtain the dependency
}
