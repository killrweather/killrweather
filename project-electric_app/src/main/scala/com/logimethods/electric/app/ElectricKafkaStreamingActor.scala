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

import akka.actor.ActorRef
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather._
import com.datastax.killrweather.BusinessEvent.OutputStreamInitialized
import akka.actor.actorRef2Scala
import com.logimethods.electric.Electric
import com.logimethods.electric.Electric.RawUsagePointData
import com.logimethods.electric.ElectricEvent
import scala.reflect.runtime.universe

/** The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class ElectricKafkaStreamingActor(kafkaParams: Map[String, String],
                          ssc: StreamingContext,
                          settings: Settings,
                          listener: ActorRef) 
     extends KafkaStreamingActor(kafkaParams,
                          ssc,
                          settings,
                          listener) {

  import settings._
  import com.logimethods.electric.ElectricEvent._
  import com.logimethods.electric.Electric._
  
  log.debug("kafkaParams: {}.", kafkaParams)

  val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.DISK_ONLY_2)
    .map(_._2.split(","))
    .map(RawUsagePointData(_))

  /** Saves the raw data to Cassandra - raw table. */
  kafkaStream.saveToCassandra(CassandraKeyspace, CassandraTableRaw)

  /** For a given weather station, year, month, day, aggregates hourly precipitation values by day.
    * Weather station first gets you the partition key - data locality - which spark gets via the
    * connector, so the data transfer between spark and cassandra is very fast per node.
    *
    * Persists daily aggregate data to Cassandra daily precip table by weather station,
    * automatically sorted by most recent (due to how we set up the Cassandra schema:
    * @see https://github.com/killrweather/killrweather/blob/master/data/create-timeseries.cql.
    *
    * Because the 'oneHourPrecip' column is a Cassandra Counter we do not have to do a spark
    * reduceByKey, which is expensive. We simply let Cassandra do it - not expensive and fast.
    * This is a Cassandra 2.1 counter functionality ;)
    *
    * This new functionality in Cassandra 2.1.1 is going to make time series work even faster:
    * https://issues.apache.org/jira/browse/CASSANDRA-6602
    */
//  kafkaStream.map { electric =>
//    (electric.wsid, electric.year, electric.month, electric.day, electric.oneHourPrecip)
//  }.saveToCassandra(CassandraKeyspace, CassandraTableDailyPrecip)

  if (log.isDebugEnabled) {
	  kafkaStream.print // for demo/debug purposes only
  }

  /** Notifies the supervisor that the Spark Streams have been created and defined.
    * Now the [[StreamingContext]] can be started. */
  listener ! OutputStreamInitialized
}


// Implementation
trait ElectricKafkaStreamingActorComponentImpl extends KafkaStreamingActorComponent { // For expressing dependencies
  // Dependencies
  this: KafkaStreamingActorComponent =>
  
  def kafkaStreamingActor(kafkaParams: Map[String, String],
                          ssc: StreamingContext,
                          settings: Settings,
                          listener: ActorRef): KafkaStreamingActor // Way to obtain the dependency
    = new ElectricKafkaStreamingActor(kafkaParams: Map[String, String],
                          ssc: StreamingContext,
                          settings: Settings,
                          listener: ActorRef)
}

