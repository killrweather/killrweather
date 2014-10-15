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

import akka.actor.{ActorRef, Props, Actor}
import akka.routing.RoundRobinRouter
import com.datastax.killrweather.Weather.RawWeatherData

import org.apache.spark.streaming.StreamingContext
import kafka.server.KafkaConfig
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.streaming._
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.killrweather.actor.{KafkaProducer, WeatherActor}
import com.datastax.killrweather.WeatherEvent.PublishFeed

class KafkaSupervisor(kafka: EmbeddedKafka, ssc: StreamingContext,
                      settings: WeatherSettings, listener: ActorRef) extends WeatherActor {
  import settings._

 context.actorOf(Props(new KafkaStreamActor(kafka.kafkaParams, ssc, settings, listener)), "kafka-stream")

  val publisher = context.actorOf(Props(
    new RawDataPublisher(kafka.kafkaConfig, ssc, settings))
    .withRouter(RoundRobinRouter(nrOfInstances = DataYearRange.length,
    routerDispatcher = "killrweather.dispatchers.temperature")), "kafka-publisher")

  def receive: Actor.Receive = {
    case PublishFeed =>
      DataYearRange foreach (publisher ! _)
  }
}

/** 2. The RawDataPublisher transforms annual weather .gz files to line data and publishes to a Kafka topic.
  *
  * This just batches one data file for the demo. But you could do something like this
  * to set up a monitor on an S3 bucket, so that when new files arrive, Spark streams
  * them in. New files are read as text files with 'textFileStream' (using key as LongWritable,
  * value as Text and input format as TextInputFormat)
  * {{{
  *   streamingContext.textFileStream(dir)
       .reduceByWindow(_ + _, Seconds(30), Seconds(1))
  * }}}
  */
class RawDataPublisher(val config: KafkaConfig, ssc: StreamingContext, settings: WeatherSettings)
  extends KafkaProducer {
  import settings._

  def receive: Actor.Receive = {
    case year: Int if DataYearRange contains year => publish(year)
  }

  def publish(year: Int): Unit = {
    val location = s"$DataLoadPath/$year.csv.gz"
    log.debug(s"Attempting to read $location")

    /* Using rdd.toLocalIterator will consume as much memory as the largest partition in this RDD */
    ssc.sparkContext.textFile(location)
      .flatMap(_.split("\\n"))
      .toLocalIterator
      .foreach(send(KafkaTopicRaw, KafkaGroupId, _))
  }
}

/** 3. The KafkaStreamActor creates a streaming pipeline from Kafka to Cassandra via Spark.
  * It creates the Kafka stream which streams the raw data, transforms it, to
  * a column entry for a specific weather station[[com.datastax.killrweather.Weather.RawWeatherData]],
  * and saves the new data to the cassandra table as it arrives.
  */
class KafkaStreamActor(kafkaParams: Map[String, String], ssc: StreamingContext,
                       settings: WeatherSettings, listener: ActorRef) extends WeatherActor {

  import settings._
  import WeatherEvent._
  import Weather._

  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.MEMORY_ONLY)

  stream.map { case (_, d) => d.split(",")}
    .map(RawWeatherData(_))
    .saveToCassandra(CassandraKeyspace, CassandraTableRaw)

  listener ! OutputStreamInitialized

  def receive: Actor.Receive = {
    case _ =>
  }
}
