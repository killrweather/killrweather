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

import scala.concurrent.duration._
import akka.actor.SupervisorStrategy._
import akka.actor._
import akka.pattern.gracefulStop
import akka.util.Timeout
import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.embedded.{Assertions, EmbeddedKafka}
import com.datastax.spark.connector.streaming._
import com.datastax.killrweather.api.WeatherApi

/**
 * NOTE: if [[NodeGuardian]] is ever put on an Akka router, multiple instances of the stream will
 * exist on the node. Might want to call 'union' on the streams in that case.
 */
class NodeGuardian(ssc: StreamingContext, kafka: EmbeddedKafka, settings: WeatherSettings)
  extends Actor with Assertions with ActorLogging {
  import com.datastax.killrweather.KillrWeatherEvents._
  import WeatherApi._
  import Weather._
  import settings._

  override val supervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 10, withinTimeRange = 1 minute) {
      case _: AssertionError           => Resume
      case _: NullPointerException     => Restart
      case _: IllegalArgumentException => Stop
      case _: Exception                => Escalate
    }

  implicit val timeout = Timeout(3.seconds)

  // 1. read the 2014 data file and publish the raw data to Kafka for streaming in later.
  context.actorOf(Props(new RawDataActor(kafka.kafkaConfig, ssc, settings)), "kafka-raw-publisher")

  var highLow: Option[ActorRef] = None

  override def postStop(): Unit = {

  }

  def receive: Actor.Receive = {
    case TaskCompleted                => startStreaming()
    case e: GetTemperatureAggregate                  => highLow map (_ forward e)
    case GetWeatherStation(sid)       => weatherStation(sid, sender)
    case GetRawWeatherData            =>
    case GetSkyConditionLookup        =>
    case PoisonPill                   => gracefulShutdown()
  }

  // 2. save raw to cassandra
  def startStreaming(): Unit = {

    val rawInputStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
      ssc, kafka.kafkaParams, Map(KafkaTopicRaw -> 1), StorageLevel.MEMORY_ONLY)

    rawInputStream.map { case (_,d) => d.split(",")}
      .map (RawWeatherData(_))
      .saveToCassandra(CassandraKeyspace, CassandraTableRaw) // can also create a new table and specify columns: SomeColumns("key", "value")

    ssc.checkpoint(SparkCheckpointDir)

    ssc.start()

    ssc.awaitTermination()

    highLow = Some(context.actorOf(Props(new HighLowActor(ssc, settings)), "high-low"))
    // for now call this directly until hooked up:
    highLow map (_ ! GetTemperatureAggregate(10024))
  }

  /** Fill out the where clause and what needs to be passed in to request one. */
  def weatherStation(sid: WeatherStationId, requester: ActorRef): Unit = {
     //Future(ssc.sc.cassandraTable[WeatherStation](CassandraKeyspace, "weather_station").where(...)) pipeTo requester
  }

  def gracefulShutdown(): Unit = {
    context.children foreach (c => awaitCond(gracefulStop(c, timeout.duration).isCompleted, timeout.duration))
    log.info(s"Graceful stop completed.")
  }

}
