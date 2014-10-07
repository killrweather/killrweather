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

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef}
import com.datastax.killrweather.KillrWeatherEvents._
import com.datastax.killrweather.Weather.RawWeatherData
import com.datastax.spark.connector.embedded.Assertions
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import kafka.serializer.StringEncoder
import kafka.server.KafkaConfig
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.StreamingContext

trait WeatherActor extends Actor with ActorLogging


/** Stores the raw data in Cassandra for multi-purpose data analysis.
  *
  * This just batches one data file for the demo. But you could do something like this
  * to set up a monitor on a directory, so that when new files arrive, Spark streams
  * them in. New files are read as text files with 'textFileStream' (using key as LongWritable,
  * value as Text and input format as TextInputFormat)
  * {{{
  *   ssc.textFileStream("dirname")
     .reduceByWindow(_ + _, Seconds(30), Seconds(1))
  * }}}
  *
  * Should make this a supervisor which partitions the workload to routees vs doing
  * all the work itself. But normally this would be done by other strategies anyway.
  */
class RawDataActor(val kafkaConfig: KafkaConfig, ssc: StreamingContext, settings: WeatherSettings)
  extends KafkaProducer {

  import settings._

  val lines: RDD[String] = ssc.sparkContext.textFile(RawDataFile).flatMap(_.split("\\n"))

  publish(lines)

  def receive : Actor.Receive = {
    case e =>
  }

  // TODO start but don't wait until done. go right back
  def publish(data: RDD[String]): Unit = {
    log.info(s"Batch sending raw data to kafka")
    batchSend(KafkaTopicRaw, KafkaGroupId, KafkaBatchSendSize, lines.toLocalIterator.toSeq)
    context.parent ! TaskCompleted
    context stop self
  }
}

/** 3. reads raw from kafka stream, processes, stores in cassandra. */
class HighLowActor(ssc: StreamingContext, settings: WeatherSettings) extends WeatherActor with Assertions {

  import com.datastax.spark.connector.streaming._
  import com.datastax.killrweather.api.WeatherApi._
  import settings._

  def receive : Actor.Receive = {
    case GetHiLow(zip, doy) => compute(zip, doy, sender)
  }

  /**
   * IMPLEMENT ME - for a given weather station:
   * Hi-Low temperature average and cumulative rainfall for weather station 
   */
  def compute(zip: Int, doy: Int, requester: ActorRef): Unit = {

    /* The iterator will consume as much memory as the largest partition in this RDD */
    ssc.cassandraTable[RawWeatherData](CassandraKeyspace, CassandraTableRaw)
      .toLocalIterator.foreach(row => log.info(s"Read from Cassandra [$row]"))

    /*
    While you're here, try playing around with select and where:
    ssc.cassandraTable[RawWeatherData](CassandraKeyspace, CassandraTableRaw)
      .select(...column names)
      .where("month = ?", 9)
    */

  }
}


trait KafkaProducer extends WeatherActor {

  def kafkaConfig: KafkaConfig

  private val producer = {
    val props = new Properties()
    props.put("metadata.broker.list", kafkaConfig.hostName + ":" + kafkaConfig.port)
    props.put("serializer.class", classOf[StringEncoder].getName)
    props.put("partitioner.class", "kafka.producer.DefaultPartitioner")
    props.put("request.required.acks", "1")
    props.put("producer.type", "async")
    props.put("batch.num.messages", "100")

    new Producer[String, String](new ProducerConfig(props))
  }

  def send(topic : String, key : String, message : String): Unit =
    producer.send(KeyedMessage(topic, key, message))

  def batchSend(topic: String, group: String, batchSize: Int, lines: Seq[String]): Unit =
    if (lines.nonEmpty) {
      val (send, unsent) = lines.toSeq.splitAt(batchSize)
      val messages = send map { data => KeyedMessage(topic, group, data)}
      producer.send(messages.toArray: _*)
      log.debug(s"Published messages to kafka topic '$topic'. Batching remaining ${unsent.size}")
      batchSend(topic, group, batchSize, unsent)
    }

  override def postStop(): Unit =
    producer.close()

}