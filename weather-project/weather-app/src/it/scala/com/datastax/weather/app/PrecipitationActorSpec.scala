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
package com.datastax.weather.app

import scala.concurrent.duration._
import akka.actor._
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.killrweather._

class PrecipitationActorSpec extends ActorSparkSpec {

  import com.datastax.weather.WeatherEvent._
  import com.datastax.weather.Weather._
  import settings._

  val kafka = new EmbeddedKafka

  kafka.createTopic(KafkaTopicRaw)

  val ssc = new StreamingContext(sc, Milliseconds(SparkStreamingBatchInterval))

  override val kafkaActor = Some(system.actorOf(Props(new KafkaStreamingActor(
    kafka.kafkaParams, ssc, settings, self)), "kafka-stream"))

  val precipitation = system.actorOf(Props(new PrecipitationActor(ssc, settings)), "precipitation")

  start(clean = true)

  expectMsgPF(20.seconds) {
    case OutputStreamInitialized => ssc.start()
  }

  "PrecipitationActor" must {
    "computes and return a monthly aggregation to the requester." in {
      precipitation ! GetPrecipitation(sample.wsid, sample.year)
      expectMsgPF(timeout.duration) {
        case a: AnnualPrecipitation =>
          a.wsid should be (sample.wsid)
          a.year should be (sample.year)
      }
    }
    "Return the top k temps for any station in a given year" in {
      precipitation ! GetTopKPrecipitation(sample.wsid, sample.year, k = 10)
      expectMsgPF(timeout.duration) {
        case a: TopKPrecipitation =>
          a.wsid should be (sample.wsid)
          a.year should be (sample.year)
          a.top.size should be (10)
      }
    }
  }
}