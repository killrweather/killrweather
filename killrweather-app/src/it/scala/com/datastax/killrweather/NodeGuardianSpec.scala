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
import akka.actor.Props
import com.datastax.spark.connector.embedded.EmbeddedKafka

class NodeGuardianSpec extends ActorSparkSpec {
  import WeatherEvent._
  import Weather._
  import settings._

  val year = 2005
  val wsid = "010010:99999"

  lazy val kafka = new EmbeddedKafka

  system.eventStream.subscribe(self, classOf[NodeInitialized])

  kafka.createTopic(settings.KafkaTopicRaw)

  val brokers = Set(s"${kafka.kafkaConfig.hostName}:${kafka.kafkaConfig.port}") // TODO the right way

  val guardian = system.actorOf(Props(new NodeGuardian(ssc, kafka, brokers, settings)), "node-guardian")

  override def afterAll() { super.afterAll(); kafka.shutdown() }

  "KillrWeather" must {
    "publish a NodeInitialized to the event stream on initialization" in {
      expectMsgPF(10.seconds) {
        case NodeInitialized(actor) => actor.path should be (guardian.path)
      }
    }
    "return monthly temperature stats for a given wsid, month and year" in {
      Thread.sleep(10000) // since we are in memory and need data to get populated on startup

      guardian ! GetMonthlyTemperature("992840:99999", 1, year)

      expectMsgPF(3.seconds) {
        case e =>
          val temps =  e.asInstanceOf[Seq[Temperature]]
          log.debug(s"Received temps $temps")
      }
    }
    "return annual cumulative precip stats for a given wsid and year" in {
      guardian ! GetPrecipitation("010000:99999", year)

      expectMsgPF(3.seconds) {
        case e: Precipitation =>
          log.debug(s"Received e")
      }
    }
  }
}