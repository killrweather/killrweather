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
package com.datastax.killrweather.service

import akka.actor.Props
import com.datastax.killrweather.{KafkaProperties, KafkaPublisherActor, WeatherStationId}
import play.libs.Akka

import scala.util.Random

class LoadGenerationService extends KafkaProperties {

  def generateLoad(weatherStationId: WeatherStationId, loadSpec: LoadSpec) = {
    val kafkaProducer = Akka.system().actorOf(Props(new KafkaPublisherActor(KafkaHosts, 1)))
    val actor = Akka.system().actorOf(Props(new DataGeneratorActor(kafkaProducer, weatherStationId, Random)))
    actor ! loadSpec
  }
}
