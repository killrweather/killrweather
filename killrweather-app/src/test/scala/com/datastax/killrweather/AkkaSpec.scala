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

import akka.util.Timeout

import scala.concurrent.duration._
import akka.actor.{ActorLogging, ActorSystem}
import akka.testkit.{ImplicitSender, TestKit}
import org.scalatest.{Matchers, BeforeAndAfterAll, WordSpec}
import com.datastax.spark.connector.embedded.Assertions
import com.datastax.spark.connector.streaming.TypedStreamingActor

/** A very basic Akka actor which streams `String` event data to spark on receive. */
class TestStreamingActor extends TypedStreamingActor[String] with ActorLogging {

  override def push(e: String): Unit = {
    super.push(e)
    // TODO
  }
}
 //with SharedEmbeddedCassandra
abstract class AkkaSpec extends TestKit(ActorSystem()) with ImplicitSender with AbstractSpec {

  implicit val timeout = Timeout(3.seconds)

  override def afterAll() {
    system.shutdown()
    awaitCond(system.isTerminated)
  }
}

trait SparkSpec extends WeatherApp with AbstractSpec {

  override val settings = new WeatherSettings()

  override def afterAll() {

  }
}

trait AbstractSpec extends WordSpec with Matchers with BeforeAndAfterAll with Assertions
