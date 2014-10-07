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

import com.datastax.spark.connector.embedded.Assertions
import com.datastax.spark.connector.util.Logging
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.{SparkConf, SparkContext}

trait KillrApp extends App with Assertions with Logging {

  val settings = new Settings
  import settings._

  /** Configures Spark.
    * The appName parameter is a name for your application to show on the cluster UI.
    * master is a Spark, Mesos or YARN cluster URL, or a special “local[*]” string to run in local mode.
    *
    * When running on a cluster, you will not want to launch the application with spark-submit and receive it there.
    * For local testing and unit tests, you can pass 'local[*]' to run Spark Streaming in-process
    * (detects the number of cores in the local system).
    *
    * Note that this internally creates a SparkContext (starting point of all Spark functionality)
    * which can be accessed as ssc.sparkContext.
  */
  lazy val conf = new SparkConf().setAppName(getClass.getSimpleName).setMaster(SparkMaster)
    .set("spark.cassandra.connection.host", CassandraHosts)
    .set("spark.cleaner.ttl", SparkCleanerTtl.toString)

  lazy val sc = new SparkContext(conf)

  def ssc: StreamingContext

}

object KillrWeatherEvents {
  sealed trait BlueprintEvent extends Serializable
  case object Shutdown extends BlueprintEvent
  case object TaskCompleted extends BlueprintEvent
  case class WordCount(word: String, count: Int)
  case class StreamingWordCount(time: Long, word: String, count: Int)
}
