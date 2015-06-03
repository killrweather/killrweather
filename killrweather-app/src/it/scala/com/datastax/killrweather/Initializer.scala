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

import akka.actor.ActorRef
import com.datastax.spark.connector._
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope
import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.rdd.RDD

/**
 * -Dkillrweather.initialize=true. The default is false,
 * assumes manual creation by running the cql scripts.
 */
private[killrweather] class Initializer(sc: SparkContext, settings: WeatherSettings)
  extends Serializable with TestFileHelper with Logging {

  import com.datastax.killrweather.Weather._
  import settings._

  /** Loads data from /data/load files (because this is for a runnable demo.
    * Because we run locally vs against a cluster as a demo app, we keep that file size data small.
    *
    * @param clean For running IT tests.
    *              Set to false if you have run the cql create and load scripts
    *                 and need to keep that schema which includes the weather stations,
    *                 like the app and for testing the [[WeatherStationActor]].
    *                 See: https://github.com/killrweather/killrweather/wiki/2.%20Code%20and%20Data%20Setup#data-setup
    *              Set to true if you do not need the weather stations for an IT test,
    *                and would like the relevant schema created automatically.
    *              Defaults to false for the app.
    */
  def start(clean: Boolean = false, kafkaActor: Option[ActorRef] = None): Unit = {
    createSchema(clean)
    loadDataFiles(kafkaActor)
  }

  private def loadDataFiles(kafkaActor: Option[ActorRef]): Unit = {
    import settings.{CassandraKeyspace => keyspace, CassandraTableRaw => table, KafkaGroupId => group, KafkaTopicRaw => topic}

    def toLines(file: String): RDD[String] =
      sc.textFile(file).flatMap(_.split("\\n"))

    kafkaActor match {
      case Some(actor) =>
        val toActor = (line: String) => actor ! KafkaMessageEnvelope[String, String](topic, group, line)

        for (file <- fileFeed(DataLoadPath, DataFileExtension))
          toLines(file.getAbsolutePath).toLocalIterator.foreach(toActor)

      case _ =>
        for (file <- fileFeed(DataLoadPath, DataFileExtension))
          toLines(file.getAbsolutePath).map(_.split(",")).map(RawWeatherData(_)).saveToCassandra(keyspace, table)
    }
  }

  private def createSchema(clean: Boolean): Unit = {
    log.info("Initializing schema.")

    CassandraConnector(sc.getConf).withSessionDo { session =>
      // insure for test we are not going to look at existing data, but new from the kafka actor processes
      if (clean) {
        session.execute(s"DROP KEYSPACE IF EXISTS $CassandraKeyspace")
      }
      session.execute(s"CREATE KEYSPACE IF NOT EXISTS $CassandraKeyspace WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute(s"USE $CassandraKeyspace")

      session.execute( s"""CREATE TABLE IF NOT EXISTS weather_station (
          id text PRIMARY KEY,  // Composite of Air Force Datsav3 station number and NCDC WBAN number
          name text,            // Name of reporting station
          country_code text,    // 2 letter ISO Country ID
          state_code text,      // 2 letter state code for US stations
          call_sign text,       // International station call sign
          lat double,            // Latitude in decimal degrees
          long double,           // Longitude in decimal degrees
          elevation double       // Elevation in meters
        )""")

      session.execute( s"""CREATE TABLE IF NOT EXISTS $CassandraTableRaw (
        wsid text, year int, month int, day int, hour int,temperature double, dewpoint double, pressure double,
        wind_direction int, wind_speed double,sky_condition int, sky_condition_text text, one_hour_precip double, six_hour_precip double,
        PRIMARY KEY ((wsid), year, month, day, hour)
       ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC, hour DESC)""")

      session.execute( s"""CREATE TABLE IF NOT EXISTS $CassandraTableDailyTemp (
         wsid text,year int,month int,day int,high double,low double,mean double,variance double,stdev double,
         PRIMARY KEY ((wsid), year, month, day)
      ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)""")

      session.execute( s"""CREATE TABLE IF NOT EXISTS $CassandraTableDailyPrecip (
         wsid text,year int,month int,day int,precipitation counter,
         PRIMARY KEY ((wsid), year, month, day)
      ) WITH CLUSTERING ORDER BY (year DESC, month DESC, day DESC)""")

      session.execute( s"""CREATE TABLE IF NOT EXISTS year_cumulative_precip (
         wsid text, year int, precipitation counter, PRIMARY KEY ((wsid), year)
      ) WITH CLUSTERING ORDER BY (year DESC)""")

      log.info("Schema initialized.")
    }
  }
}
