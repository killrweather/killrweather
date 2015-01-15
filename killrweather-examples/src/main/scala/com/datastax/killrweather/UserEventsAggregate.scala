/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
package com.datastax.killrweather

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import com.datastax.spark.connector._
import org.joda.time.{DateTime, DateTimeZone}

/* Question from webinar: 01/11/2015
   We use cassandra wide rows heavily to store time-series as they are perfect for that use-case.

  create table user_events (
  user_id  text,
  timestmp timestamp,
  event text,
  primary key((user_id), timestmp));

  using spark: select all user_ids which had at least 1 event during the last month.
 */
object UserEventsAggregate extends App {

  val users = Seq("user-a", "user-b", "user-c")
  val events = Seq("first", "second")
  val from = lastMonth.withDayOfMonth(1).withTime(1, 0, 0, 0)
  val to = lastMonth.withDayOfMonth(30).withTime(23, 59, 59, 999)

  val conf = new SparkConf().setMaster("local[*]").setAppName(getClass.getSimpleName)
    .set("spark.cassandra.connection.host", "127.0.0.1")

  // Note this is only for a quick prototype, not prod! 'SimpleStrategy', 'replication_factor': 1
  CassandraConnector(conf).withSessionDo { session =>
    session.execute( """create keyspace if not exists my_keyspace with replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }""")
    session.execute("drop table if exists my_keyspace.user_events")
    session.execute( """create table my_keyspace.user_events ( user_id  text, timestmp timestamp, event text, primary key((user_id), timestmp) ) WITH CLUSTERING ORDER BY (timestmp DESC)""")
    for (month <- 1 to 12; user <- users) user match {
      case id if id == "user-a" =>
        // give 1 user 0 events for last month
        if (month != lastMonth.getMonthOfYear) events.foreach(event => session.execute(cql(id, month, event)))
      case id =>
        events.foreach(event => session.execute(cql(id, month, event)))
    }
  }

  val sc = new SparkContext(conf)

  val results = sc.cassandraTable[UserEvent]("my_keyspace", "user_events")
    .where("timestmp > ? and timestmp < ?", from, to)
    .map(_.userId).distinct().collect()

  require(!results.contains("user-a"), "'results' must not contain 'user-a' which has 0 events")

  results foreach println

  sc.stop()

  /* helpers */
  def cql(id: String, month: Int, ename: String): String = s"""
       insert into my_keyspace.user_events(user_id,timestmp,event)
       values('$id', ${timestamp(month)}, '$ename event for month $month')"""

  def now = new DateTime(DateTimeZone.UTC)
  def lastMonth = now.minusMonths(1)
  def timestamp(moy: Int, day: Option[Int] = Some(1)): Long = now.withYear(2014).withMonthOfYear(moy).getMillis
  case class UserEvent(userId: String, timestmp: Long, event: String) extends Serializable

}

