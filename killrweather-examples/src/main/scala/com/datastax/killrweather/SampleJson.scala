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

import com.datastax.spark.connector.cql.CassandraConnector
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext}

/** Spark SQL: Txt, Parquet, JSON Support with the Spark Cassandra Connector */
object SampleJson extends App {
  import com.datastax.spark.connector._
  import GitHubEvents._

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[*]")
    .setAppName("app2")

  CassandraConnector(conf).withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS githubstats")
    session.execute("CREATE KEYSPACE githubstats WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute(
      """CREATE TABLE githubstats.monthly_commits (
        |user VARCHAR PRIMARY KEY,
        |commits INT,
        |month INT,
        |year INT)""".stripMargin)
  }

  val sc = new SparkContext(conf)
  val sqlContext = new SQLContext(sc)
  val json = sc.parallelize(Seq("""{"user":"helena","commits":98, "month":12, "year":2014}""","""{"user":"pkolaczk", "commits":42, "month":12, "year":2014}"""))

  sqlContext.read.json(json).map(MonthlyCommits(_)).saveToCassandra("githubstats","monthly_commits")

  sc.cassandraTable[MonthlyCommits]("githubstats","monthly_commits").collect foreach println

  sc.stop()
}


object GitHubEvents {

  sealed trait Stat extends Serializable
  trait User extends Stat
  trait Repo extends Stat
  trait Commits extends Stat
  case class MonthlyCommits(user: String, commits: Int, month: Int, year: Int) extends Commits
  object MonthlyCommits {
    def apply(r: Row): MonthlyCommits = MonthlyCommits(
      r.getString(0), r.getInt(1), r.getInt(2), r.getInt(3))
  }
}