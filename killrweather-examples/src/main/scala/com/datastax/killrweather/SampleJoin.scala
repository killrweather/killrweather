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
import org.apache.spark.sql.cassandra.CassandraSQLContext

/**
 * Spark SQL Integration with the Spark Cassandra Connector
 *	  Uses Spark SQL simple query parser, so it has limit query supported.
 *   Supports Selection, Writing and Join Queries.
 */
object SampleJoin extends App {
  import com.datastax.spark.connector._

  val conf = new SparkConf(true)
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .setMaster("local[*]")
    .setAppName("app2")

  initialize()

  val sc = new SparkContext(conf)
  val cc = new CassandraSQLContext(sc)
  cc.setKeyspace("nosql_join")
  cc.sql("""
    SELECT test1.a, test1.b, test1.c, test2.a
    FROM test1 AS test1
    JOIN
      test2 AS test2 ON test1.a = test2.a
      AND test1.b = test2.b
      AND test1.c = test2.c
   """)
    .map{row => (row.getInt(0), row.getInt(1), row.getInt(2), row.getInt(3))}
    .saveToCassandra("nosql_join", "test3")

  sc.stop()



  def initialize(): Unit = {
    CassandraConnector(conf).withSessionDo { session =>
      session.execute("CREATE KEYSPACE IF NOT EXISTS nosql_join WITH REPLICATION = { 'class': 'SimpleStrategy', 'replication_factor': 1 }")
      session.execute("CREATE TABLE IF NOT EXISTS nosql_join.test1 (a INT, b INT, c INT, d INT, e INT, f INT, g INT, h INT, PRIMARY KEY ((a, b, c), d , e, f))")

      //session.execute("CREATE INDEX test1_g ON test1(g)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 1, 1, 1, 1)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 1, 2, 1, 1, 2)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 1, 1, 2, 1)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 1, 1, 2, 2, 1, 2, 2)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 1, 2, 1, 1)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 1, 2, 2, 1, 2)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 1, 2, 2, 1)")
      session.execute("INSERT INTO nosql_join.test1 (a, b, c, d, e, f, g, h) VALUES (1, 2, 1, 2, 2, 2, 2, 2)")

      session.execute("CREATE TABLE IF NOT EXISTS nosql_join.test2 (a INT, b INT, c INT, name TEXT, PRIMARY KEY (a, b))")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (1, 1, 1, 'Tom')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (1, 2, 3, 'Larry')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (1, 3, 3, 'Henry')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (2, 1, 3, 'Jerry')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (2, 2, 3, 'Alex')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (2, 3, 3, 'John')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (3, 1, 3, 'Jack')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (3, 2, 3, 'Hank')")
      session.execute("INSERT INTO nosql_join.test2 (a, b, c, name) VALUES (3, 3, 3, 'Dug')")

      session.execute("CREATE TABLE IF NOT EXISTS nosql_join.test3 (a INT, b INT, c INT, d INT, PRIMARY KEY (a, b, c, d))")
    }
  }
}



