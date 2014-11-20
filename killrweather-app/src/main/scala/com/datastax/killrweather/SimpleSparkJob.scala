package com.datastax.killrweather

import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
 * Very simple example fo how to connect Spark and Cassandra.
 *
 */
object SimpleSparkJob {

  def main(args: Array[String]): Unit = {
    // the setMaster("local") lets us run & test the job right in our IDE
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    // "local" here is the master, meaning we don't explicitly have a spark master set up
    val sc = new SparkContext("local", "weather", conf)

    // we need this later to execute queries
    // there's another way of writing data to cassandra, we'll cover it later
    // this is good for single queries
    // the future version will be for saving RDDs
    val connector = CassandraConnector(conf)

    // keyspace & table
    val table: CassandraRDD[CassandraRow] = sc.cassandraTable("isd_weather_data", "raw_weather_data")

    // get a simple count of all the rows in the demo table
    val rowCount = table.count()


    println("Total Rows in Raw Weather Table: ", rowCount)

  }
}
