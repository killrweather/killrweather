package com.datastax.killrweather

import com.datastax.spark.connector.rdd.CassandraRDD
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

/**
 * Very simple example of how to connect Spark and Cassandra.
 *
 */
object SimpleSparkJob {

  def main(args: Array[String]): Unit = {
    // the setMaster("local") lets us run & test the job right in our IDE
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").setMaster("local")

    // "local" here is the master, meaning we don't explicitly have a spark master set up
    val sc = new SparkContext("local", "weather", conf)

    // keyspace & table
    val table: CassandraRDD[CassandraRow] = sc.cassandraTable("isd_weather_data", "raw_weather_data")
      // Add this to drill further into the data
      //.where("wsid='725030:14732'")

    // get a simple count of all the rows in the demo table
    val rowCount = table.count()


    println(s"Total Rows in Raw Weather Table: $rowCount")

  }
}
