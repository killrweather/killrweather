package com.datastax.killrweather

import scala.concurrent.duration._
import com.datastax.killrweather.GitHubEvents.MonthlyCommits
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{Assertions, EmbeddedKafka}
import com.datastax.spark.connector._
import com.datastax.spark.connector.streaming._

object KafkaStreamingJson extends App with Assertions {

  val data = Seq(
    """{"user":"helena","commits":98, "month":12, "year":2015}""",
    """{"user":"pkolaczk", "commits":42, "month":12, "year":2015}""")

  /* Kafka (embedded) setup */
  val kafka = new EmbeddedKafka
  kafka.createTopic("github")

  // simulate another process streaming data to Kafka
  val producer = new Producer[String, String](kafka.producerConfig)
  data.foreach (m => producer.send(new KeyedMessage[String, String]("github", "githubstats", m)))
  producer.close()

  /* Spark initialization */
  val conf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster("local[*]")
    .set("spark.cassandra.connection.host", "127.0.0.1")
    .set("spark.cleaner.ttl", "5000")
  val sc = new SparkContext(conf)
  val ssc = new StreamingContext(sc, Milliseconds(5000))
  val sqlContext = new SQLContext(sc)

  /* Cassandra setup */
  CassandraConnector(conf).withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS githubstats")
    session.execute("CREATE KEYSPACE githubstats WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("CREATE TABLE githubstats.monthly_commits (user VARCHAR PRIMARY KEY, commits INT, date INT)")
  }

  val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafka.kafkaParams, Map("github" -> 10), StorageLevel.MEMORY_ONLY)
    .map(_._2)

  kafkaStream.foreachRDD { rdd =>
    sqlContext.jsonRDD(rdd).registerTempTable("mytable")
    sqlContext.sql(
      "SELECT user, commits, month, year FROM mytable WHERE commits >= 5 AND year = 2015")
      .map(MonthlyCommits(_))
      .saveToCassandra("githubstats","monthly_commits")
  }

  ssc.start()

  val table = sc.cassandraTable("githubstats", "monthly_commits")
  awaitCond(table.collect.size > 1, 5.seconds)
  println("Reading from githubstats.monthly_commits in cassandra:")
  table.collect foreach println

  ssc.awaitTermination()

}

