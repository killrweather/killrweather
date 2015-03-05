package com.datastax.killrweather

import scala.concurrent.duration._
import com.datastax.killrweather.GitHubEvents.MonthlyCommits
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{Assertions, EmbeddedKafka}

/**
 * Uses spark sql for json work.
 * @see [[https://github.com/killrweather/killrweather/blob/master/killrweather-examples/src/main/scala/com/datastax/killrweather/KafkaStreamingJson2.scala"]]
 *      for a cleaner version without Spark SQL doing the JSON mapping.
 */
object KafkaStreamingJson extends App with Assertions {
  import com.datastax.spark.connector.streaming._
  import com.datastax.spark.connector._

  /* Small sample data */
  val data = Seq(
    """{"user":"helena","commits":98, "month":3, "year":2015}""",
    """{"user":"jacek-lewandowski", "commits":72, "month":3, "year":2015}""",
    """{"user":"pkolaczk", "commits":42, "month":3, "year":2015}""")

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
  val ssc = new StreamingContext(sc, Seconds(1))
  val sqlContext = new SQLContext(sc)

  /* Cassandra setup */
  CassandraConnector(conf).withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS githubstats")
    session.execute("CREATE KEYSPACE githubstats WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("""CREATE TABLE githubstats.monthly_commits (user VARCHAR PRIMARY KEY, commits INT, month INT, year INT)""")
  }

  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafka.kafkaParams, Map("github" -> 5), StorageLevel.MEMORY_ONLY)
    .map(_._2)

  stream.foreachRDD { rdd =>
    /* this check is here to handle the empty collection error
       after the 3 items in the static sample data set are processed */
      if (rdd.toLocalIterator.nonEmpty) {
        sqlContext.jsonRDD(rdd).registerTempTable("mytable")
        sqlContext.sql(
          "SELECT user, commits, month, year FROM mytable WHERE commits >= 5 AND year = 2015")
          .map(MonthlyCommits(_))
          .saveToCassandra("githubstats","monthly_commits")
      }
  }

  stream.print

  ssc.start()

  /* validate */
  val table = sc.cassandraTable("githubstats", "monthly_commits")
  awaitCond(table.collect.size > 1, 8.seconds)
  table.toLocalIterator foreach println

  ssc.awaitTermination()

}

