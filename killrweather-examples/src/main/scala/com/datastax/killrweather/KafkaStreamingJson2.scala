package com.datastax.killrweather

import scala.concurrent.duration._
import com.datastax.killrweather.GitHubEvents.MonthlyCommits
import kafka.producer.{KeyedMessage, Producer}
import kafka.serializer.StringDecoder
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s._
import org.json4s.native.JsonParser
import com.datastax.spark.connector.cql.CassandraConnector
import com.datastax.spark.connector.embedded.{Assertions, EmbeddedKafka}

/**
 * Uses json4s for json work from the kafka stream.
 */
object KafkaStreamingJson2 extends App with Assertions {
  import com.datastax.spark.connector.streaming._

  implicit val formats = DefaultFormats

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
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(1))

  /* Cassandra setup */
  CassandraConnector(conf).withSessionDo { session =>
    session.execute("DROP KEYSPACE IF EXISTS githubstats")
    session.execute("CREATE KEYSPACE githubstats WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': 1 }")
    session.execute("""CREATE TABLE githubstats.monthly_commits (user VARCHAR PRIMARY KEY, commits INT, month INT, year INT)""")
  }

  val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
    ssc, kafka.kafkaParams, Map("github" -> 5), StorageLevel.MEMORY_ONLY)
    .map{ case (_,v) => JsonParser.parse(v).extract[MonthlyCommits]}
    .saveToCassandra("githubstats","monthly_commits")

  ssc.start()

  /* validate */
  val table = ssc.cassandraTable[MonthlyCommits]("githubstats", "monthly_commits")
  awaitCond(table.collect.size > 1, 5.seconds)
  table.toLocalIterator foreach println

  ssc.awaitTermination()
}

