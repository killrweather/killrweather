package com.datastax.killrweather

import java.util.concurrent.{TimeUnit, CountDownLatch}

import scala.concurrent.duration._
import akka.actor.Props
import com.datastax.spark.connector.embedded.EmbeddedKafka
import com.datastax.spark.connector.streaming._

trait KafkaSpec extends ActorSparkSpec {


  implicit val ec = system.dispatcher

  val year = 2005

  val sid = "010010:99999"

  lazy val kafkaServer = new EmbeddedKafka

  kafkaServer.createTopic(settings.KafkaTopicRaw)
}

class KafkaSupervisorSpec extends KafkaSpec { 
  import WeatherEvent._
  import settings._

  val latch = new CountDownLatch(100)

  val kafkaActor = system.actorOf(Props(new KafkaSupervisor(kafkaServer, ssc, settings, self)), "kafka")

  expectMsgPF(1.minute) {
    case OutputStreamInitialized => start()
  }

  val consumer = new KafkaTestConsumer(kafkaServer.kafkaConfig.zkConnect, KafkaGroupId, KafkaTopicRaw, 2, latch)

  override def afterAll() {
    consumer.shutdown()
    kafkaServer.shutdown()
    super.afterAll()
  }

  "KafkaSupervisor" must {
    "RawDataPublisher: transforms annual weather .gz files to line data and publish to a Kafka topic" in {
      kafkaActor ! PublishFeed

      latch.await(5.minutes.toMillis, TimeUnit.MILLISECONDS)
      latch.getCount should be (0) // we have at least 100 messages in kafka
    }
    "KafkaStreamActor: streams in data from kafka, transforms it, and saves it to Cassandra" in {
      val rows = ssc.cassandraTable(CassandraKeyspace, CassandraTableRaw)
        .where("weather_station = ? AND year = ? and month = ?", sid, 2005, 1)
        .toLocalIterator
    }
  }
}