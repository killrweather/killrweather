package com.datastax.killrweather

import java.io.{File => JFile}

import scala.util.Try
import scala.concurrent.duration._
import akka.actor.ActorRef
import org.apache.spark.{SparkContext, SparkConf}
import com.datastax.spark.connector._

abstract class ActorSparkSpec extends AkkaSpec with AbstractSpec {
  import Weather._
  import settings._

  lazy val conf = new SparkConf().setAppName(getClass.getSimpleName)
    .setMaster(settings.SparkMaster)
    .set("spark.cassandra.connection.host", settings.CassandraHosts)
    .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .set("spark.kryo.registrator", "com.datastax.killrweather.KillrKryoRegistrator")
    .set("spark.cleaner.ttl", settings.SparkCleanerTtl.toString)

  lazy val sc = new SparkContext(conf)

  val kafkaActor: Option[ActorRef] = None

  def start(clean: Boolean = true): Unit = {
    val initialization = new Initializer(sc, settings)
    initialization.start(clean, kafkaActor)
    log.info("Initialization completed.")
  }

  /* Acquire a valid sample to query with. Only exists if you call start() first. */
  lazy val sample: Day = Try {
    val data = sc.cassandraTable[RawWeatherData](CassandraKeyspace, CassandraTableRaw)
    awaitCond(data.collect.size >= 1000, 30.seconds)
    Day(data.first)
  }.getOrElse(throw new IllegalStateException(
    "Call the 'start' function before requesting the sample."))

  override def afterAll() {
    deleteOnExit()
    super.afterAll()
  }

  protected def path(name: String): String = {
    val dir = new JFile(DataLoadPath).getParentFile.getAbsolutePath
    new JFile(s"$dir/$name").getAbsolutePath
  }


  private def deleteOnExit(): Unit =
    new JFile(".").list.collect {
      case path if path.startsWith(getClass.getSimpleName) && path.endsWith(".test.output") =>
        val dir = new scala.reflect.io.Directory(new JFile(path))
        dir.deleteRecursively()
    }
}