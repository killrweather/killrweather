package com.datastax.killrweather.service

import akka.actor.{ActorSystem, Props}
import akka.testkit.{TestActorRef, TestKit, TestProbe}
import com.datastax.killrweather.{KafkaProperties, WeatherStationId}
import com.datastax.spark.connector.embedded.KafkaEvent.KafkaMessageEnvelope
import org.joda.time.{DateTime, Duration}
import org.scalatest.FunSuiteLike

import scala.language.postfixOps
import scala.util.Random

class DataGeneratorActorTest extends TestKit(ActorSystem("DataGeneratorActorTest")) with FunSuiteLike with KafkaProperties {
  test("Should generate messages") {
    val kafkaSender = TestProbe()
    val weatherStation = WeatherStationId("1234")
    val now = DateTime.now
    val duration = Duration.millis(1)
    // make this deterministic by hard coding seed
    val random = new Random(1)
    val underTest = TestActorRef(Props(new DataGeneratorActor(kafkaSender.ref, weatherStation, random)))

    underTest ! LoadSpec(now.minusHours(1), now, duration)

    val firstMsg = s"${weatherStation.id},${now.year().get()},${now.monthOfYear().get()},${now.dayOfMonth().get()},00,5.0,-3.9,1020.4,270,4.6,2,3.6543909535164545,5.0"
    kafkaSender.expectMsg(KafkaMessageEnvelope[String, String](KafkaTopic, KafkaKey, firstMsg))
    val next = now.plus(duration)
    val secondMsg = s"${weatherStation.id},${next.year().get()},${next.monthOfYear().get()},${next.dayOfMonth().get()},00,5.0,-3.9,1020.4,270,4.6,2,2.0504040574610083,5.0"
    kafkaSender.expectMsg(KafkaMessageEnvelope[String, String](KafkaTopic, KafkaKey, secondMsg))
  }
}
