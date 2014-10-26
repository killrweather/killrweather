package com.datastax.killrweather

import akka.actor.ActorSystem
import akka.serialization.SerializationExtension
import com.datastax.killrweather.Weather.RawWeatherData
import kafka.serializer.{Decoder, Encoder}
import kafka.utils.VerifiableProperties

class KillrWeatherEncoder(props: VerifiableProperties = null)(implicit system: ActorSystem) extends Encoder[RawWeatherData] {
  val serialization = SerializationExtension(system)

  def toBytes(t: RawWeatherData): Array[Byte] =
    serialization.serialize(t).get // ouch
}

class KillrWeatherDecoder(props: VerifiableProperties = null)(implicit system: ActorSystem) extends Decoder[RawWeatherData] {
  val serialization = SerializationExtension(system)

  def fromBytes(bytes: Array[Byte]): RawWeatherData =
    serialization.deserialize(bytes, classOf[RawWeatherData]).get // ouch
}
