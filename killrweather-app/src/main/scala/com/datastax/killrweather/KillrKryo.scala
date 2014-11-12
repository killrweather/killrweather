package com.datastax.killrweather

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
/** work in progress */
class KillrKryoRegistrator extends KryoRegistrator {

  import WeatherEvent._
  import Weather._

  override def registerClasses(kryo: Kryo) {
    kryo.register(classOf[RawWeatherData])
  }
}