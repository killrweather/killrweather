package com.datastax.killrweather.api

import java.util.UUID

import org.json4s._

object WeatherCenterJson {
  import com.datastax.killrweather.api.WeatherApi._

  lazy val formats: Formats => Formats =
    _ + uuidSerializer + hints

  lazy private val hints = FullTypeHints(List(
    classOf[HiLowForecast], classOf[HiLowForecast]
  ))

  protected object uuidSerializer extends CustomSerializer[UUID](format => (
    { case JString(uuid) => UUID.fromString(uuid) },
    { case uuid: UUID => JString(uuid.toString) }))
}
