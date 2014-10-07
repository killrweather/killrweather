package com.datastax.killrweather.api

import com.datastax.killrweather.Weather
import org.joda.time.{DateTime, DateTimeZone}

object WeatherApi {
  import Weather._

  sealed trait Forecast extends DataRequest
  sealed trait Aggregate extends Forecast

  class WeatherStationId private (val value: String) extends AnyVal {
    override def toString: String = s"Id($value)"
  }

  object WeatherStationId {
    import javax.servlet.http.HttpServletRequest

import scalaz._

    val HttpHeader = "X-BLUEPRINTS-STATION-ID"

    def apply(value: String): Validation[String, WeatherStationId] =
      if (regex.pattern.matcher(value).matches) Success(new WeatherStationId(value.toLowerCase))
      else Failure(s"invalid Id '$value'")

    def apply(request: HttpServletRequest): Option[Validation[String, WeatherStationId]] =
      Option(request.getHeader(HttpHeader)) map (id => WeatherStationId(id))

    private val regex = """[0-9]+:[0-9]+""".r
  }

  /** The response data with high-low temps. */
  case class HiLowForecast() extends WeatherModel

  /**
   * Quick access lookup table for sky_condition. Useful for potential analytics.
   * See http://en.wikipedia.org/wiki/Okta
   */
  case class SkyConditionLookup(code: Int, condition: String) extends WeatherModel


  /** Composite of Air Force Datsav3 station number and NCDC WBAN number
    * @param sid uses the composite key format: stationNum:wbanNum
    */
  case class GetWeatherStation(sid: WeatherStationId) extends DataRequest
  case class GetRawWeatherData(perPage: Int) extends DataRequest
  case object GetSkyConditionLookup extends DataRequest
  case class GetHiLow(zipcode: Int, dayOfYear: Int) extends DataRequest
  object GetHiLow {
    def apply(zip: Int, doy: Option[Int] = None): GetHiLow =
      GetHiLow(zip, doy getOrElse new DateTime(DateTimeZone.UTC).dayOfYear().get())
  }
  /**
   * TODO: what type of data params do we want in order to request this?
   */
  case class ComputeHiLow() extends Aggregate
}
