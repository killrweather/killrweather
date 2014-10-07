package com.datastax.killrweather.api

object ApiData {

  class UID private (val value: String) extends AnyVal {
    override def toString: String = s"Id($value)"
  }

  object UID {
    import javax.servlet.http.HttpServletRequest

import scalaz._

    val HttpHeader = "X-BLUEPRINTS-ID"

    def apply(value: String): Validation[String, UID] =
      if (regex.pattern.matcher(value).matches) Success(new UID(value.toLowerCase))
      else Failure(s"invalid Id '$value'")

    def apply(request: HttpServletRequest): Option[Validation[String, UID]] =
      Option(request.getHeader(HttpHeader)) map (id => UID(id))

    private val regex = """[0-9a-f]{32}""".r
  }

}
