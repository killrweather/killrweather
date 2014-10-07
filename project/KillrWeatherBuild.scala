/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

import sbt._
import sbt.Keys._

object KillrWeatherBuild extends Build {
  import Settings._

  lazy val root = Project(
    id = "root",
    base = file("."),
    settings = parentSettings,
    aggregate = Seq(core, api, app)
  )

  /** Blueprint core setup and Basic samples. */
  lazy val core = Project(
    id = "core",
    base = file("./killrweather-core"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.core)
  )

  lazy val api = Project(
    id = "api",
    base = file("./killrweather-api"),
    dependencies = Seq(core),
    settings = defaultSettings ++ withContainer ++ Seq(libraryDependencies ++= Dependencies.api)
  ) configs (IntegrationTest) configs(config("container"))

  /** Timeseries apps: WeatherCenter, Campaign. */
  lazy val app = Project(
    id = "app",
    base = file("./killrweather-app"),
    dependencies = Seq(core, api),
    settings = defaultSettings ++ withContainer ++ Seq(libraryDependencies ++= Dependencies.app)
  ) configs (IntegrationTest) configs(config("container"))

  /** More sample apps coming, using ML and Spark SQL. */
}

/** To use the connector, the only dependency required is:
  * "com.datastax.spark"  %% "spark-cassandra-connector" and possibly slf4j.
  * The others are here for other non-spark core and streaming code.
  */
object Dependencies {
  import Versions._

  object Compile {
    val akkaActor         = "com.typesafe.akka"   %% "akka-actor"                         % Akka    force() // ApacheV2
    val akkaCluster       = "com.typesafe.akka"   %% "akka-cluster"                       % Akka            // ApacheV2
    val akkaContrib       = "com.typesafe.akka"   %% "akka-contrib"                       % Akka            // ApacheV2
    val akkaRemote        = "com.typesafe.akka"   %% "akka-remote"                        % Akka            // ApacheV2
    val akkaSlf4j         = "com.typesafe.akka"   %% "akka-slf4j"                         % Akka            // ApacheV2
    val jodaTime          = "joda-time"           % "joda-time"                           % JodaTime        // ApacheV2
    val jodaConvert       = "org.joda"            % "joda-convert"                        % JodaConvert     // ApacheV2
    val json4sCore        = "org.json4s"          %% "json4s-core"                        % Json4s          // ApacheV2
    val json4sJackson     = "org.json4s"          %% "json4s-jackson"                     % Json4s          // ApacheV2
    val json4sNative      = "org.json4s"          %% "json4s-native"                      % Json4s          // ApacheV2
    val kafka             = "org.apache.kafka"    %% "kafka"                              % Kafka           // ApacheV2
    val scalazContrib     = "org.typelevel"       %% "scalaz-contrib-210"                 % ScalazContrib   // MIT
    val scalazContribVal  = "org.typelevel"       %% "scalaz-contrib-validation"          % ScalazContrib   // MIT
    val scalazContribUndo ="org.typelevel"        %% "scalaz-contrib-undo"                % ScalazContrib   // MIT
    val scalazNst         = "org.typelevel"       %% "scalaz-nscala-time"                 % ScalazContrib   // MIT
    val scalazSpire       = "org.typelevel"       %% "scalaz-spire"                       % ScalazContrib   // MIT
    val scalazStream      = "org.scalaz.stream"   %% "scalaz-stream"                      % ScalazStream    // MIT
    val slf4jApi          = "org.slf4j"           % "slf4j-api"                           % Slf4j           // MIT
    val sparkML           = "org.apache.spark"    %% "spark-mllib"                        % Spark           // ApacheV2
    val sparkCassandra    = "com.datastax.spark"  %% "spark-cassandra-connector"          % SparkCassandra  // ApacheV2
    val sparkCassandraEmb = "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % SparkCassandra  // ApacheV2

  }

  object Runtime {
    val jettyWebapp       = "org.eclipse.jetty"   % "jetty-webapp"                        % JettyWebapp  % "container"
    val servletApi        = "javax.servlet"       % "javax.servlet-api"                   % JavaxServlet % "container"
    val scalatra          = "org.scalatra"        %% "scalatra"                           % Scalatra
    val scalatraJson      = "org.scalatra"        %% "scalatra-json"                      % Scalatra
  }

  /* TBD if i'll keep these or not, it's just for running the API without deploying to tomcat
  - ease of use during Presentations, so we can just run in IDE or SBT command line. */
  object Test {
    val akkaTestKit     = "com.typesafe.akka"     %% "akka-testkit"                       % Akka        % "test,it"
    val scalatest       = "org.scalatest"         %% "scalatest"                          % "2.2.1"     % "test,it"
    val scalatraTest    = "org.scalatra"          %% "scalatra-scalatest"                 % "2.2.2"     % "test,it"
  }

  import Compile._

  val akka = Seq(akkaActor, akkaCluster, akkaContrib, akkaRemote)

  val connector = Seq(sparkCassandra, sparkCassandraEmb)

  val json = Seq(json4sCore, json4sJackson, json4sNative)

  val logging = Seq(akkaSlf4j, slf4jApi)

  val rest = Seq(Runtime.jettyWebapp, Runtime.scalatra, Runtime.scalatraJson, Runtime.servletApi)

  val scalaz = Seq(scalazContrib, scalazContribVal, scalazContribUndo, scalazNst, scalazSpire, scalazStream)

  val time = Seq(jodaConvert, jodaTime)

  val test = Seq(Test.akkaTestKit, Test.scalatest, Test.scalatraTest)

  /** Module deps */
  val core = connector ++ scalaz

  val api = json ++ rest ++ scalaz ++ time ++ test ++ Seq(akkaActor, sparkCassandra)

  val app = akka ++ json ++ connector ++ logging ++
    rest ++ scalaz ++ time ++ test ++ Seq(kafka, sparkML)

}
