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
    aggregate = Seq(core, app)
  )

  lazy val core = Project(
    id = "core",
    base = file("./killrweather-core"),
    settings = defaultSettings ++ Seq(libraryDependencies ++= Dependencies.core)
  )

  lazy val app = Project(
    id = "app",
    base = file("./killrweather-app"),
    dependencies = Seq(core),
    settings = defaultSettings ++ withSigar ++
      Seq(libraryDependencies ++= Dependencies.app)
  ) configs IntegrationTest

}

/** To use the connector, the only dependency required is:
  * "com.datastax.spark"  %% "spark-cassandra-connector" and possibly slf4j.
  * The others are here for other non-spark core and streaming code.
  */
object Dependencies {
  import Versions._

  object Compile {
    val akkaActor         = "com.typesafe.akka"   %% "akka-actor"                         % Akka    force() // ApacheV2
    val akkaCluster       = "com.typesafe.akka"   %% "akka-cluster"                       % Akka    force() // ApacheV2
    val akkaContrib       = "com.typesafe.akka"   %% "akka-contrib"                       % Akka    force() // ApacheV2
    val akkaRemote        = "com.typesafe.akka"   %% "akka-remote"                        % Akka    force() // ApacheV2
    val akkaSlf4j         = "com.typesafe.akka"   %% "akka-slf4j"                         % Akka    force() // ApacheV2
    val bijection         = "com.twitter"         %% "bijection-core"                     % "0.7.0"
    // can't use latest: spark :( val config = "com.typesafe"        % "config"   % Config  force() // ApacheV2
    val jodaTime          = "joda-time"           % "joda-time"                           % JodaTime        // ApacheV2
    val jodaConvert       = "org.joda"            % "joda-convert"                        % JodaConvert     // ApacheV2
    val json4sCore        = "org.json4s"          %% "json4s-core"                        % Json4s          // ApacheV2
    val json4sJackson     = "org.json4s"          %% "json4s-jackson"                     % Json4s          // ApacheV2
    val json4sNative      = "org.json4s"          %% "json4s-native"                      % Json4s          // ApacheV2
    val kafka             = "org.apache.kafka"    %% "kafka"                              % Kafka withSources() // ApacheV2
    val scalazContrib     = "org.typelevel"       %% "scalaz-contrib-210"                 % ScalazContrib   // MIT
    val scalazContribVal  = "org.typelevel"       %% "scalaz-contrib-validation"          % ScalazContrib   // MIT
    val scalazContribUndo = "org.typelevel"       %% "scalaz-contrib-undo"                % ScalazContrib   // MIT
    val scalazNst         = "org.typelevel"       %% "scalaz-nscala-time"                 % ScalazContrib   // MIT
    val scalazSpire       = "org.typelevel"       %% "scalaz-spire"                       % ScalazContrib   // MIT
    val scalazStream      = "org.scalaz.stream"   %% "scalaz-stream"                      % ScalazStream    // MIT
    val slf4jApi          = "org.slf4j"           % "slf4j-api"                           % Slf4j           // MIT
    val sparkML           = "org.apache.spark"    %% "spark-mllib"                        % Spark           // ApacheV2
    val sparkSQL          = "org.apache.spark"    %% "spark-sql"                          % Spark           // ApacheV2
    // for spark, streaming, sql and ml
    val sparkCassandra    = "com.datastax.spark"  %% "spark-cassandra-connector"          % SparkCassandra withSources() // ApacheV2
    val sparkCassandraEmb = "com.datastax.spark"  %% "spark-cassandra-connector-embedded" % SparkCassandra  // ApacheV2

  }

  object Test {
    val akkaTestKit     = "com.typesafe.akka"     %% "akka-testkit"                       % Akka         % "test,it" // ApacheV2
    val scalatest       = "org.scalatest"         %% "scalatest"                          % ScalaTest    % "test,it"
    val sigar           = "org.fusesource"        % "sigar"                               % Sigar        % "test,it"
  }

  import Compile._

  val akka = Seq(akkaActor, akkaCluster, akkaContrib, akkaRemote)

  val connector = Seq(sparkCassandra, sparkCassandraEmb)

  val json = Seq(json4sCore, json4sJackson, json4sNative)

  val logging = Seq(akkaSlf4j, slf4jApi)

  val scalaz = Seq(scalazContrib, scalazContribVal, scalazContribUndo, scalazNst, scalazSpire, scalazStream)

  val time = Seq(jodaConvert, jodaTime)

  val test = Seq(Test.akkaTestKit, Test.scalatest, Test.sigar)

  /** Module deps */
  val core = connector ++ json ++ scalaz ++ Seq(kafka)

  val app = akka ++ core ++ logging ++ time ++ test ++ Seq(sparkML, sparkSQL)

}
