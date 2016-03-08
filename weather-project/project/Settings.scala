/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.language.postfixOps

import sbt._
import sbt.Keys._
import net.virtualvoid.sbt.graph.Plugin.graphSettings
import com.scalapenos.sbt.prompt.SbtPrompt.autoImport._

object Settings extends Build {

  lazy val buildSettings = Seq(
    name := "KillrWeather",
    normalizedName := "killrweather",
    organization := "com.datastax.killrweather",
    organizationHomepage := Some(url("http://www.github.com/killrweather/killrweather")),
    scalaVersion := Versions.Scala,
    homepage := Some(url("https://github.com/killrweather/killrweather")),
    licenses := Seq(("Apache License, Version 2.0", url("http://www.apache.org/licenses/LICENSE-2.0"))),
    promptTheme := ScalapenosTheme
  )

  override lazy val settings = super.settings ++ buildSettings

  val parentSettings = buildSettings ++ Seq(
    publishArtifact := false,
    publish := {}
  )

  lazy val defaultSettings = testSettings ++ graphSettings ++ sigarSettings ++ Seq(
    autoCompilerPlugins := true,
    // removed "-Xfatal-warnings" as temporary workaround for log4j fatal error.
    scalacOptions ++= Seq("-encoding", "UTF-8", s"-target:jvm-${Versions.JDK}", "-feature", "-language:_", "-deprecation", "-unchecked", "-Xlint"),
    javacOptions in Compile ++= Seq("-encoding", "UTF-8", "-source", Versions.JDK, "-target", Versions.JDK, "-Xlint:deprecation", "-Xlint:unchecked"),
    run in Compile <<= Defaults.runTask(fullClasspath in Compile, mainClass in (Compile, run), runner in (Compile, run)),
    ivyLoggingLevel in ThisBuild := UpdateLogging.Quiet,
    parallelExecution in ThisBuild := false,
    parallelExecution in Global := false/*,
    ivyXML := <dependencies>
      <exclude org="org.slf4j" module="slf4j-log4j12"/>
    </dependencies>*/
  )

  val tests = inConfig(Test)(Defaults.testTasks) ++ inConfig(IntegrationTest)(Defaults.itSettings)

  val testOptionSettings = Seq(
    Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
  )

  lazy val testSettings = tests ++ Seq(
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    testOptions in Test ++= testOptionSettings,
    testOptions in IntegrationTest ++= testOptionSettings,
    baseDirectory in Test := baseDirectory.value.getParentFile(),
    fork in Test := true,
    fork in IntegrationTest := true,
    (compile in IntegrationTest) <<= (compile in Test, compile in IntegrationTest) map { (_, c) => c },
    managedClasspath in IntegrationTest <<= Classpaths.concat(managedClasspath in IntegrationTest, exportedProducts in Test)
  )

  lazy val sigarSettings = Seq(
    unmanagedSourceDirectories in (Compile,run) += baseDirectory.value.getParentFile / "sigar",
    javaOptions in run ++= {
      System.setProperty("java.library.path", file("./sigar").getAbsolutePath)
      Seq("-Xms128m", "-Xmx1024m")
    })

}
