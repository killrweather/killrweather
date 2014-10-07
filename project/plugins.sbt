// pulls in: sbt-pgp, sbt-release, sbt-mima-plugin, sbt-dependency-graph, sbt-buildinfo, sbt-sonatype
addSbtPlugin("org.typelevel" % "sbt-typelevel" % "0.3.1")

addSbtPlugin("com.typesafe.sbt" % "sbt-scalariform" % "1.3.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.6.2")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.11.2")

addSbtPlugin("com.earldouglas" % "xsbt-web-plugin" % "0.4.2")

// For Intellij users:
// This might already be in ~/.sbt.. for Scala users,
// but for Java users that don't have it already...
addSbtPlugin("com.github.mpeltonen" % "sbt-idea" % "1.6.0")