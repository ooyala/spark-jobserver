import sbt._
import Keys._

object Dependencies {
  val excludeCglib = ExclusionRule(organization = "org.sonatype.sisu.inject")
  val excludeJackson = ExclusionRule(organization = "org.codehaus.jackson")
  val excludeNetty = ExclusionRule(organization = "org.jboss.netty")
  val excludeAsm = ExclusionRule(organization = "asm")

  lazy val typeSafeConfigDeps = "com.typesafe" % "config" % "1.0.0"
  lazy val yammerDeps = "com.yammer.metrics" % "metrics-core" % "2.2.0"

  lazy val yodaDeps = Seq(
    "org.joda" % "joda-convert" % "1.2",
    "joda-time" % "joda-time" % "2.1"
  )

  lazy val akkaDeps = Seq(
    // Akka is provided because Spark already includes it, and Spark's version is shaded so it's not safe
    // to use this one
    "com.typesafe.akka" %% "akka-slf4j" % "2.2.4" % "provided",
    "io.spray" %% "spray-json" % "1.2.5",
    // upgrade version from 1.2.0 to 1.2.1 to solve the logging noise issue
    // details here: https://groups.google.com/forum/#!msg/spray-user/YN2ocRzwhY0/KJOegaDIep8J
    // NOTE: DO NOT upgrade to 1.2.2 since it is incompatiable and will cause tests fail
    "io.spray" % "spray-can" % "1.2.1",
    "io.spray" % "spray-routing" % "1.2.1",
    "io.spray" % "spray-client" % "1.2.1",
    yammerDeps
  ) ++ yodaDeps

  lazy val sparkDeps = Seq(
    "org.apache.spark" %% "spark-core" % "0.9.1" % "provided" exclude("io.netty", "netty-all"),
    // Force netty version.  This avoids some Spark netty dependency problem.
    "io.netty" % "netty" % "3.6.6.Final"
  )

  lazy val slickDeps = Seq(
    "com.typesafe.slick" %% "slick" % "2.0.2-RC1",
    "com.h2database" % "h2" % "1.3.170",
    "mysql" % "mysql-connector-java" % "5.1.31",
    "org.mariadb.jdbc" % "mariadb-java-client" % "1.1.7"
  )

  lazy val logbackDeps = Seq(
    "ch.qos.logback" % "logback-classic" % "1.0.7"
  )

  lazy val coreTestDeps = Seq(
    "org.scalatest" %% "scalatest" % "1.9.1" % "test",
    "com.typesafe.akka" %% "akka-testkit" % "2.2.4" % "test",
    "io.spray" % "spray-testkit" % "1.2.0" % "test"
  )


  lazy val serverDeps = apiDeps ++ yodaDeps
  lazy val apiDeps = sparkDeps :+ typeSafeConfigDeps
  lazy val monitoringDeps = Seq(
    "com.codahale.metrics" % "metrics-core" % "3.0.1",
    "org.coursera" % "metrics-datadog" % "1.0.1",
    "org.apache.httpcomponents" % "fluent-hc" % "4.3.2",
    "org.apache.httpcomponents" % "httpcore" % "4.3.2"
  )

  val repos = Seq(
    "Typesafe Repo" at "http://repo.typesafe.com/typesafe/releases/",
    "sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots/",
    "spray repo" at "http://repo.spray.io"
  )
}
