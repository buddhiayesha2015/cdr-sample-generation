name := "cdr-sample-generation"

version := "1.4.2"

scalaVersion := "2.11.8"

scalacOptions ++= Seq("-unchecked", "-deprecation")

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.12" % "2.4.6" % "provided",
  "org.apache.spark" % "spark-sql_2.12" % "2.4.6" % "provided"
)

lazy val commonSettings = Seq(
  organization := "lk.uom.datasearch",
  test in assembly := {}
)

lazy val app = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    mainClass in assembly := Some("lk.uom.datasearch.bhagya.SampleCreation"),
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false)
  )

assemblyMergeStrategy in assembly := {
  case PathList("javax", "servlet", xs@_*) => MergeStrategy.last
  case PathList("javax", "activation", xs@_*) => MergeStrategy.last
  case PathList("org", "apache", xs@_*) => MergeStrategy.last
  case "application.conf" => MergeStrategy.concat
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}
