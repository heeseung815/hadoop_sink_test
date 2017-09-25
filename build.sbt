name := "hadoop_sink_test"

version := "0.1"

scalaVersion := "2.12.2"

lazy val akkaVersion = "2.5.4"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-stream" % akkaVersion,
//  "org.apache.orc" % "orc-mapreduce" % "1.4.0",
//  "org.apache.hadoop" % "hadoop-mapreduce-client-core" % "2.8.1"
  "org.apache.hadoop" % "hadoop-client" % "2.8.1",
//  "org.apache.hadoop" % "hadoop-common" % "2.8.1",
  "org.apache.hive" % "hive-exec" % "2.3.0",
  "org.apache.crunch" % "crunch-hive" % "0.15.0",
  "org.apache.logging.log4j" % "log4j-api" % "2.8.2"
)

// Without this repo, you might get a failure trying to resolve transitive
// dependency org.pentaho:pentaho-aggdesigner-algorithm:5.1.5-jhyde
resolvers += "conjars" at "http://conjars.org/repo"