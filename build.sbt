name := "spark-sandbox"

version := "1.0"

scalaVersion := "2.11.7"

resolvers ++= Seq(
  "Apache Staging" at "https://repository.apache.org/content/repositories/staging/",
  "Typesafe" at "http://repo.typesafe.com/typesafe/releases",
  "scalaz-bintray" at "http://dl.bintray.com/scalaz/releases"
)

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.5.0" % "compile",
  "org.apache.spark" %% "spark-mllib" % "1.5.0" % "compile",
  "org.apache.lucene" % "lucene-benchmark" % "5.5.0" % "compile"
)

scalacOptions ++= Seq("-deprecation", "-encoding", "UTF-8")
