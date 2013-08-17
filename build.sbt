name := "decl"

version := "0.0.1"

scalaVersion := "2.9.3"

organization := "com.mikegagnon"

scalacOptions ++= Seq("-unchecked")

resolvers ++= Seq(
  "Concurrent Maven Repo" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.8.1" % "test",
  "org.scalatest" % "scalatest_2.9.3" % "1.9.1",
  "com.twitter" % "algebird-core_2.9.3" % "0.2.0",
  "com.twitter" % "scalding-core_2.9.3" % "0.8.8"
)
