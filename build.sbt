name := "decl"

version := "0.0.1"

scalaVersion := "2.9.2"

organization := "com.mikegagnon"

scalacOptions ++= Seq("-unchecked")

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.8.1" % "test",
  "org.scalatest" % "scalatest_2.9.2" % "1.9.1"
)
