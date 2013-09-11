name := "decl"

version := "0.0.1"

scalaVersion := "2.9.2"

organization := "com.mikegagnon"

scalacOptions ++= Seq("-unchecked", "-deprecation")

// Unfortunately, Scalding job tests cannot run in parallel
parallelExecution in Test := false

resolvers ++= Seq(
  "snapshots" at "http://oss.sonatype.org/content/repositories/snapshots",
  "releases" at "http://oss.sonatype.org/content/repositories/releases",
  "Concurrent Maven Repo" at "http://conjars.org/repo",
  "Clojars Repository" at "http://clojars.org/repo",
  "Twitter Maven" at "http://maven.twttr.com"
)

libraryDependencies ++= Seq(
  "junit" % "junit" % "4.8.1" % "test",
  "org.scalatest" % "scalatest_2.9.2" % "1.9.1",
  "com.twitter" % "scalding-core_2.9.2" % "0.8.8",
  "com.twitter" % "scalding-commons_2.9.2" % "0.2.0"
)
