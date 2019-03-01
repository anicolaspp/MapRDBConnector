
name := "MapRDBConnector"

version := "1.0.0"

scalaVersion := "2.11.8"


resolvers += "MapR Releases" at "http://repository.mapr.com/maven/"

resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/"



libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % "2.3.2" % "provided",
  "org.apache.spark" % "spark-sql_2.11" % "2.3.2" % "provided",

  "org.ojai" % "ojai" % "3.0-mapr-1808",
  "org.ojai" % "ojai-scala" % "3.0-mapr-1808",
  
  "com.mapr.db" % "maprdb-spark" % "2.3.1-mapr-1808" % "provided",
  "com.mapr.db" % "maprdb" % "6.1.0-mapr" % "provided",
  "xerces" % "xercesImpl" % "2.11.0" % "provided" //Needs to be manually added since there is a reference to SP5 in the chain from com.mapr.db-maprdb that sbt was not able to find
)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := s"${name.value}-${version.value}.jar"
