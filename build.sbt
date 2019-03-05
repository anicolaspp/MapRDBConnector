
import ReleaseTransformations._

name := "MapRDBConnector"

version := "1.0.0-SNAPSHOT"

scalaVersion := "2.11.8"


lazy val maprdbconnector = project.in(file("."))
  .settings(

    homepage := Some(url("https://github.com/anicolaspp/MapRDBConnector")),

    scmInfo := Some(ScmInfo(url("https://github.com/anicolaspp/MapRDBConnector"), "git@github.com:anicolaspp/MapRDBConnector.git")),

    pomExtra := <developers>
      <developer>
        <name>Nicolas A Perez</name>
        <email>anicolaspp@gmail.com</email>
        <organization>anicolaspp</organization>
        <organizationUrl>https://github.com/anicolaspp</organizationUrl>
      </developer>
      <developer>
        <name>Ivan Ulianov</name>
        <organization>iulianov</organization>
        <organizationUrl>https://github.com/iulianov</organizationUrl>
      </developer>
    </developers>,

    licenses += ("MIT License", url("https://opensource.org/licenses/MIT")),

    publishMavenStyle := true,

    publishTo in ThisBuild := Some(
      if (isSnapshot.value)
        Opts.resolver.sonatypeSnapshots
      else
        Opts.resolver.sonatypeStaging
    ),

    publishArtifact in Test := false,

    pomIncludeRepository := { _ => true },

    releaseProcess := Seq[ReleaseStep](
      checkSnapshotDependencies,
      inquireVersions,
      runClean,
      runTest,
      setReleaseVersion,
      commitReleaseVersion,
      tagRelease,
      publishArtifacts,
      setNextVersion,
      commitNextVersion,
      ReleaseStep(action = Command.process("sonatypeReleaseAll", _)),
      pushChanges
    ),

    resolvers += "MapR Releases" at "http://repository.mapr.com/maven/",

    resolvers += "Typesafe repository" at "http://repo.typesafe.com/typesafe/releases/",

    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.11" % "2.3.2" % "provided",
      "org.apache.spark" % "spark-sql_2.11" % "2.3.2" % "provided",

      "org.ojai" % "ojai" % "3.0-mapr-1808",
      "org.ojai" % "ojai-scala" % "3.0-mapr-1808",

      "com.mapr.db" % "maprdb-spark" % "2.3.1-mapr-1808" % "provided",
      "com.mapr.db" % "maprdb" % "6.1.0-mapr" % "provided",
      "xerces" % "xercesImpl" % "2.11.0" % "provided"
    )
  )

addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)

assemblyMergeStrategy in assembly := {
  case PathList("org", "apache", "spark", "unused", xs@_*) => MergeStrategy.last
  case x =>
    val oldStrategy = (assemblyMergeStrategy in assembly).value
    oldStrategy(x)
}

assemblyJarName := s"${name.value}-${version.value}.jar"
