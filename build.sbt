lazy val compilerSettings = Seq(
  scalacOptions ++= Seq(
    "-language:higherKinds",
    "-Ypartial-unification",
    "-Yliteral-types",
    "-encoding", "UTF-8",
    "-language:implicitConversions",
    "-language:postfixOps",
    "-language:existentials"
  ),
  addCompilerPlugin("org.spire-math" %% "kind-projector" % "0.9.3"),
  addCompilerPlugin("com.github.mpilquist" %% "simulacrum" % "0.10.0")
)

lazy val commonResolvers = Seq(
  Resolver.sonatypeRepo("releases"),
  Resolver.sonatypeRepo("snapshots"),
  Resolver.jcenterRepo
)

lazy val buildSettings = Seq(
  licenses += ("Apache-2.0", url("https://www.apache.org/licenses/LICENSE-2.0.html")),
  organization := "com.github.yilinwei",
  scalaOrganization := "org.typelevel",
  scalaVersion := "2.12.1",
  name := "fs2-hazelcast",
  version := "0.1.0-SNAPSHOT"
)

lazy val catsVersion = "0.9.0"

lazy val commonSettings = Seq(
  resolvers := commonResolvers,
  libraryDependencies ++= Seq(
    "org.typelevel" %% "cats-core" % catsVersion,
    "org.typelevel" %% "cats-free" % catsVersion,
    "com.hazelcast" % "hazelcast" % "3.8",
    "co.fs2" %% "fs2-core" % "0.9.4",
    "co.fs2" %% "fs2-cats" % "0.3.0",
    "org.scalatest" %% "scalatest" % "3.0.1" % "test",
    "org.scalacheck" %% "scalacheck" % "1.13.4" % "test"
  )
) ++ compilerSettings

lazy val root = (project in file(".")).settings(
  buildSettings,
  commonSettings
)
