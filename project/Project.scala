import sbt._
import Keys._
import com.typesafe.sbt.SbtStartScript
import com.typesafe.sbt.SbtScalariform._
import scalariform.formatter.preferences._

object BwlBuild extends Build {
  val PROJECT_NAME = "bwl"

  val commonResolvers = Seq(
    "Wajam" at "http://ci1.cx.wajam/",
    "Maven.org" at "http://repo1.maven.org/maven2",
    "Sun Maven2 Repo" at "http://download.java.net/maven/2",
    "Scala-Tools" at "http://scala-tools.org/repo-releases/",
    "Sun GF Maven2 Repo" at "http://download.java.net/maven/glassfish",
    "Oracle Maven2 Repo" at "http://download.oracle.com/maven",
    "Sonatype" at "http://oss.sonatype.org/content/repositories/release",
    "spy" at "http://files.couchbase.com/maven2/",
    "Twitter" at "http://maven.twttr.com/"
  )

  val commonDeps = Seq(
    "com.wajam" %% "commons-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "nrv-extension" % "0.1-SNAPSHOT",
    "com.wajam" %% "spnl-core" % "0.1-SNAPSHOT",
    "com.wajam" %% "scn-core" % "0.1-SNAPSHOT",
    "commons-io" % "commons-io" % "2.4" % "test,it",
    "org.scalatest" %% "scalatest" % "1.9.1" % "test,it",
    "junit" % "junit" % "4.10" % "test,it",
    "org.mockito" % "mockito-core" % "1.9.0" % "test,it"
  )

  val demoDeps = Seq(
    "com.typesafe" % "config" % "1.0.2"
  )

  def configureScalariform(pref: IFormattingPreferences): IFormattingPreferences = {
    pref.setPreference(AlignParameters, true)
      .setPreference(DoubleIndentClassDeclaration, true)
  }

  val defaultSettings = Defaults.defaultSettings ++ Defaults.itSettings ++ scalariformSettings ++ Seq(
    libraryDependencies ++= commonDeps,
    resolvers ++= commonResolvers,
    retrieveManaged := true,
    publishMavenStyle := true,
    organization := "com.wajam",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.2",
    ScalariformKeys.preferences := configureScalariform(FormattingPreferences())
  )

  lazy val root = Project(PROJECT_NAME, file("."))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .aggregate(core)
    .aggregate(demo)

  lazy val core = Project(PROJECT_NAME+"-core", file(PROJECT_NAME+"-core"))
    .configs(IntegrationTest)
    .settings(defaultSettings: _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in Test := false)
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)

  lazy val demo = Project(PROJECT_NAME+"-demo", file(PROJECT_NAME+"-demo"))
    .configs(IntegrationTest)
    .settings(defaultSettings ++ (libraryDependencies ++= demoDeps): _*)
    .settings(testOptions in IntegrationTest := Seq(Tests.Filter(s => s.contains("Test"))))
    .settings(parallelExecution in IntegrationTest := false)
    .settings(SbtStartScript.startScriptForClassesSettings: _*)
    .settings(unmanagedClasspath in Runtime <+= baseDirectory map { bd => Attributed.blank(bd / "../etc") })
    .settings(unmanagedClasspath in IntegrationTest <+= baseDirectory map { bd => Attributed.blank(bd / "../etc") })
    .dependsOn(core)
}
