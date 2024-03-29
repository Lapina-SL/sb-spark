lazy val commonSettings = Seq(
  name := "filter",
  version := "1.0",
  scalaVersion := "2.11.12",
  libraryDependencies += "org.apache.spark" %%  "spark-core" % "2.4.6",
  libraryDependencies += "org.apache.spark" %%  "spark-sql" % "2.4.6",
)

lazy val root = (project in file(".")).
  settings(commonSettings: _*)