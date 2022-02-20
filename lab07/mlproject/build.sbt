ThisBuild / version := "0.1"

ThisBuild / scalaVersion := "2.11.12"

lazy val root = (project in file("."))
  .settings(
    name := "mlproject",
    libraryDependencies += "org.apache.spark" %%  "spark-core" % "2.4.7",
    libraryDependencies += "org.apache.spark" %%  "spark-sql" % "2.4.7",
    libraryDependencies += "org.apache.spark" %%  "spark-mllib" % "2.4.6"
  )