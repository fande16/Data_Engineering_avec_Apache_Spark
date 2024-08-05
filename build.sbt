ThisBuild / version := "0.1.0"

ThisBuild / scalaVersion := "2.12.12"

lazy val sparkVersion = sys.env.getOrElse("SPARK_VERSION", "2.4.5")

lazy val root = (project in file("."))
  .settings(
    name := "project_scala_spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "com.typesafe.scala-logging" %% "scala-logging" % "3.9.5",

    )
  )
