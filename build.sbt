name := "Exoplanets"
version := "0.1"
scalaVersion := "2.12.15"

val sparkVersion = "3.2.0"

val sparkDependencies = Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.mongodb.spark" % "mongo-spark-connector_2.12" % "10.2.0",
  "org.scalatest" %% "scalatest" % sparkVersion % Test,
  "org.mockito" %% "mockito-scala" % "1.17.12" % Test
)

libraryDependencies ++= sparkDependencies
