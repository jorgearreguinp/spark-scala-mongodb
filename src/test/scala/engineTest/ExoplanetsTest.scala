package engineTest

import common.Constants._
import common.engine.Exoplanets._
import common.naming.ExoplanetsObjects._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

class ExoplanetsTest extends AnyFlatSpec with Matchers {

  val spark: SparkSession = SparkSession.builder()
    .master(MASTER)
    .appName(APP_NAME)
    .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/jorge.exoplanets")
    .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/jorge.exoplanets")
    .getOrCreate()

  val exoplanetsDF: DataFrame = spark.read.format("mongodb").load()

  "When I execute stellarProperties method" should
  "contain all columns in DF" in {
    val joinExoplanetsDF = addingNumberColumn(exoplanetsDF)
    var df = massNotNull(joinExoplanetsDF)
    df = discoveredBeforeTwentyTen(df)
    df = numberOfStarsBetweenThreeAndFive(df)
    df = sha1GaiaMagnitude(df)
    df = setDateOnDiscoveryYear(df)
    df = rankedDiscoveryMethod(df)
    df = stellarProperties(df)

    df.columns should contain allOf(
      ID.name,
      Number.name,
      PlanetHost.name.replaceAll(" ", ""),
      NumStars.name.replaceAll(" ", ""),
      NumPlanets.name.replaceAll(" ", ""),
      DiscoveryMethod.name.replaceAll(" ", ""),
      DiscoveryYear.name.replaceAll(" ", ""),
      DiscoveryFacility.name.replaceAll(" ", ""),
      OrbitalPeriodDays.name.replaceAll(" ", ""),
      OrbitSemiMajorAxis.name.replaceAll(" ", ""),
      Mass.name.replaceAll(" ", ""),
      Eccentricity.name.replaceAll(" ", ""),
      InsolationFlux.name.replaceAll(" ", ""),
      EquilibriumTemperature.name.replaceAll(" ", ""),
      SpectralType.name.replaceAll(" ", ""),
      StellarEffectiveTemperature.name.replaceAll(" ", ""),
      StellarRadius.name.replaceAll(" ", ""),
      StellarMass.name.replaceAll(" ", ""),
      StellarMetallicity.name.replaceAll(" ", ""),
      StellarMetallicityRatio.name.replaceAll(" ", ""),
      StellarSurfaceGravity.name.replaceAll(" ", ""),
      Distance.name.replaceAll(" ", ""),
      GaiaMagnitude.name.replaceAll(" ", ""),
      RowNumber.name,
      Rank.name,
      DenseRank.name
    )
  }

}
