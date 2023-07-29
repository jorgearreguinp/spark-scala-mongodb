package common.engine

import common.Constants._
import common.naming.ExoplanetsObjects._
import common.naming.WriteParquet.write
import org.apache.log4j.Logger
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}

object Exoplanets extends Serializable {

  lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master(MASTER)
      .appName(APP_NAME)
      .config("spark.mongodb.read.connection.uri", "mongodb://127.0.0.1/jorge.exoplanets")
      .config("spark.mongodb.write.connection.uri", "mongodb://127.0.0.1/jorge.exoplanets")
      .getOrCreate()

    val exoplanetsDF = spark.read.format("mongodb").load()

    exoplanetsDF.printSchema()

    val joinExoplanetsDF = addingNumberColumn(exoplanetsDF)

    var df = massNotNull(joinExoplanetsDF)
    df = discoveredBeforeTwentyTen(df)
    df = numberOfStarsBetweenThreeAndFive(df)
    df = sha1GaiaMagnitude(df)
    df = setDateOnDiscoveryYear(df)
    df = rankedDiscoveryMethod(df)
    df = stellarProperties(df)

    df.show(5, truncate = false)

    write("parquet", df)

  }

  def addingNumberColumn(df: DataFrame): DataFrame = {
    val numberColumnToCorrectType =
      df
        .select(
          col("No.*"),
          col("No")
        )
        .toDF("Number","No")

    val joinExoplanetsDF = numberColumnToCorrectType
      .join(
        df, Seq(No.name), "inner")
      .drop(No.column)

    joinExoplanetsDF
  }

  def massNotNull(df: DataFrame): DataFrame = {
    df
      .filter(
        Mass.column.isNotNull
      )
      .select(
        ID.column,
        Number.column.cast(IntegerType).alias(Number.name),
        PlanetName.column,
        PlanetHost.column,
        NumStars.column,
        NumPlanets.column,
        DiscoveryMethod.column,
        DiscoveryYear.column,
        DiscoveryFacility.column,
        OrbitalPeriodDays.column,
        OrbitSemiMajorAxis.column,
        when(Mass.column.isNull, 0)
          .otherwise(Mass.column).cast(IntegerType).alias(Mass.name),
        Eccentricity.column,
        InsolationFlux.column,
        EquilibriumTemperature.column,
        SpectralType.column,
        StellarEffectiveTemperature.column,
        StellarRadius.column,
        StellarMass.column,
        StellarMetallicity.column,
        StellarMetallicityRatio.column,
        StellarSurfaceGravity.column,
        Distance.column,
        GaiaMagnitude.column
      )
      .cache()
  }

  def betweenEarthMass(df: DataFrame): DataFrame = {
    df
      .filter(
        Mass.column.between(0, 2)
      )
      .cache()
  }

  def discoveredBeforeTwentyTen(df: DataFrame): DataFrame = {
    df
      .filter(
        DiscoveryYear.column < 2010
      )
      .select(
        SELECT_ALL
      )
  }

  def numberOfStarsBetweenThreeAndFive(df: DataFrame): DataFrame = {
    df
      .filter(
        NumStars.column.between(3,5)
      )
      .select(
        SELECT_ALL
      )
  }

  def sha1GaiaMagnitude(df: DataFrame): DataFrame = {
    df
      .filter(
        GaiaMagnitude.column.isNotNull
      )
      .select(
        ID.column,
        Number.column,
        PlanetName.column,
        PlanetHost.column,
        NumStars.column,
        NumPlanets.column,
        DiscoveryMethod.column,
        DiscoveryYear.column,
        DiscoveryFacility.column,
        OrbitalPeriodDays.column,
        OrbitSemiMajorAxis.column,
        Mass.column,
        Eccentricity.column,
        InsolationFlux.column,
        EquilibriumTemperature.column,
        SpectralType.column,
        StellarEffectiveTemperature.column,
        StellarRadius.column,
        StellarMass.column,
        StellarMetallicity.column,
        StellarMetallicityRatio.column,
        StellarSurfaceGravity.column,
        Distance.column,
        sha1(
          bin(GaiaMagnitude.column)
        ).alias(GaiaMagnitude.name)
      )
  }

  def setDateOnDiscoveryYear(df: DataFrame): DataFrame = {
    df
      .select(
        ID.column,
        Number.column,
        PlanetName.column,
        PlanetHost.column,
        NumStars.column,
        NumPlanets.column,
        DiscoveryMethod.column,
        concat(
          DiscoveryYear.column,
          lit("-01-01")
        ).cast(DateType).alias(DiscoveryYear.name),
        DiscoveryFacility.column,
        OrbitalPeriodDays.column,
        OrbitSemiMajorAxis.column,
        Mass.column,
        Eccentricity.column,
        InsolationFlux.column,
        EquilibriumTemperature.column,
        SpectralType.column,
        StellarEffectiveTemperature.column,
        StellarRadius.column,
        StellarMass.column,
        StellarMetallicity.column,
        StellarMetallicityRatio.column,
        StellarSurfaceGravity.column,
        Distance.column,
        GaiaMagnitude.column
      )
  }

  def rankedDiscoveryMethod(df: DataFrame): DataFrame = {
    val windowFunction = Window.partitionBy(DiscoveryMethod.column).orderBy(Number.column)
    df
      .select(
        ID.column,
        Number.column,
        PlanetName.column,
        PlanetHost.column,
        NumStars.column,
        NumPlanets.column,
        DiscoveryMethod.column,
        DiscoveryYear.column,
        DiscoveryFacility.column,
        OrbitalPeriodDays.column,
        OrbitSemiMajorAxis.column,
        Mass.column,
        Eccentricity.column,
        InsolationFlux.column,
        EquilibriumTemperature.column,
        SpectralType.column,
        StellarEffectiveTemperature.column,
        StellarRadius.column,
        StellarMass.column,
        StellarMetallicity.column,
        StellarMetallicityRatio.column,
        StellarSurfaceGravity.column,
        Distance.column,
        GaiaMagnitude.column,
        lit(row_number().over(windowFunction)).alias("row_number"),
        lit(rank().over(windowFunction)).alias("rank"),
        lit(dense_rank().over(windowFunction)).alias("dense_rank")
      )
  }

  def stellarProperties(df: DataFrame): DataFrame = {
    df
      .filter(
        StellarEffectiveTemperature.column.between(0, 5000) &&
          StellarRadius.column.isNotNull &&
          StellarMetallicity.column.isNotNull
      )
      .select(
        ID.column,
        Number.column,
        PlanetName.column.alias(PlanetName.name.replaceAll(" ", "")),
        PlanetHost.column.alias(PlanetHost.name.replaceAll(" ", "")),
        NumStars.column.alias(NumStars.name.replaceAll(" ", "")),
        NumPlanets.column.alias(NumPlanets.name.replaceAll(" ", "")),
        DiscoveryMethod.column.alias(DiscoveryMethod.name.replaceAll(" ", "")),
        DiscoveryYear.column.alias(DiscoveryYear.name.replaceAll(" ", "")),
        DiscoveryFacility.column.alias(DiscoveryFacility.name.replaceAll(" ", "")),
        OrbitalPeriodDays.column.alias(OrbitalPeriodDays.name.replaceAll(" ", "")),
        OrbitSemiMajorAxis.column.alias(OrbitSemiMajorAxis.name.replaceAll(" ", "")),
        Mass.column.alias(Mass.name.replaceAll(" ", "")),
        Eccentricity.column.alias(Eccentricity.name.replaceAll(" ", "")),
        InsolationFlux.column.alias(InsolationFlux.name.replaceAll(" ", "")),
        EquilibriumTemperature.column.alias(EquilibriumTemperature.name.replaceAll(" ", "")),
        SpectralType.column.alias(SpectralType.name.replaceAll(" ", "")),
        StellarEffectiveTemperature.column.alias(StellarEffectiveTemperature.name.replaceAll(" ", "")),
        StellarRadius.column.alias(StellarRadius.name.replaceAll(" ", "")),
        StellarMass.column.alias(StellarMass.name.replaceAll(" ", "")),
        StellarMetallicity.column.alias(StellarMetallicity.name.replaceAll(" ", "")),
        StellarMetallicityRatio.column.alias(StellarMetallicityRatio.name.replaceAll(" ", "")),
        StellarSurfaceGravity.column.alias(StellarSurfaceGravity.name.replaceAll(" ", "")),
        Distance.column.alias(Distance.name.replaceAll(" ", "")),
        GaiaMagnitude.column.alias(GaiaMagnitude.name.replaceAll(" ", "")),
        col("row_number"),
        col("rank"),
        col("dense_rank")
      )
      .orderBy(StellarEffectiveTemperature.column.asc)
  }

}
