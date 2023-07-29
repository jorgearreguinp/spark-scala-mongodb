package common.naming

import common.naming.ExoplanetsObjects._
import org.apache.spark.sql.types._

object Schema {

  lazy val exoplanetsSchema: StructType = StructType(List(
    StructField(Number.name, IntegerType),
    StructField(PlanetName.name, StringType),
    StructField(PlanetHost.name, StringType),
    StructField(NumStars.name, IntegerType),
    StructField(NumPlanets.name, IntegerType),
    StructField(DiscoveryMethod.name, StringType),
    StructField(DiscoveryYear.name, IntegerType),
    StructField(DiscoveryFacility.name, StringType),
    StructField(OrbitalPeriodDays.name, DoubleType),
    StructField(OrbitSemiMajorAxis.name, DoubleType),
    StructField(Mass.name, IntegerType),
    StructField(Eccentricity.name, DoubleType),
    StructField(InsolationFlux.name, DoubleType),
    StructField(EquilibriumTemperature.name, DoubleType),
    StructField(SpectralType.name, DoubleType),
    StructField(StellarEffectiveTemperature.name, DoubleType),
    StructField(StellarRadius.name, DoubleType),
    StructField(StellarMass.name, DoubleType),
    StructField(StellarMetallicity.name, DoubleType),
    StructField(StellarMetallicityRatio.name, StringType),
    StructField(StellarSurfaceGravity.name, DoubleType),
    StructField(Distance.name, DoubleType),
    StructField(GaiaMagnitude.name,DoubleType)
  ))

}
