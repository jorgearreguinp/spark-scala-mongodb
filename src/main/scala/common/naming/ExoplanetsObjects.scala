package common.naming

import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.row_number
import org.apache.spark.sql.types.{DataType, IntegerType}

object ExoplanetsObjects {

  val keyValue: String = "exoplanets"

  case object ID extends Field {
    override val name: String = "_id"
  }

  case object No extends Field {
    override val name: String = "No"
  }

  case object Number extends Field {
    override val name: String = "Number"
  }

  case object PlanetName extends Field {
    override val name: String = "Planet Name"
  }

  case object PlanetHost extends Field {
    override val name: String = "Planet Host"
  }

  case object NumStars extends Field {
    override val name: String = "Num Stars"
  }

  case object NumPlanets extends Field {
    override val name: String = "Num Planets"
  }

  case object DiscoveryMethod extends Field {
    override val name: String = "Discovery Method"
  }

  case object DiscoveryYear extends Field {
    override val name: String = "Discovery Year"
  }

  case object DiscoveryFacility extends Field {
    override val name: String = "Discovery Facility"
  }

  case object OrbitalPeriodDays extends Field {
    override val name: String = "Orbital Period Days"
  }

  case object OrbitSemiMajorAxis extends Field {
    override val name: String = "Orbit Semi-Major Axis"
  }

  case object Mass extends Field {
    override val name: String = "Mass"
  }

  case object Eccentricity extends Field {
    override val name: String = "Eccentricity"
  }

  case object InsolationFlux extends Field {
    override val name: String = "Insolation Flux"
  }

  case object EquilibriumTemperature extends Field {
    override val name: String = "Equilibrium Temperature"
  }

  case object SpectralType extends Field {
    override val name: String = "Spectral Type"
  }

  case object StellarEffectiveTemperature extends Field {
    override val name: String = "Stellar Effective Temperature"
  }

  case object StellarRadius extends Field {
    override val name: String = "Stellar Radius"
  }

  case object StellarMass extends Field {
    override val name: String = "Stellar Mass"
  }

  case object StellarMetallicity extends Field {
    override val name: String = "Stellar Metallicity"
  }

  case object StellarMetallicityRatio extends Field {
    override val name: String = "Stellar Metallicity Ratio"
  }

  case object StellarSurfaceGravity extends Field {
    override val name: String = "Stellar Surface Gravity"
  }

  case object Distance extends Field {
    override val name: String = "Distance"
  }

  case object GaiaMagnitude extends Field {
    override val name: String = "Gaia Magnitude"
  }

  case object RowNumber extends Field {
    override val name: String = "row_number"
  }

  case object Rank extends Field {
    override val name: String = "rank"
  }

  case object DenseRank extends Field {
    override val name: String = "dense_rank"
  }

  case object Position extends Field {
    override val name: String = "Position"
    val dataType: DataType = IntegerType
    def run(): Column = {
      val w: WindowSpec = Window.orderBy(Number.column)
      val c: Column = row_number()
        .over(w)
        .alias(name)
        .cast(dataType)
      c
    }
  }

}
