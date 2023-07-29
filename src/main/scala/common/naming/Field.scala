package common.naming

import org.apache.spark.sql.{Column, functions}

trait Field {

  val name: String
  lazy val column: Column = functions.col(name)
  lazy val trim: Column = functions.trim(column).alias(name)

}
